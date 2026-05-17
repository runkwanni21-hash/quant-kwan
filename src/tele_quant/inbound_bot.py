"""텔레그램 수신 봇 — 사용자 명령을 받아 즉시 분석 응답을 돌려준다.

지원 명령:
  /분석 <종목코드|이름>  — 개별 종목 즉시 분석 (펀더멘탈 + 기술적 + 수혜주 힌트)
  /브리핑 [KR|US]       — 4H 통합 브리핑 (30초~1분 소요)
  /포트                  — 모의 포트폴리오 현황
  /매크로                — 매크로 온도계 (WTI·금리·환율·VIX)
  /수혜주 <종목>         — 수급 체인 수혜주 발굴
  /도움말                — 명령 목록

보안: TELEGRAM_INBOUND_ALLOWED_IDS 에 등록된 chat_id 만 응답.
미등록 시 TELEGRAM_BOT_TARGET_CHAT_ID 로 fallback.

실계좌 주문 없음. 공개 정보 기반 리서치 보조.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import TYPE_CHECKING, Any

import httpx

from tele_quant.textutil import mask_bot_token

if TYPE_CHECKING:
    from tele_quant.db import Store
    from tele_quant.settings import Settings

log = logging.getLogger(__name__)

_DISCLAIMER = "⚠ 공개 정보 기반 리서치 보조 — 투자 판단 책임은 사용자에게 있음"

_HELP_TEXT = """\
🤖 **tele-quant 봇 명령어**

/분석 <종목>   — 개별 종목 즉시 분석
  예) /분석 삼성전자   /분석 005930   /분석 NVDA

/브리핑 [KR|US]  — 4H 통합 브리핑 (30초~1분 소요)
  예) /브리핑   /브리핑 US

/포트  — 모의 포트폴리오 현황

/매크로  — 매크로 온도계 (WTI·금리·환율·VIX)

/수혜주 <종목>  — 이슈 수혜주 발굴
  예) /수혜주 삼성전자

/도움말  — 이 도움말

──────────────
공개 정보 기반 리서치 보조. 실계좌 주문 없음.
"""

# ── 심볼 해석 ─────────────────────────────────────────────────────────────────

def _resolve_symbol(query: str) -> tuple[str, str] | None:
    """종목 코드 또는 이름을 (symbol, market) 튜플로 변환.

    Returns None if the query cannot be resolved to a known symbol.
    """
    from tele_quant.relation_feed import _NAME_MAP, _UNIVERSE_US

    q = query.strip()

    # 1. 6자리 숫자 + .KS/.KQ 코드
    if re.match(r"^\d{6}\.(KS|KQ)$", q, re.IGNORECASE):
        sym = q.upper()
        return sym, "KR"

    # 6자리 숫자만 (접미사 없음) → .KS 추가
    if re.match(r"^\d{6}$", q):
        sym = q + ".KS"
        return sym, "KR"

    # 2. US 티커 (대문자 1~5자)
    q_up = q.upper()
    if re.match(r"^[A-Z]{1,5}$", q_up) and (q_up in _UNIVERSE_US or q_up in _NAME_MAP):
        return q_up, "US"

    # 3. 한글/영문 이름 완전 일치 (대소문자 무시)
    q_low = q.lower()
    for sym, name in _NAME_MAP.items():
        if name.lower() == q_low:
            market = "KR" if sym.endswith((".KS", ".KQ")) else "US"
            return sym, market

    # 4. 한글/영문 이름 부분 포함 일치 (가장 짧은 이름이 우선)
    candidates: list[tuple[str, str, str]] = []  # (sym, market, name)
    for sym, name in _NAME_MAP.items():
        n_low = name.lower()
        if q_low in n_low or n_low in q_low:
            market = "KR" if sym.endswith((".KS", ".KQ")) else "US"
            candidates.append((sym, market, name))

    if candidates:
        # 이름이 짧을수록 더 직접적인 매칭
        candidates.sort(key=lambda x: len(x[2]))
        return candidates[0][0], candidates[0][1]

    return None


def _market_of(symbol: str) -> str:
    return "KR" if symbol.endswith((".KS", ".KQ")) else "US"


# ── 개별 종목 빠른 분석 ───────────────────────────────────────────────────────

def _quick_tech_score(d: dict[str, Any], side: str) -> tuple[float, str]:
    """4H 데이터로 간단한 기술적 점수 계산 (0~40점).

    daily_alpha._score_technical_long/short 의 경량 버전.
    """
    score = 0.0
    parts: list[str] = []

    rsi = d.get("rsi")
    bb_pct = d.get("bb_pct")
    vol_ratio = d.get("vol_ratio") or 1.0
    obv = d.get("obv", "")

    if side == "LONG":
        if rsi is not None:
            if rsi < 40:
                score += 15
                parts.append(f"RSI {rsi:.0f}(과매도)")
            elif rsi < 55:
                score += 10
                parts.append(f"RSI {rsi:.0f}(중립)")
            elif rsi < 70:
                score += 5
                parts.append(f"RSI {rsi:.0f}(상승)")
            else:
                score -= 5
                parts.append(f"RSI {rsi:.0f}(과열)")
        if bb_pct is not None:
            if bb_pct < 20:
                score += 10
                parts.append("볼린저 하단 근접")
            elif bb_pct < 50:
                score += 5
                parts.append("볼린저 중하단")
        if vol_ratio >= 1.5:
            score += 8
            parts.append(f"거래량 {vol_ratio:.1f}x↑")
        elif vol_ratio >= 1.2:
            score += 4
            parts.append(f"거래량 {vol_ratio:.1f}x")
        if "상승" in obv:
            score += 7
            parts.append("OBV 상승")
    else:  # SHORT
        if rsi is not None:
            if rsi > 70:
                score += 15
                parts.append(f"RSI {rsi:.0f}(과열)")
            elif rsi > 55:
                score += 8
                parts.append(f"RSI {rsi:.0f}(고점권)")
        if bb_pct is not None:
            if bb_pct > 80:
                score += 10
                parts.append("볼린저 상단 돌파")
            elif bb_pct > 60:
                score += 5
                parts.append("볼린저 중상단")
        if vol_ratio >= 1.5:
            score += 8
            parts.append(f"거래량 {vol_ratio:.1f}x↑")
        if "하락" in obv:
            score += 7
            parts.append("OBV 하락")

    return min(score, 40.0), " · ".join(parts) if parts else "데이터 부족"


def analyze_single(
    symbol: str,
    market: str = "",
    store: Store | None = None,
) -> str:
    """단일 종목 즉시 분석 — 텔레그램 응답용 포맷 텍스트 반환."""
    from tele_quant.daily_alpha import _fetch_4h_data
    from tele_quant.fundamentals import (
        build_fundamental_line,
        fetch_fundamentals,
        get_edge_label,
        is_institutional_blind_spot,
        score_fundamentals,
    )
    from tele_quant.relation_feed import _NAME_MAP

    if not market:
        market = _market_of(symbol)

    name = _NAME_MAP.get(symbol, symbol)

    lines: list[str] = []
    lines.append(f"🔍 **{name}** ({symbol})")
    lines.append("")

    # ── 기술적 분석 ──────────────────────────────────────────────────────────
    try:
        tech = _fetch_4h_data(symbol)
        rsi = tech.get("rsi")
        close = tech.get("close")

        if close:
            price_str = f"{close:,.0f}원" if market == "KR" else f"${close:.2f}"
            lines.append(f"현재가: {price_str}")

        long_tech, long_tech_reason = _quick_tech_score(tech, "LONG")
        short_tech, short_tech_reason = _quick_tech_score(tech, "SHORT")

        # RSI 기반으로 LONG/SHORT 방향 자동 선택
        direction = "SHORT" if (rsi or 50) > 65 else "LONG"
        tech_score = long_tech if direction == "LONG" else short_tech
        tech_reason = long_tech_reason if direction == "LONG" else short_tech_reason

        lines.append(f"방향 시사: {direction}  |  기술점수: {tech_score:.0f}/40")
        lines.append(f"기술: {tech_reason}")
    except Exception as exc:
        log.debug("[inbound] tech fetch failed for %s: %s", symbol, exc)
        tech_score = 0.0
        direction = "LONG"
        lines.append("기술 데이터 조회 실패")

    # ── 펀더멘탈 분석 ────────────────────────────────────────────────────────
    try:
        fund = fetch_fundamentals(symbol, market)
        val_score, val_reason = score_fundamentals(fund, direction)
        fund_line = build_fundamental_line(fund)

        lines.append("")
        lines.append(f"펀더멘탈: {fund_line}")
        if val_reason:
            lines.append(f"가치: {val_reason[:120]}")

        edge = get_edge_label(fund)
        if edge:
            lines.append(f"엣지: {edge}")

        blind = is_institutional_blind_spot(fund)
        if blind:
            lines.append("🎯 기관 사각지대 구간")
    except Exception as exc:
        log.debug("[inbound] fundamentals fetch failed for %s: %s", symbol, exc)
        val_score = 0.0
        val_reason = ""

    # ── 종합 점수 ────────────────────────────────────────────────────────────
    total = min(tech_score / 40 * 50 + val_score / 100 * 50, 100)
    grade = "★★★" if total >= 80 else "★★" if total >= 65 else "★" if total >= 50 else "—"

    lines.append("")
    lines.append(f"종합: {total:.0f}점 {grade}")
    if total >= 80:
        lines.append("→ 모의 포트폴리오 진입 기준 충족 (80점↑)")
    elif total >= 70:
        lines.append("→ 관찰 후보 (추가 확인 권장)")
    else:
        lines.append("→ 현재 기준 미달")

    lines.append("")
    lines.append(_DISCLAIMER)

    return "\n".join(lines)


# ── 수혜주 발굴 ───────────────────────────────────────────────────────────────

def _find_beneficiaries(symbol: str, store: Store | None) -> str:
    """종목의 수급 체인 수혜주 목록 반환."""
    from tele_quant.relation_feed import _NAME_MAP
    from tele_quant.supply_chain_alpha import (
        MoverEvent,
        _match_mover_to_rules,
        load_supply_chain_rules,
    )

    market = _market_of(symbol)
    name = _NAME_MAP.get(symbol, symbol)

    try:
        rules = load_supply_chain_rules()
    except Exception as exc:
        log.warning("[inbound] rule load failed: %s", exc)
        return f"{name} 체인 규칙 로드 실패"

    # 가상의 급등 이벤트로 체인 조회
    mover = MoverEvent(
        symbol=symbol,
        name=name,
        market=market,
        return_1d=5.0,      # 가상 5% 급등
        volume_ratio=2.0,
        reason_type="catalyst",
        confidence="high",
        sector="",
    )

    matched = _match_mover_to_rules(mover, rules)
    if not matched:
        return f"{name}({symbol}) — 체인 규칙 매칭 없음\n연관 섹터 체인이 설정되어 있지 않습니다."

    lines = [f"🔗 {name}({symbol}) 수혜주 체인\n"]
    for rule in matched[:6]:
        targets = rule.get("long_targets") or rule.get("targets") or []
        tier = rule.get("tier", "")
        reason = rule.get("reason", rule.get("description", ""))

        if targets:
            target_names = [
                f"{_NAME_MAP.get(t, t)}({t})" for t in targets[:4]
            ]
            lines.append(f"• [{tier}] {', '.join(target_names)}")
            if reason:
                lines.append(f"  이유: {reason[:80]}")

    lines.append("")
    lines.append("⚠ 수혜주 체인은 과거 패턴 기반 — 실제 반응은 별도 확인 필요")
    return "\n".join(lines)


# ── 텔레그램 API 헬퍼 ─────────────────────────────────────────────────────────

async def _tg_get(
    client: httpx.AsyncClient,
    token: str,
    method: str,
    **params: Any,
) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{token}/{method}"
    resp = await client.get(url, params=params, timeout=35.0)
    resp.raise_for_status()
    return resp.json()


async def _tg_post(
    client: httpx.AsyncClient,
    token: str,
    method: str,
    payload: dict[str, Any],
) -> None:
    url = f"https://api.telegram.org/bot{token}/{method}"
    resp = await client.post(url, json=payload, timeout=15.0)
    if resp.status_code == 400:
        log.warning("[inbound] 400 Bad Request: %s", mask_bot_token(resp.text[:300]))
    else:
        resp.raise_for_status()


async def _send(
    client: httpx.AsyncClient,
    token: str,
    chat_id: int | str,
    text: str,
) -> None:
    """텍스트를 4000자 청크로 나눠 전송."""
    chunks = [text[i : i + 4000] for i in range(0, len(text), 4000)]
    for i, chunk in enumerate(chunks):
        prefix = f"({i + 1}/{len(chunks)})\n" if len(chunks) > 1 else ""
        await _tg_post(
            client,
            token,
            "sendMessage",
            {"chat_id": chat_id, "text": prefix + chunk, "disable_web_page_preview": True},
        )


async def _send_typing(
    client: httpx.AsyncClient,
    token: str,
    chat_id: int | str,
) -> None:
    import contextlib as _contextlib
    with _contextlib.suppress(Exception):
        await _tg_post(
            client, token, "sendChatAction", {"chat_id": chat_id, "action": "typing"}
        )


# ── 명령 처리 ─────────────────────────────────────────────────────────────────

async def _handle_command(
    text: str,
    chat_id: int | str,
    client: httpx.AsyncClient,
    token: str,
    store: Store | None,
    settings: Settings,
) -> None:
    """단일 메시지 텍스트를 파싱하고 적절한 응답을 보낸다."""
    text = text.strip()
    cmd, _, args = text.partition(" ")
    cmd = cmd.lower().lstrip("/")
    args = args.strip()

    # 봇 멘션 (@botname) 제거
    cmd = cmd.split("@")[0]

    await _send_typing(client, token, chat_id)

    # ── /도움말 / /help ───────────────────────────────────────────────────────
    if cmd in ("도움말", "help", "start"):
        await _send(client, token, chat_id, _HELP_TEXT)
        return

    # ── /매크로 ───────────────────────────────────────────────────────────────
    if cmd in ("매크로", "macro"):
        try:
            from tele_quant.macro_pulse import (
                build_macro_section,
                fetch_macro_snapshot,
            )
            snap = fetch_macro_snapshot()
            reply = "📊 매크로 온도계\n\n" + build_macro_section(snap)
            reply += f"\n\n{_DISCLAIMER}"
        except Exception as exc:
            log.warning("[inbound] macro failed: %s", exc)
            reply = f"매크로 데이터 조회 실패: {exc}"
        await _send(client, token, chat_id, reply)
        return

    # ── /포트 ─────────────────────────────────────────────────────────────────
    if cmd in ("포트", "portfolio", "포트폴리오"):
        if store is None:
            await _send(client, token, chat_id, "DB 연결이 없어 포트폴리오 조회 불가")
            return
        try:
            from tele_quant.mock_portfolio import (
                build_portfolio_section,
                get_portfolio_summary,
            )
            summary = get_portfolio_summary(store)
            section = build_portfolio_section(store)
            header = (
                f"💼 모의 포트폴리오\n"
                f"보유 {summary['open_count']}/{summary['max_positions']}  "
                f"승률 {summary['win_rate']:.0f}%  "
                f"평균수익 {summary['avg_return']:+.1f}%\n\n"
            )
            reply = header + section + f"\n\n{_DISCLAIMER}"
        except Exception as exc:
            log.warning("[inbound] portfolio failed: %s", exc)
            reply = f"포트폴리오 조회 실패: {exc}"
        await _send(client, token, chat_id, reply)
        return

    # ── /브리핑 [KR|US] ────────────────────────────────────────────────────────
    if cmd in ("브리핑", "briefing"):
        market = args.upper() if args.upper() in ("KR", "US") else "KR"
        await _send(
            client, token, chat_id,
            f"⏳ {market} 4H 브리핑 생성 중... (30초~1분 소요)",
        )
        if store is None:
            await _send(client, token, chat_id, "DB 연결이 없어 브리핑 실행 불가")
            return
        try:
            from tele_quant.briefing import run_4h_briefing
            reply = run_4h_briefing(market, store, settings)
        except Exception as exc:
            log.warning("[inbound] briefing failed: %s", exc)
            reply = f"브리핑 생성 실패: {exc}"
        await _send(client, token, chat_id, reply)
        return

    # ── /수혜주 <종목> ─────────────────────────────────────────────────────────
    if cmd in ("수혜주", "beneficiary", "chain"):
        if not args:
            await _send(client, token, chat_id, "사용법: /수혜주 삼성전자")
            return
        resolved = _resolve_symbol(args)
        if resolved is None:
            await _send(client, token, chat_id, f"'{args}' 종목을 찾을 수 없습니다.")
            return
        symbol, _market = resolved
        reply = _find_beneficiaries(symbol, store)
        await _send(client, token, chat_id, reply)
        return

    # ── /분석 <종목> ───────────────────────────────────────────────────────────
    query = args if cmd in ("분석", "analyze", "analysis") else text

    if not query:
        await _send(client, token, chat_id, "사용법: /분석 삼성전자  또는  /분석 NVDA")
        return

    resolved = _resolve_symbol(query)
    if resolved is None:
        await _send(
            client, token, chat_id,
            f"'{query}' 를 인식할 수 없습니다.\n"
            "예) /분석 삼성전자   /분석 005930   /분석 NVDA",
        )
        return

    symbol, market = resolved
    await _send(client, token, chat_id, f"⏳ {symbol} 분석 중...")
    try:
        reply = analyze_single(symbol, market, store)
    except Exception as exc:
        log.warning("[inbound] analyze_single failed %s: %s", symbol, exc)
        reply = f"{symbol} 분석 실패: {exc}"
    await _send(client, token, chat_id, reply)


# ── 폴링 루프 ─────────────────────────────────────────────────────────────────

def _get_allowed_ids(settings: Settings) -> set[str]:
    """허용된 chat_id 집합 반환. 미설정이면 TELEGRAM_BOT_TARGET_CHAT_ID 사용."""
    raw = getattr(settings, "telegram_inbound_allowed_ids", "") or ""
    ids = {s.strip() for s in raw.split(",") if s.strip()}
    if not ids and settings.telegram_bot_target_chat_id:
        ids.add(str(settings.telegram_bot_target_chat_id))
    return ids


def _get_inbound_token(settings: Settings) -> str | None:
    """수신 봇 토큰 반환. 전용 설정이 없으면 기존 BOT_TOKEN 사용."""
    token = getattr(settings, "telegram_inbound_bot_token", None) or settings.telegram_bot_token
    return token


async def run_inbound_bot(settings: Settings, store: Store | None = None) -> None:
    """텔레그램 수신 봇 폴링 루프 (무한 루프, Ctrl-C로 종료).

    getUpdates long-polling 방식 사용 (webhook 불필요).
    """
    token = _get_inbound_token(settings)
    if not token:
        raise ValueError(
            "TELEGRAM_BOT_TOKEN 또는 TELEGRAM_INBOUND_BOT_TOKEN 이 설정되지 않았습니다.\n"
            ".env.local 에 TELEGRAM_BOT_TOKEN=<토큰> 을 추가하세요."
        )

    allowed_ids = _get_allowed_ids(settings)
    log.info(
        "[inbound-bot] 시작 — 허용된 chat_id: %s",
        ", ".join(allowed_ids) if allowed_ids else "(제한 없음)",
    )

    offset = 0
    retry_delay = 1.0

    async with httpx.AsyncClient(timeout=40.0) as client:
        # 봇 정보 확인
        try:
            me = await _tg_get(client, token, "getMe")
            bot_name = me.get("result", {}).get("username", "unknown")
            log.info("[inbound-bot] 연결됨 @%s", bot_name)
        except Exception as exc:
            raise RuntimeError(
                f"Telegram getMe 실패 — 토큰을 확인하세요: {mask_bot_token(str(exc))}"
            ) from exc

        while True:
            try:
                data = await _tg_get(
                    client, token, "getUpdates",
                    offset=offset, timeout=30, allowed_updates="message",
                )
                updates: list[dict[str, Any]] = data.get("result", [])
                retry_delay = 1.0  # 성공 시 리셋

                for update in updates:
                    offset = update["update_id"] + 1
                    msg = update.get("message", {})
                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    text = msg.get("text", "")

                    if not text or not chat_id:
                        continue

                    # 보안: 허용된 채팅 ID만 처리
                    if allowed_ids and chat_id not in allowed_ids:
                        log.warning(
                            "[inbound-bot] 미허가 chat_id=%s — 무시 (텍스트: %s…)",
                            chat_id,
                            text[:40],
                        )
                        continue

                    log.info("[inbound-bot] 수신 chat=%s text=%s…", chat_id, text[:60])

                    # 명령 처리 (에러가 나도 루프 유지)
                    try:
                        await _handle_command(text, chat_id, client, token, store, settings)
                    except Exception as exc:
                        log.error("[inbound-bot] 명령 처리 오류: %s", exc, exc_info=True)
                        import contextlib as _cl
                        with _cl.suppress(Exception):
                            await _send(client, token, chat_id, f"처리 중 오류 발생: {exc}")

            except httpx.TimeoutException:
                # long-poll timeout은 정상 — 바로 다시 폴링
                continue
            except asyncio.CancelledError:
                log.info("[inbound-bot] 종료 신호 수신")
                break
            except Exception as exc:
                masked = mask_bot_token(str(exc))
                log.warning("[inbound-bot] 폴링 오류 (%.0fs 후 재시도): %s", retry_delay, masked)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60.0)  # exponential back-off
