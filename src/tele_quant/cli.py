from __future__ import annotations

import asyncio
from datetime import UTC
from typing import Annotated

import httpx
import typer
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn
from rich.table import Table

from tele_quant.logging import configure_logging, console
from tele_quant.ollama_client import OllamaClient
from tele_quant.pipeline import TeleQuantPipeline
from tele_quant.settings import Settings
from tele_quant.telegram_client import TelegramGateway
from tele_quant.telegram_sender import TelegramSender
from tele_quant.textutil import mask_bot_token

app = typer.Typer(no_args_is_help=True, rich_markup_mode="rich")


def _settings() -> Settings:
    settings = Settings()
    settings.ensure_runtime_dirs()
    return settings


@app.callback()
def main(
    log_level: Annotated[str, typer.Option("--log-level", help="INFO, DEBUG, WARNING")] = "INFO",
) -> None:
    configure_logging(log_level)


@app.command()
def doctor() -> None:
    """환경 설정을 빠르게 점검합니다."""
    settings = _settings()
    table = Table(title="Tele Quant Doctor")
    table.add_column("Item")
    table.add_column("Status")
    table.add_row("TELEGRAM_API_ID", "OK" if settings.telegram_api_id else "MISSING")
    table.add_row("TELEGRAM_API_HASH", "OK" if settings.telegram_api_hash else "MISSING")
    table.add_row("SEND_MODE", settings.telegram_send_mode)
    table.add_row("BOT_TOKEN", "설정됨" if settings.telegram_bot_token else "없음")
    table.add_row("SOURCE_CHATS", str(len(settings.source_chats)))
    table.add_row("INCLUDE_ALL_CHANNELS", str(settings.telegram_include_all_channels))
    table.add_row("ANALYSIS_ENABLED", str(settings.analysis_enabled))
    table.add_row("DIGEST_CHUNK_SIZE", str(settings.digest_chunk_size))
    table.add_row("OLLAMA_HOST", settings.ollama_host)
    table.add_row("SQLITE_PATH", str(settings.sqlite_path))
    table.add_row("INTRADAY_TECH_ENABLED", str(settings.intraday_tech_enabled))
    table.add_row("INTRADAY_PERIOD", settings.intraday_period)
    table.add_row("INTRADAY_INTERVAL", settings.intraday_interval)
    table.add_row("WEEKEND_MACRO_ONLY", str(settings.weekend_macro_only))
    console.print(table)

    issues = settings.validate_minimum()
    if issues:
        console.print("[bold red]설정 문제:[/bold red]")
        for issue in issues:
            console.print(f"- {issue}")
    else:
        console.print("[green]기본 설정 OK[/green]")

    async def check_ollama() -> None:
        ok = await OllamaClient(settings).health()
        console.print(f"Ollama: {'[green]OK[/green]' if ok else '[red]연결 실패[/red]'}")

    asyncio.run(check_ollama())


@app.command()
def auth() -> None:
    """텔레그램 사용자 계정 로그인을 1회 수행합니다."""

    async def run() -> None:
        settings = _settings()
        async with TelegramGateway(settings):
            console.print("[green]텔레그램 로그인/세션 생성 완료[/green]")

    asyncio.run(run())


@app.command("list-chats")
def list_chats(
    limit: Annotated[int, typer.Option("--limit", help="가져올 대화/채널 수")] = 300,
    only_channels: Annotated[bool, typer.Option("--only-channels/--all-dialogs")] = True,
) -> None:
    """내 계정이 볼 수 있는 텔레그램 채널 목록을 보여줍니다."""

    async def run() -> None:
        settings = _settings()
        async with TelegramGateway(settings) as gateway:
            rows = await gateway.list_dialogs(limit=limit, only_channels=only_channels)
        table = Table(title="Telegram chats/channels")
        table.add_column("id", overflow="fold")
        table.add_column("username")
        table.add_column("title")
        table.add_column("type")
        table.add_column("unread", justify="right")
        for row in rows:
            table.add_row(
                str(row.get("id") or ""),
                row.get("username") or "",
                row.get("title") or "",
                row.get("type") or "",
                str(row.get("unread") or 0),
            )
        console.print(table)
        console.print(
            "\n.env.local의 TELEGRAM_SOURCE_CHATS에는 username 또는 id를 쉼표로 넣으면 됩니다."
        )

    asyncio.run(run())


@app.command()
def once(
    send: Annotated[
        bool, typer.Option("--send/--no-send", help="요약을 텔레그램으로 보낼지")
    ] = True,
    hours: Annotated[
        float | None, typer.Option("--hours", help="몇 시간 전까지 볼지. 기본은 .env.local")
    ] = None,
    macro_only: Annotated[
        bool,
        typer.Option("--macro-only/--full", help="매크로 다이제스트만 전송, 종목 분석 생략"),
    ] = False,
) -> None:
    """수집→중복제거→Ollama 요약을 1회 실행합니다. ANALYSIS_ENABLED=true이면 종목 시나리오도 전송."""

    async def run() -> None:
        settings = _settings()
        pipeline = TeleQuantPipeline(settings)
        digest, analysis = await pipeline.run_once(send=send, hours=hours, macro_only=macro_only)
        console.rule("[dim]Digest Preview[/dim]")
        console.print(mask_bot_token(digest))
        if analysis:
            console.rule("[dim]Analysis Preview[/dim]")
            console.print(analysis)
        elif macro_only:
            console.print("[dim]macro-only 모드: 종목 분석 생략[/dim]")

    asyncio.run(run())


@app.command()
def loop() -> None:
    """DIGEST_INTERVAL_HOURS(기본 4시간) 간격으로 계속 실행합니다."""

    async def run() -> None:
        settings = _settings()
        pipeline = TeleQuantPipeline(settings)
        await pipeline.run_loop()

    asyncio.run(run())


@app.command()
def analyze(
    send: Annotated[
        bool, typer.Option("--send/--no-send", help="분석 메시지를 텔레그램으로 보낼지")
    ] = False,
    hours: Annotated[float | None, typer.Option("--hours", help="몇 시간 전까지 볼지")] = None,
) -> None:
    """데이터를 수집하고 종목 시나리오 분석만 실행합니다."""

    async def run() -> None:
        settings = _settings()
        pipeline = TeleQuantPipeline(settings)
        # run_once with analysis enabled but we only send analysis message
        _, analysis = await pipeline.run_once(send=False, hours=hours)
        if analysis:
            if send:
                async with TelegramGateway(settings) as gateway:
                    sender = TelegramSender(settings, gateway=gateway)
                    await sender.send(analysis)
                console.print("[green]분석 메시지 전송 완료[/green]")
            console.rule("[dim]Analysis[/dim]")
            console.print(analysis)
        else:
            console.print(
                "[yellow]분석할 종목 후보가 없습니다. (최소 점수 미달 또는 데이터 없음)[/yellow]"
            )

    asyncio.run(run())


@app.command()
def candidates(
    hours: Annotated[float | None, typer.Option("--hours", help="몇 시간 전까지 볼지")] = None,
    use_llm: Annotated[
        bool,
        typer.Option("--llm/--no-llm", help="LLM 정밀 추출 사용 (느림, 기본: AliasBook 빠른 추출)"),
    ] = False,
    expanded: Annotated[
        bool,
        typer.Option("--expanded/--no-expanded", help="상관관계/섹터 확장 후보 포함"),
    ] = False,
) -> None:
    """종목 후보 목록을 표로 보여줍니다.

    기본(--no-llm): AliasBook 기반 빠른 추출 (Ollama 없이 10초 내외).
    --llm: LLM 정밀 추출 (느리지만 더 풍부한 결과).
    --expanded: 상관관계·섹터 확장 후보까지 포함.
    """

    async def run() -> None:
        settings = _settings()
        pipeline = TeleQuantPipeline(settings)
        cands = await pipeline.run_candidates(hours=hours, use_llm=use_llm, expanded=expanded)
        if not cands:
            console.print("[yellow]언급된 종목 후보가 없습니다.[/yellow]")
            return

        if expanded:
            table = Table(title=f"종목 후보 ({len(cands)}개, 확장 포함)")
            table.add_column("종목명")
            table.add_column("심볼")
            table.add_column("시장")
            table.add_column("섹터")
            table.add_column("출처")
            table.add_column("언급", justify="right")
            table.add_column("호재수", justify="right")
            table.add_column("리스크수", justify="right")
            table.add_column("상관피어")
            for c in cands:
                sector = getattr(c, "sector", "") or "미분류"
                origin = getattr(c, "origin", "") or ""
                peer_parent = getattr(c, "correlation_parent", "") or ""
                peer_val = getattr(c, "correlation_value", None)
                if peer_parent and peer_val is not None:
                    peer = f"{peer_parent}({peer_val:.2f})"
                else:
                    peer = peer_parent
                table.add_row(
                    c.name or "",
                    c.symbol,
                    c.market,
                    sector,
                    origin,
                    str(c.mentions),
                    str(len(c.catalysts)),
                    str(len(c.risks)),
                    peer,
                )
        else:
            table = Table(title=f"종목 후보 ({len(cands)}개)")
            table.add_column("종목명")
            table.add_column("심볼")
            table.add_column("시장")
            table.add_column("언급횟수", justify="right")
            table.add_column("심리")
            table.add_column("호재수", justify="right")
            table.add_column("리스크수", justify="right")
            for c in cands:
                table.add_row(
                    c.name or "",
                    c.symbol,
                    c.market,
                    str(c.mentions),
                    c.sentiment,
                    str(len(c.catalysts)),
                    str(len(c.risks)),
                )
        console.print(table)

    asyncio.run(run())


@app.command("test-send")
def test_send() -> None:
    """텔레그램 전송만 테스트합니다."""

    async def run() -> None:
        settings = _settings()
        async with TelegramGateway(settings) as gateway:
            sender = TelegramSender(settings, gateway=gateway)
            await sender.send("Tele Quant 전송 테스트 ✅")
        console.print("[green]전송 완료[/green]")

    asyncio.run(run())


@app.command("bot-chat-id")
def bot_chat_id() -> None:
    """BotFather 봇의 getUpdates 결과에서 chat_id를 찾습니다."""

    async def run() -> None:
        settings = _settings()
        sender = TelegramSender(settings)
        updates = await sender.get_bot_updates()
        if not updates:
            console.print("봇에게 /start를 보낸 뒤 다시 실행하세요.")
            return
        table = Table(title="Bot updates")
        table.add_column("chat_id")
        table.add_column("from")
        table.add_column("text")
        for update in updates[-10:]:
            msg = update.get("message") or update.get("channel_post") or {}
            chat = msg.get("chat") or {}
            user = msg.get("from") or {}
            table.add_row(
                str(chat.get("id", "")),
                user.get("username") or user.get("first_name") or "",
                msg.get("text", ""),
            )
        console.print(table)

    asyncio.run(run())


@app.command()
def evidence(
    hours: Annotated[float | None, typer.Option("--hours", help="몇 시간 전까지 볼지")] = None,
) -> None:
    """EvidenceCluster 목록을 표로 출력합니다. (압축된 증거 묶음 확인)"""

    async def run() -> None:
        settings = _settings()
        pipeline = TeleQuantPipeline(settings)

        lookback = hours if hours is not None else settings.fetch_lookback_hours
        issues = settings.validate_minimum()
        if issues:
            console.print("[red]설정 오류: " + "; ".join(issues) + "[/red]")
            return

        from tele_quant.evidence import build_evidence_clusters

        async with TelegramGateway(settings) as gateway:
            kept, stats = await pipeline._collect_and_dedupe(gateway, lookback)

        clusters = build_evidence_clusters(kept, settings)

        table = Table(title=f"Evidence Clusters ({len(clusters)}개, {lookback}h)")
        table.add_column("ID", overflow="fold")
        table.add_column("극성")
        table.add_column("티커")
        table.add_column("테마")
        table.add_column("출처수", justify="right")
        table.add_column("점수", justify="right")
        table.add_column("헤드라인")

        for c in clusters[:40]:
            pol_icon = {"positive": "📈", "negative": "📉", "neutral": "📌"}.get(c.polarity, "")
            table.add_row(
                c.cluster_id,
                pol_icon + c.polarity,
                ",".join(c.tickers[:3]),
                ",".join(c.themes[:3]),
                str(c.source_count),
                f"{c.cluster_score:.1f}",
                c.headline[:50],
            )
        console.print(table)
        console.print(
            f"수집: tg={stats.telegram_items} naver={stats.report_items} dedup후={stats.kept_items}"
        )

    asyncio.run(run())


@app.command()
def sources(
    hours: Annotated[float | None, typer.Option("--hours", help="몇 시간 전까지 볼지")] = None,
) -> None:
    """채널별 수집량 / 품질점수 / 드롭 현황을 표로 출력합니다."""

    async def run() -> None:
        settings = _settings()
        lookback = hours if hours is not None else settings.fetch_lookback_hours
        issues = settings.validate_minimum()
        if issues:
            console.print("[red]설정 오류: " + "; ".join(issues) + "[/red]")
            return

        from tele_quant.source_quality import score_source_message

        async with TelegramGateway(settings) as gateway:
            raw_items = await gateway.fetch_recent_messages(hours=lookback)

        # Per-source aggregation
        source_stats: dict[str, dict] = {}
        for item in raw_items:
            sn = item.source_name
            if sn not in source_stats:
                source_stats[sn] = {"total": 0, "dropped": 0, "scores": []}
            sc = score_source_message(sn, item.text)
            source_stats[sn]["total"] += 1
            source_stats[sn]["scores"].append(sc)
            if settings.source_quality_enabled and sc < settings.source_quality_min_score:
                source_stats[sn]["dropped"] += 1

        table = Table(title=f"Source Stats ({len(source_stats)}채널, {lookback}h)")
        table.add_column("채널명")
        table.add_column("수집", justify="right")
        table.add_column("드롭", justify="right")
        table.add_column("평균점수", justify="right")

        sorted_sources = sorted(source_stats.items(), key=lambda x: x[1]["total"], reverse=True)
        for sn, st in sorted_sources[:30]:
            scores = st["scores"]
            avg = sum(scores) / len(scores) if scores else 0.0
            table.add_row(sn[:40], str(st["total"]), str(st["dropped"]), f"{avg:.1f}")

        console.print(table)

    asyncio.run(run())


@app.command("validate-tickers")
def validate_tickers() -> None:
    """config/ticker_aliases.yml의 모든 심볼을 yfinance로 검증합니다."""
    import yfinance as yf

    from tele_quant.analysis.aliases import load_alias_config

    settings = _settings()
    try:
        book = load_alias_config()
    except FileNotFoundError:
        console.print(
            f"[red]ticker_aliases.yml을 찾을 수 없습니다: {settings.ticker_aliases_path}[/red]"
        )
        return

    all_syms = book.all_symbols
    results: list[tuple[str, str, str, bool]] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("yfinance 검증 중…", total=len(all_syms))
        for sym_def in all_syms:
            try:
                hist = yf.Ticker(sym_def.symbol).history(period="5d", auto_adjust=True)
                ok = not hist.empty
            except Exception:
                ok = False
            results.append((sym_def.symbol, sym_def.name, sym_def.market, ok))
            progress.advance(task)

    table = Table(title="Ticker Validation")
    table.add_column("Symbol", overflow="fold")
    table.add_column("Name")
    table.add_column("Market")
    table.add_column("Status", justify="center")

    failed = 0
    for symbol, name, market, ok in results:
        status = "[green]OK[/green]" if ok else "[red]FAIL[/red]"
        table.add_row(symbol, name, market, status)
        if not ok:
            failed += 1

    console.print(table)
    if failed:
        console.print(
            f"[yellow]{failed}/{len(results)}개 심볼 데이터 없음 (상폐·이름변경 확인)[/yellow]"
        )
    else:
        console.print(f"[green]전체 {len(results)}개 심볼 검증 OK[/green]")


@app.command()
def providers() -> None:
    """외부 API provider 설정 현황을 표로 출력합니다."""
    from tele_quant.provider_config import available_providers

    result = available_providers(load_external=True)
    table = Table(title="API Providers")
    table.add_column("Provider")
    table.add_column("Status", justify="center")
    for name in [
        "yfinance",
        "fred",
        "finnhub",
        "fmp",
        "alpha_vantage",
        "polygon",
        "newsapi",
        "naver",
    ]:
        enabled = result.get(name, False)
        status = "[green]enabled[/green]" if enabled else "[dim]disabled[/dim]"
        table.add_row(name, status)
    console.print(table)
    console.print("[dim]키 값은 절대 출력하지 않습니다. 존재 여부만 확인합니다.[/dim]")


@app.command()
def weekly(
    send: Annotated[
        bool, typer.Option("--send/--no-send", help="주간 리포트를 텔레그램으로 전송할지")
    ] = False,
    days: Annotated[int, typer.Option("--days", help="최근 몇 일간의 리포트를 모을지")] = 7,
    mode: Annotated[
        str,
        typer.Option("--mode", help="no_llm: 순수 Python 집계 / deep_polish: Ollama 문장 다듬기"),
    ] = "no_llm",
) -> None:
    """최근 N일 fast 리포트를 모아 주간 총정리를 생성합니다."""
    from datetime import timedelta

    from tele_quant.db import Store
    from tele_quant.models import utc_now
    from tele_quant.weekly import build_weekly_deterministic_summary, build_weekly_input

    async def run() -> None:
        settings = _settings()
        store = Store(settings.sqlite_path)
        since = utc_now() - timedelta(days=days)
        limit = settings.weekly_max_reports
        reports = store.recent_run_reports(since=since, limit=limit)

        # Load relation feed for weekly report section
        relation_feed_data = None
        try:
            from tele_quant.relation_feed import load_relation_feed

            relation_feed_data = load_relation_feed(settings)
            if relation_feed_data.available:
                console.print(
                    f"[weekly] relation_feed: movers={len(relation_feed_data.movers)}"
                    f" leadlag={len(relation_feed_data.leadlag)}"
                )
        except Exception as _rf_exc:
            console.print(f"[yellow][weekly] relation_feed load failed: {_rf_exc}[/yellow]")

        console.print(
            f"[weekly] reports={len(reports)} days={days} mode={mode}",
        )

        # Load LONG ≥80 scenario history and fetch current prices for performance review
        perf_entries: list[dict] = []
        no_price_count = 0
        if getattr(settings, "weekly_performance_review", True):
            try:
                import yfinance as yf

                scenario_rows = store.recent_scenarios(since=since, side="LONG", min_score=80)
                console.print(f"[weekly] performance scenarios={len(scenario_rows)}")

                # scenario_history에서 price 있는 항목 처리 — 첫 80점 이상 시점 기준
                sym_all: dict[str, list[dict]] = {}
                for row in scenario_rows:
                    sym = row.get("symbol", "")
                    if sym:
                        sym_all.setdefault(sym, []).append(row)

                seen_syms: dict[str, dict] = {}
                for sym, rows_for_sym in sym_all.items():
                    # Sort ascending by created_at → oldest = first 80-point recommendation
                    rows_sorted = sorted(rows_for_sym, key=lambda r: r.get("created_at") or "")
                    first_row = rows_sorted[0]
                    entry_price = first_row.get("close_price_at_report")
                    if entry_price is None:
                        for r in rows_sorted:
                            if r.get("close_price_at_report") is not None:
                                first_row = r
                                entry_price = r.get("close_price_at_report")
                                break
                    if entry_price is None:
                        continue
                    best_row = max(rows_sorted, key=lambda r: r.get("score") or 0)
                    mkt = "KR" if sym.endswith((".KS", ".KQ")) else "US"
                    seen_syms[sym] = {
                        "symbol": sym,
                        "name": first_row.get("name"),
                        "score": first_row.get("score", 0),
                        "max_score": best_row.get("score", first_row.get("score", 0)),
                        "max_score_at": best_row.get("created_at"),
                        "entry_price": entry_price,
                        "created_at": first_row.get("created_at"),
                        "first_seen_at": first_row.get("created_at"),
                        "repeat_count": len(rows_for_sym),
                        "market": mkt,
                        "entry_basis": "report_time_latest_close",
                        "_source": "scenario_history",
                    }

                for sym, info in seen_syms.items():
                    try:
                        hist = yf.Ticker(sym).history(period="2d", auto_adjust=True)
                        if hist.empty:
                            no_price_count += 1
                            continue
                        current = float(hist["Close"].iloc[-1])
                        ret_pct = (current - info["entry_price"]) / info["entry_price"] * 100
                        perf_entries.append(
                            {
                                **info,
                                "current_price": current,
                                "return_pct": ret_pct,
                                "win": ret_pct > 0,
                            }
                        )
                    except Exception:
                        no_price_count += 1

                # Fallback: scenario_history가 비어 있으면 run_reports analysis_text 파싱
                if not perf_entries and reports:
                    console.print("[weekly] scenario_history 없음 → analysis_text fallback 파싱")
                    from tele_quant.weekly import parse_long_candidates_from_analysis

                    # Diagnose why scenario_history is empty
                    has_long_80 = (
                        any(
                            r.get("side") == "LONG" and (r.get("score") or 0) >= 80
                            for r in scenario_rows
                        )
                        if scenario_rows
                        else False
                    )
                    has_no_price = (
                        any(
                            r.get("close_price_at_report") is None
                            for r in scenario_rows
                            if r.get("side") == "LONG" and (r.get("score") or 0) >= 80
                        )
                        if scenario_rows
                        else False
                    )
                    _diag: list[str] = []
                    if not scenario_rows:
                        _diag.append("DB 저장 없음 (scenario_history 비어 있음)")
                    elif not has_long_80:
                        _diag.append("80점 이상 LONG 후보 없음")
                    elif has_no_price:
                        _diag.append("가격 확인 실패 (close_price_at_report NULL)")
                    if _diag:
                        console.print(f"[weekly] 성과 리뷰 진단: {'; '.join(_diag)}")

                    fallback_seen: dict[str, dict] = {}
                    has_analysis = False
                    for rep in reports:
                        if not rep.analysis:
                            continue
                        has_analysis = True
                        candidates = parse_long_candidates_from_analysis(
                            rep.analysis, min_score=80.0
                        )
                        for cand in candidates:
                            sym = cand["symbol"]
                            if sym and sym not in fallback_seen:
                                fallback_seen[sym] = {
                                    **cand,
                                    "created_at": rep.created_at.isoformat()
                                    if rep.created_at
                                    else None,
                                    "market": "KR" if sym.endswith((".KS", ".KQ")) else "US",
                                }

                    if not has_analysis:
                        _diag.append("분석 리포트 없음 (macro-only 기간)")
                    elif not fallback_seen:
                        _diag.append("fallback 파싱 실패 (80점 이상 롱 섹션 미발견)")

                    console.print(
                        f"[weekly] fallback candidates={len(fallback_seen)} source=analysis_text"
                    )
                    for sym, info in fallback_seen.items():
                        try:
                            hist = yf.Ticker(sym).history(period="2d", auto_adjust=True)
                            if hist.empty:
                                no_price_count += 1
                                continue
                            current = float(hist["Close"].iloc[-1])
                            entry = info.get("entry_price")
                            if entry:
                                ret_pct = (current - entry) / entry * 100
                                perf_entries.append(
                                    {
                                        **info,
                                        "current_price": current,
                                        "return_pct": ret_pct,
                                        "win": ret_pct > 0,
                                        "_source": "fallback",
                                    }
                                )
                            else:
                                no_price_count += 1
                                _diag.append(f"{sym}: 진입가 없음")
                        except Exception:
                            no_price_count += 1

                if no_price_count:
                    console.print(f"[weekly] 가격 확인 불가: {no_price_count}개 제외")

            except Exception as exc:
                console.print(f"[yellow][weekly] performance review failed: {exc}[/yellow]")

        weekly_input = build_weekly_input(reports, performance_entries=perf_entries)
        console.print(
            f"[weekly] tickers={len(weekly_input.top_tickers)}"
            f" macro_keywords={len(weekly_input.macro_keywords)}",
        )

        if weekly_input.report_count == 0:
            console.print("[yellow]최근 리포트가 없어 주간 요약 생략[/yellow]")
            return

        # Relation signal performance review
        relation_signal_review: str | None = None
        try:
            from tele_quant.weekly import build_relation_signal_review_section

            relation_signal_review = build_relation_signal_review_section(store, since=since)
            console.print("[weekly] relation_signal_review=ok")
        except Exception as _rsr_exc:
            console.print(f"[yellow][weekly] relation_signal_review failed: {_rsr_exc}[/yellow]")

        # Pair watch weekly review
        pair_watch_review: str | None = None
        try:
            from tele_quant.live_pair_watch import build_pair_watch_weekly_review

            pair_watch_review = build_pair_watch_weekly_review(
                store, since=since, settings=settings
            )
            console.print("[weekly] pair_watch_review=ok")
        except Exception as _pwr_exc:
            console.print(f"[yellow][weekly] pair_watch_review failed: {_pwr_exc}[/yellow]")

        summary = build_weekly_deterministic_summary(
            weekly_input,
            relation_feed_data=relation_feed_data,
            relation_signal_review=relation_signal_review,
            pair_watch_review=pair_watch_review,
        )

        if mode == "deep_polish":
            try:
                from tele_quant.ollama_client import OllamaClient

                ollama = OllamaClient(settings)
                import asyncio

                polished = await asyncio.wait_for(
                    ollama.polish_weekly_report(summary),
                    timeout=settings.weekly_ollama_timeout_seconds,
                )
                summary = polished
                console.print("[weekly] polish=ok")
            except TimeoutError:
                console.print("[yellow][weekly] polish timeout → deterministic kept[/yellow]")
            except Exception as exc:
                console.print(
                    f"[yellow][weekly] polish failed: {exc} → deterministic kept[/yellow]"
                )
        else:
            console.print("[weekly] polish=skipped")

        if send:
            async with TelegramGateway(settings) as gateway:
                sender = TelegramSender(settings, gateway=gateway)
                await sender.send(summary)
            console.print("[weekly] sent=ok")
        else:
            console.rule("[dim]Weekly Report Preview[/dim]")
            console.print(summary)

    asyncio.run(run())


@app.command()
def watchlist() -> None:
    """config/watchlist.yml의 관심종목 그룹·섹터·시간대 초점을 표로 출력합니다."""
    from datetime import datetime

    from rich.table import Table

    from tele_quant.watchlist import load_watchlist, report_focus_for_hour

    settings = _settings()
    cfg = load_watchlist(settings.watchlist_path)
    if cfg is None:
        console.print(f"[red]watchlist.yml을 불러올 수 없습니다: {settings.watchlist_path}[/red]")
        return

    # 그룹별 종목 표
    table = Table(title="Watchlist 그룹 현황")
    table.add_column("그룹 키")
    table.add_column("라벨")
    table.add_column("종목 수", justify="right")
    table.add_column("종목 목록")

    for key, grp in cfg.groups.items():
        table.add_row(
            key,
            grp.label,
            str(len(grp.symbols)),
            ", ".join(grp.symbols[:8]) + ("…" if len(grp.symbols) > 8 else ""),
        )
    console.print(table)

    # 선호 섹터
    if cfg.prefer_sectors:
        console.print("\n[bold]선호 섹터:[/bold] " + ", ".join(cfg.prefer_sectors))

    # 리포트 스타일
    console.print(f"[bold]최대 후보 수:[/bold] {cfg.max_candidates}")
    console.print(f"[bold]관심종목 우선 표시:[/bold] {cfg.show_watchlist_first}")

    # 시간대별 focus
    focus_table = Table(title="시간대별 리포트 초점")
    focus_table.add_column("시간대")
    focus_table.add_column("라벨")
    focus_table.add_column("초점")

    for hour_key, ctx in sorted(cfg.schedule_context.items()):
        focus_table.add_row(
            f"{hour_key}시",
            ctx.get("label", ""),
            ", ".join(ctx.get("focus", [])[:3]),
        )
    console.print(focus_table)

    # 현재 시간 초점
    now_hour = datetime.now(UTC).hour
    cur_focus = report_focus_for_hour(now_hour, cfg)
    if cur_focus:
        console.print(
            f"\n[green]현재({now_hour}시) 초점:[/green] {', '.join(cur_focus.get('focus', []))}"
        )

    if cfg.disclaimer:
        console.print(f"\n[dim]{cfg.disclaimer}[/dim]")


@app.command("relation-feed")
def relation_feed_cmd(
    send: Annotated[
        bool,
        typer.Option("--send/--no-send", help="요약을 텔레그램으로 전송할지"),
    ] = False,
    no_fallback: Annotated[
        bool,
        typer.Option("--no-fallback", help="fallback lead-lag 계산 생략"),
    ] = False,
    fallback_only: Annotated[
        bool,
        typer.Option("--fallback-only", help="fallback 후보만 표시 (stock feed 표 숨김)"),
    ] = False,
    force_fallback: Annotated[
        bool,
        typer.Option("--force-fallback", help="stock feed leadlag가 있어도 fallback 강제 계산"),
    ] = False,
    review: Annotated[
        bool,
        typer.Option("--review/--no-review", help="저장된 relation 신호 성과 리뷰 표시"),
    ] = False,
    limit: Annotated[
        int,
        typer.Option("--limit", help="stock feed lead-lag 표 최대 출력 행 수 (0=전체)"),
    ] = 20,
    show_all: Annotated[
        bool,
        typer.Option("--all", help="stock feed lead-lag 표 전체 출력 (--limit 무시)"),
    ] = False,
) -> None:
    """stock-relation-ai 공유 피드를 읽고 급등·급락 후행 후보를 출력합니다."""
    from tele_quant.relation_feed import build_relation_feed_section, load_relation_feed

    settings = _settings()
    feed = load_relation_feed(settings)

    if not feed.available:
        console.print("[yellow]relation feed 없음[/yellow]")
        for w in feed.load_warnings:
            console.print(f"  - {w}")
        return

    summary = feed.summary
    assert summary is not None

    # Compute fallback when appropriate
    should_compute_fallback = (
        not no_fallback and feed.movers and (not feed.leadlag or force_fallback)
    )
    if should_compute_fallback:
        try:
            from dataclasses import replace

            from tele_quant.local_data import load_correlation, load_price_history
            from tele_quant.relation_fallback import compute_fallback_leadlag

            price_store = load_price_history(settings)
            corr_store = load_correlation(settings)
            feed_for_fallback = (
                replace(feed, leadlag=[]) if (fallback_only or force_fallback) else feed
            )
            feed.fallback_candidates = compute_fallback_leadlag(
                feed_for_fallback, settings, price_store, corr_store
            )
        except Exception as _fb_exc:
            console.print(f"[yellow]fallback 계산 실패: {type(_fb_exc).__name__}[/yellow]")

    fb = feed.fallback_candidates

    # Summary table
    table = Table(title="Relation Feed Summary")
    table.add_column("항목")
    table.add_column("값")
    table.add_row("기준일", summary.asof_date)
    table.add_row("생성일시", summary.generated_at)
    table.add_row("mover rows", str(len(feed.movers)))
    table.add_row("stock feed leadlag", str(len(feed.leadlag)))
    table.add_row("fallback leadlag", str(len(fb)))
    if no_fallback:
        fallback_status = "생략 (--no-fallback)"
    elif fb:
        fallback_status = f"계산됨 ({len(fb)}건)"
    elif should_compute_fallback:
        fallback_status = "계산됨 (0건)"
    elif feed.leadlag and not force_fallback:
        fallback_status = "생략 (stock feed 있음)"
    else:
        fallback_status = "후보 없음"
    table.add_row("fallback 계산", fallback_status)
    if fb:
        med = sum(1 for c in fb if c.confidence == "medium")
        low = sum(1 for c in fb if c.confidence == "low")
        table.add_row("fallback confidence", f"medium={med} / low={low}")
    table.add_row("status", summary.status)
    if summary.warnings:
        table.add_row("warnings", ", ".join(summary.warnings))
    if feed.is_stale:
        table.add_row("⚠️ staleness", f"{feed.feed_age_hours:.0f}시간 전 생성")
    console.print(table)

    # Stock feed lead-lag table (hidden when --fallback-only)
    if fallback_only:
        if feed.leadlag and not force_fallback:
            console.print(
                f"[dim]stock feed leadlag가 {len(feed.leadlag)}개 존재하므로 fallback 계산 생략."
                " --force-fallback으로 강제 계산 가능[/dim]"
            )
    elif feed.leadlag:
        display_limit = 0 if show_all else max(limit, 0)
        seen_pairs: set[tuple[str, str]] = set()
        rows_to_display: list = []
        for r in feed.leadlag:
            pair = (r.source_symbol, r.target_symbol)
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            rows_to_display.append(r)
            if display_limit and len(rows_to_display) >= display_limit:
                break

        total_unique = sum(1 for r in {(r.source_symbol, r.target_symbol) for r in feed.leadlag})
        caption = (
            f"전체 {total_unique}개"
            if show_all or display_limit == 0 or len(rows_to_display) >= total_unique
            else f"총 {total_unique}개 중 상위 {len(rows_to_display)}개 표시 (--all로 전체 출력)"
        )
        ll_table = Table(
            title=f"Stock Feed Lead-Lag 후보 ({len(feed.leadlag)}개)",
            caption=caption,
        )
        ll_table.add_column("source")
        ll_table.add_column("등락률", justify="right")
        ll_table.add_column("move")
        ll_table.add_column("target")
        ll_table.add_column("relation")
        ll_table.add_column("lag", justify="right")
        ll_table.add_column("prob", justify="right")
        ll_table.add_column("lift", justify="right")
        ll_table.add_column("conf")
        ll_table.add_column("note")

        for r in rows_to_display:
            src_name = (r.source_name or r.source_symbol)[:20]
            tgt_name = (r.target_name or r.target_symbol)[:20]
            sign = "+" if r.source_move_type == "UP" else "-"
            ll_table.add_row(
                f"{src_name} / {r.source_symbol}",
                f"{sign}{abs(r.source_return_pct):.1f}%",
                r.source_move_type,
                f"{tgt_name} / {r.target_symbol}",
                r.relation_type[:15],
                str(r.lag_days),
                f"{r.conditional_prob:.1%}",
                f"{r.lift:.2f}x",
                r.confidence,
                r.note[:30] if r.note else "",
            )
        console.print(ll_table)
    elif not feed.leadlag and not fb:
        console.print("[yellow]lead-lag 후보 없음[/yellow]")

    # Fallback table
    if fb:
        fb_table = Table(
            title=f"Tele Quant Fallback 후보 ({len(fb)}개)",
            caption="self-computed / max confidence: medium",
        )
        fb_table.add_column("source")
        fb_table.add_column("return", justify="right")
        fb_table.add_column("target")
        fb_table.add_column("relation")
        fb_table.add_column("market_path")
        fb_table.add_column("lag", justify="right")
        fb_table.add_column("prob", justify="right")
        fb_table.add_column("base", justify="right")
        fb_table.add_column("lift", justify="right")
        fb_table.add_column("events", justify="right")
        fb_table.add_column("conf")

        for c in fb:
            sign = "+" if c.source_move_type == "UP" else ""
            src_disp = (c.source_name or c.source_symbol)[:18]
            fb_table.add_row(
                f"{src_disp} / {c.source_symbol}",
                f"{sign}{c.source_return_pct:.1f}%",
                c.target_symbol,
                c.relation_type[:14],
                c.market_path,
                str(c.lag_days),
                f"{c.conditional_prob:.1%}",
                f"{c.base_prob:.1%}",
                f"{c.lift:.2f}x",
                str(c.event_count),
                c.confidence,
            )
        console.print(fb_table)

    # Full section preview
    section = build_relation_feed_section(feed, settings=settings)
    if section:
        console.rule("[dim]섹션 미리보기[/dim]")
        console.print(section)

    if send:
        import asyncio as _asyncio

        async def _send() -> None:
            async with TelegramGateway(settings) as gateway:
                sender = TelegramSender(settings, gateway=gateway)
                await sender.send(section)
            console.print("[green]relation feed 섹션 전송 완료[/green]")

        _asyncio.run(_send())

    if review:
        from datetime import timedelta

        from tele_quant.db import Store
        from tele_quant.models import utc_now
        from tele_quant.weekly import build_relation_signal_review_section

        _store = Store(settings.sqlite_path)
        _since = utc_now() - timedelta(days=7)
        _review_section = build_relation_signal_review_section(_store, since=_since)
        console.rule("[dim]Relation Signal 성과 리뷰 (최근 7일)[/dim]")
        console.print(_review_section)


@app.command("pair-watch")
def pair_watch_cmd(
    sector: Annotated[
        str | None,
        typer.Option("--sector", help="섹터 필터: semiconductor|ess|cosmetics|defense"),
    ] = None,
    hours: Annotated[
        float | None,
        typer.Option("--hours", help="4H 기준 시간 (현재는 universe 가격 기준이므로 참고용)"),
    ] = None,
    send: Annotated[
        bool,
        typer.Option("--send/--no-send", help="관찰 섹션을 텔레그램으로 전송할지"),
    ] = False,
    no_db: Annotated[
        bool,
        typer.Option("--no-db", help="DB에 신호를 저장하지 않음"),
    ] = False,
) -> None:
    """선행·후행 페어 관찰 후보를 실시간으로 계산하고 표시합니다.

    출력: source / source_return / target / target_return / gap / prob / lift / confidence / action

    예: source NVDA +5.1% → target SK하이닉스 +0.6%, gap=미반응, confidence=medium, action=4H 확인 후보
    """
    from tele_quant.live_pair_watch import (
        build_pair_watch_section,
        format_signal_oneline,
        run_pair_watch,
    )

    settings = _settings()

    async def run() -> None:
        relation_feed = None
        try:
            from tele_quant.relation_feed import load_relation_feed

            relation_feed = load_relation_feed(settings)
        except Exception:
            pass

        corr_store = None
        try:
            from tele_quant.local_data import load_correlation

            corr_store = load_correlation(settings)
        except Exception:
            pass

        signals, used_stale, diagnostics = run_pair_watch(
            settings,
            sector_filter=sector,
            relation_feed=relation_feed,
            corr_store=corr_store,
        )

        if diagnostics:
            for d in diagnostics:
                console.print(f"[yellow]⚠ {d}[/yellow]")

        if used_stale:
            console.print("[dim]일부 가격 캐시 사용[/dim]")

        if not signals:
            console.print("[yellow]현재 기준 충족 pair-watch 신호 없음[/yellow]")
            console.print("[dim](source 움직임 부족 또는 min_confidence 미달)[/dim]")
            return

        table = Table(title=f"선행·후행 페어 관찰 ({len(signals)}개 신호)")
        table.add_column("source")
        table.add_column("4H 등락", justify="right")
        table.add_column("1D 등락", justify="right")
        table.add_column("→ target")
        table.add_column("target 4H", justify="right")
        table.add_column("gap")
        table.add_column("prob", justify="right")
        table.add_column("lift", justify="right")
        table.add_column("confidence")
        table.add_column("action")

        from tele_quant.live_pair_watch import _fmt_return

        for sig in signals:
            prob_str = f"{sig.conditional_prob:.1%}" if sig.conditional_prob is not None else "N/A"
            lift_str = f"{sig.lift:.1f}x" if sig.lift is not None else "N/A"
            action_short = (
                sig.watch_action.split(" — ")[0] if " — " in sig.watch_action else sig.watch_action
            )[:20]
            gap_color = {
                "미반응": "green",
                "약세전이미확인": "yellow",
                "부분반응": "blue",
                "현재불일치": "red",
                "불일치": "red",
                "이미반응": "dim",
            }.get(sig.gap_type, "white")
            is_rule_based = sig.conditional_prob is None and sig.lift is None
            conf_display = "규칙기반" if is_rule_based else sig.confidence
            table.add_row(
                f"{sig.source_name[:16]} / {sig.source_symbol}",
                _fmt_return(sig.source_return_4h),
                _fmt_return(sig.source_return_1d),
                f"{sig.target_name[:16]} / {sig.target_symbol}",
                _fmt_return(sig.target_return_4h),
                f"[{gap_color}]{sig.gap_type}[/{gap_color}]",
                prob_str,
                lift_str,
                conf_display,
                action_short,
            )
        console.print(table)

        # One-liner summary
        console.rule("[dim]요약[/dim]")
        for sig in signals[:5]:
            console.print(format_signal_oneline(sig))

        # Section preview
        section = build_pair_watch_section(
            signals,
            settings=settings,
            used_stale_cache=used_stale,
            diagnostics=diagnostics,
        )
        if section:
            console.rule("[dim]섹션 미리보기[/dim]")
            console.print(section)

        # DB save
        if not no_db:
            try:
                from tele_quant.db import Store

                store = Store(settings.sqlite_path)
                saved = store.save_pair_watch_signals(signals)
                if saved:
                    console.print(f"[green]pair_watch_history 저장: {saved}건[/green]")
            except Exception as exc:
                console.print(f"[yellow]DB 저장 실패: {exc}[/yellow]")

        # Telegram send
        if send and section:
            async with TelegramGateway(settings) as gateway:
                sender = TelegramSender(settings, gateway=gateway)
                await sender.send(section)
            console.print("[green]pair-watch 섹션 전송 완료[/green]")

    asyncio.run(run())


@app.command("ollama-tags")
def ollama_tags() -> None:
    """Ollama에 설치된 모델 목록을 보여줍니다."""

    async def run() -> None:
        settings = _settings()
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{settings.ollama_host.rstrip('/')}/api/tags")
            resp.raise_for_status()
            data = resp.json()
        table = Table(title="Ollama models")
        table.add_column("name")
        table.add_column("size")
        for model in data.get("models", []):
            table.add_row(model.get("name", ""), str(model.get("size", "")))
        console.print(table)

    asyncio.run(run())


@app.command("lint-report")
def lint_report(
    hours: Annotated[
        float, typer.Option("--hours", help="최근 몇 시간치 DB 리포트를 검사할지")
    ] = 4.0,
    limit: Annotated[int, typer.Option("--limit", help="최대 검사 리포트 수")] = 10,
) -> None:
    """최근 리포트의 품질 문제를 검사합니다 (브로커명 유출·근거 오류·SHORT 게이트 위반 등).

    Example: uv run tele-quant lint-report --hours 4
    """
    import re as _re
    from datetime import datetime as _datetime
    from datetime import timedelta

    from rich.markup import escape

    from tele_quant.db import Store
    from tele_quant.headline_cleaner import is_broker_header_only, is_low_quality_headline
    from tele_quant.models import utc_now

    def _read_report_field(row: object, name: str, default: str = "") -> str:
        val = row.get(name, default) if isinstance(row, dict) else getattr(row, name, default)  # type: ignore[union-attr]
        if val is None:
            return default
        if isinstance(val, _datetime):
            return val.strftime("%Y-%m-%d %H:%M")
        return str(val) if val else default

    _BROKER_HEADER_RES = [
        _re.compile(
            r"\b(?:Hana\s+Global\s+Guru\s+Eye|유안타\s*리서치센터|"
            r"하나증권\s*해외주식분석|키움증권\s*미국\s*주식\s*박기현|"
            r"연합인포맥스|ShowHashtag|S&P\s*500\s*map)\b",
            _re.IGNORECASE,
        ),
    ]
    # Broker false-positive: broker name appearing as stock candidate (not as source)
    # These appear when broker name leaks into LONG/SHORT section header/reasons
    _BROKER_AS_CANDIDATE_RE = _re.compile(
        r"(?:JPMorgan\s*(?:Chase)?|Goldman\s*Sachs|Morgan\s*Stanley|"
        r"JP모건|골드만삭스|모건스탠리|씨티|뱅크오브아메리카|BofA|Wedbush|"
        r"Piper\s+Sandler|Jefferies|HSBC)\s*/\s*(?:JPM|GS|MS|C|BAC|DB)",
        _re.IGNORECASE,
    )
    # Broker name raw leak in digest/analysis (report body should not name brokers directly)
    _BROKER_NAME_LEAK_RE = _re.compile(
        r"\b(?:JPMorgan(?:\s+Chase)?|Goldman\s+Sachs|Morgan\s+Stanley|"
        r"JP모건|골드만삭스|모건스탠리|뱅크오브아메리카|BofA|"
        r"Wedbush|Piper\s+Sandler|Jefferies)\b",
        _re.IGNORECASE,
    )
    _FORBIDDEN_WORDS = [
        "ACTION_READY",
        "LIVE_READY",
        "무조건 매수",
        "반드시 상승",
        "확정 수익",
    ]
    _NOISE_PATTERNS = [
        _re.compile(r"tel:|href=|ShowHashtag|연합인포맥스", _re.IGNORECASE),
        _re.compile(r"\d{2,3}[-–]\d{3,4}[-–]\d{4}"),  # phone numbers  # noqa: RUF001
    ]

    settings = _settings()
    store = Store(settings.sqlite_path)
    since = utc_now() - timedelta(hours=hours)
    reports = store.recent_run_reports(since=since, limit=limit)

    if not reports:
        console.print(f"[yellow]검사할 리포트 없음 (최근 {hours}h)[/yellow]")
        return

    console.print(f"[bold]lint-report: {len(reports)}개 리포트 검사[/bold] (최근 {hours}h)")

    # Check scenario_history for LONG ≥ 80 coverage
    scenario_rows = store.recent_scenarios(since=since, side="LONG", min_score=80)
    long80_saved = len(scenario_rows)
    long80_with_price = sum(1 for r in scenario_rows if r.get("close_price_at_report") is not None)

    total_issues = 0
    for row in reports:
        digest = _read_report_field(row, "digest") or _read_report_field(row, "digest_text")
        analysis = _read_report_field(row, "analysis") or _read_report_field(row, "analysis_text")
        created_raw = _read_report_field(row, "created_at")
        created = created_raw[:16] if created_raw else "unknown"
        full_text = digest + "\n" + analysis

        row_issues: list[str] = []

        # 1. Noise header residuals
        for pat in _BROKER_HEADER_RES:
            for m in pat.finditer(full_text):
                ctx_s = max(0, m.start() - 30)
                ctx_e = min(len(full_text), m.end() + 30)
                snippet = full_text[ctx_s:ctx_e].replace("\n", " ").strip()
                row_issues.append(f"[yellow]노이즈헤더 잔류:[/yellow] ...{escape(snippet[:80])}...")
                break

        # 2. Broker as stock candidate false-positive
        if analysis and _BROKER_AS_CANDIDATE_RE.search(analysis):
            m2 = _BROKER_AS_CANDIDATE_RE.search(analysis)
            assert m2
            row_issues.append(
                f"[red]브로커 종목 오인:[/red] {escape(m2.group())} — 브로커명이 종목 후보로 표시됨"
            )

        # 2b. Broker name raw leak in report body
        m_broker = _BROKER_NAME_LEAK_RE.search(full_text)
        if m_broker:
            row_issues.append(
                f"[yellow]브로커명 잔류:[/yellow] '{escape(m_broker.group())}' — 리포트 본문에 브로커명 직접 노출"
            )

        # 3. Broker-header-only lines
        for line in full_text.splitlines():
            line = line.strip()
            if len(line) > 3 and is_broker_header_only(line):
                row_issues.append(f"[yellow]브로커헤더 잔류:[/yellow] {escape(line[:80])}")
            elif len(line) > 3 and is_low_quality_headline(line):
                row_issues.append(f"[dim]저품질 라인:[/dim] {escape(line[:80])}")

        # 4. Forbidden expressions
        for fw in _FORBIDDEN_WORDS:
            if fw in full_text:
                row_issues.append(f"[red]금지표현:[/red] '{fw}'")

        # 5. Metadata residuals
        for pat_str in [r"^link\s*:", r"^카테고리\s*:", r"^출처\s*:"]:
            if _re.search(pat_str, full_text, _re.IGNORECASE | _re.MULTILINE):
                row_issues.append(f"[yellow]메타데이터 잔류:[/yellow] {pat_str[:20]}")

        # 6. Phone / link noise in analysis reasons
        if analysis:
            for npat in _NOISE_PATTERNS:
                m3 = npat.search(analysis)
                if m3:
                    ctx_s = max(0, m3.start() - 20)
                    ctx_e = min(len(analysis), m3.end() + 20)
                    snippet = analysis[ctx_s:ctx_e].replace("\n", " ")
                    row_issues.append(f"[yellow]노이즈 문장:[/yellow] {escape(snippet[:80])}")
                    break

        # 7. SHORT gate violation: check for 상승 추세 + OBV 상승 near SHORT section
        if analysis:
            short_section = _re.search(r"🔴\s*숏.+?(?=🟡|🟢|─|$)", analysis, _re.DOTALL)
            if short_section:
                sblock = short_section.group()
                if "상승 추세" in sblock and "OBV: 상승" in sblock:
                    row_issues.append(
                        "[red]SHORT 게이트 위반:[/red] 상승 추세 + OBV 상승인데 숏 후보 표시"
                    )

        if row_issues:
            total_issues += 1
            console.rule(f"[bold]{created}[/bold]")
            for issue in row_issues[:12]:
                console.print(f"  {issue}")

    # Scenario history coverage check
    console.rule("[dim]scenario_history 커버리지[/dim]")
    console.print(f"  LONG ≥80 저장: {long80_saved}개 (가격 있음: {long80_with_price}개)")
    # Count how many reports have analysis with LONG section
    reports_with_long = sum(
        1 for r in reports if ("🟢 롱 관심 후보" in (_read_report_field(r, "analysis") or ""))
    )
    if reports_with_long > 0 and long80_saved == 0:
        console.print(
            f"  [red]⚠ LONG 섹션이 있는 리포트 {reports_with_long}개인데 scenario_history 저장 0[/red]"
        )
        console.print("  권장 조치: pipeline의 save_scenarios 호출 경로 확인")
    elif long80_saved > 0 and long80_with_price == 0:
        console.print(
            "  [yellow]⚠ 저장됐지만 가격 없음 → weekly 성과 리뷰 비어 있을 수 있음[/yellow]"
        )
    else:
        console.print("  [green]scenario_history OK[/green]")

    if total_issues == 0:
        console.print("[green]품질 이슈 없음 (문제 없음)[/green]")
    else:
        console.print(f"[bold red]{total_issues}/{len(reports)} 리포트에 품질 이슈[/bold red]")
        raise SystemExit(1)
