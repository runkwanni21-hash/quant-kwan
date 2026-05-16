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
                    entry_price = first_row.get("signal_price") or first_row.get(
                        "close_price_at_report"
                    )
                    if entry_price is None:
                        for r in rows_sorted:
                            ep = r.get("signal_price") or r.get("close_price_at_report")
                            if ep is not None:
                                first_row = r
                                entry_price = ep
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

        # SHORT ≥80 성과 — LONG과 같은 방식으로 빌드
        short_entries: list[dict] = []
        try:
            import yfinance as yf

            short_rows = store.recent_scenarios(since=since, side="SHORT", min_score=80)
            short_sym_all: dict[str, list[dict]] = {}
            for row in short_rows:
                sym = row.get("symbol", "")
                if sym:
                    short_sym_all.setdefault(sym, []).append(row)

            for sym, rows_for_sym in short_sym_all.items():
                rows_sorted = sorted(rows_for_sym, key=lambda r: r.get("created_at") or "")
                first_row = rows_sorted[0]
                entry_price = first_row.get("signal_price") or first_row.get(
                    "close_price_at_report"
                )
                if entry_price is None:
                    for r in rows_sorted:
                        ep = r.get("signal_price") or r.get("close_price_at_report")
                        if ep is not None:
                            first_row = r
                            entry_price = ep
                            break
                if entry_price is None:
                    continue
                mkt = "KR" if sym.endswith((".KS", ".KQ")) else "US"
                try:
                    hist = yf.Ticker(sym).history(period="2d", auto_adjust=True)
                    if hist.empty:
                        continue
                    current = float(hist["Close"].iloc[-1])
                    # SHORT 수익률: 신호가 > 현재가 = 적중
                    ret_pct = (entry_price - current) / entry_price * 100
                    short_entries.append(
                        {
                            "symbol": sym,
                            "name": first_row.get("name"),
                            "score": first_row.get("score", 0),
                            "entry_price": entry_price,
                            "current_price": current,
                            "return_pct": ret_pct,
                            "win": ret_pct > 0,
                            "created_at": first_row.get("created_at"),
                            "market": mkt,
                            "_source": "scenario_history",
                        }
                    )
                except Exception:
                    pass
            if short_entries:
                console.print(f"[weekly] short_entries={len(short_entries)}")
        except Exception as _se_exc:
            console.print(f"[yellow][weekly] short_entries build failed: {_se_exc}[/yellow]")

        # Theme board (KR + US 합본)
        weekly_theme_board: str | None = None
        try:
            from tele_quant.theme_board import build_theme_board

            kr_board = build_theme_board("KR", store, settings)
            us_board = build_theme_board("US", store, settings)
            weekly_theme_board = kr_board + "\n\n" + us_board
            console.print("[weekly] theme_board=ok")
        except Exception as _tb_exc:
            console.print(f"[yellow][weekly] theme_board failed: {_tb_exc}[/yellow]")

        # Load AI narrative history for weekly section
        weekly_narratives: list[dict] | None = None
        try:
            weekly_narratives = store.recent_narratives(since=since, limit=40)
            if weekly_narratives:
                console.print(f"[weekly] narrative_history: {len(weekly_narratives)} records")
        except Exception as _wn_exc:
            console.print(f"[yellow][weekly] narrative load failed: {_wn_exc}[/yellow]")

        # Load Fear & Greed history for weekly trend section
        weekly_fear_greed: list[dict] | None = None
        try:
            weekly_fear_greed = store.recent_fear_greed(since=since, limit=50)
            if weekly_fear_greed:
                console.print(f"[weekly] fear_greed_history: {len(weekly_fear_greed)} records")
        except Exception as _fg_exc:
            console.print(f"[yellow][weekly] fear_greed load failed: {_fg_exc}[/yellow]")

        summary = build_weekly_deterministic_summary(
            weekly_input,
            relation_feed_data=relation_feed_data,
            relation_signal_review=relation_signal_review,
            pair_watch_review=pair_watch_review,
            short_entries=short_entries if short_entries else None,
            narratives=weekly_narratives,
            fear_greed_history=weekly_fear_greed,
            daily_alpha_store=store,
            theme_board_section=weekly_theme_board,
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
    table = Table(title="Relation Feed Summary (자체 계산)")
    table.add_column("항목")
    table.add_column("값")
    table.add_row("기준일", summary.asof_date)
    table.add_row("생성일시", summary.generated_at)
    table.add_row("스캔 종목", str(summary.price_rows))
    table.add_row("급등락 모버", str(len(feed.movers)))
    table.add_row("상관관계 후보", str(len(fb)))
    if no_fallback:
        fallback_status = "생략 (--no-fallback)"
    elif fb:
        fallback_status = f"계산됨 ({len(fb)}건)"
    elif should_compute_fallback:
        fallback_status = "계산됨 (0건)"
    else:
        fallback_status = "후보 없음"
    table.add_row("lead-lag 계산", fallback_status)
    if fb:
        med = sum(1 for c in fb if c.confidence == "medium")
        low = sum(1 for c in fb if c.confidence == "low")
        table.add_row("신뢰도", f"medium={med} / low={low}")
    table.add_row("status", summary.status)
    if summary.warnings:
        table.add_row("warnings", ", ".join(summary.warnings))
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


@app.command("pair-watch-cleanup")
def pair_watch_cleanup_cmd(
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run/--apply", help="--dry-run: 변경 없이 통계만 / --apply: 실제 정리"),
    ] = True,
) -> None:
    """pair_watch_history 중복 제거 및 레거시 가격 미기록 row 정리.

    예:
      uv run tele-quant pair-watch-cleanup --dry-run
      uv run tele-quant pair-watch-cleanup --apply
    """
    from tele_quant.db import Store

    settings = _settings()
    store = Store(settings.sqlite_path)

    stats = store.pair_watch_cleanup_stats()

    console.print("\n[bold cyan]Pair-watch cleanup[/bold cyan]")
    console.print(f"  total rows (active):         {stats['total_active']}")
    console.print(f"  duplicate groups:            {stats['duplicate_groups']}")
    console.print(f"  archived duplicates (dry):   {stats['duplicate_rows_to_archive']}")
    console.print(f"  price missing:               {stats['price_missing']}")
    console.print(f"  unverified legacy:           {stats['unverified_legacy']}")

    if dry_run:
        console.print("\n[yellow]--dry-run 모드: DB 변경 없음. --apply 옵션으로 실행하세요.[/yellow]")
        return

    result = store.pair_watch_cleanup_apply()
    console.print("\n[bold green]cleanup --apply 완료[/bold green]")
    console.print(f"  archived duplicates:          {result['archived']}")
    console.print(f"  legacy_missing_price marked:  {result['legacy_marked']}")
    console.print(f"  exact backfilled:             {result['exact_backfilled']}")
    console.print(f"  nearest-day backfilled:       {result['nearest_backfilled']}")
    console.print(f"  failed (no historical price): {result['failed_backfill']}")
    console.print(f"  unverified remaining:         {result['unverified_remaining']}")


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


@app.command("theme-board")
def theme_board_cmd(
    market: Annotated[
        str, typer.Option("--market", help="KR 또는 US")
    ] = "KR",
    no_send: Annotated[
        bool, typer.Option("--no-send/--send", help="전송 없이 출력만")
    ] = True,
) -> None:
    """퀀터멘탈 테마 보드 — 급등/급락/수혜/피해/주도주/후발/과열 분류.

    Example: uv run tele-quant theme-board --market KR --no-send
             uv run tele-quant theme-board --market US --no-send
    """
    from tele_quant.db import Store
    from tele_quant.theme_board import build_theme_board

    settings = _settings()
    store = Store(settings.sqlite_path)
    report = build_theme_board(market.upper(), store, settings)
    console.print(report)

    if not no_send:
        import asyncio

        from tele_quant.telegram_sender import TelegramGateway, TelegramSender

        async def _send() -> None:
            async with TelegramGateway(settings) as gateway:
                sender = TelegramSender(settings, gateway=gateway)
                await sender.send(report)

        asyncio.run(_send())
        console.print("[green]theme-board 전송 완료[/green]")


@app.command("sector-cycle")
def sector_cycle_cmd(
    market: Annotated[
        str, typer.Option("--market", help="KR 또는 US")
    ] = "KR",
    no_send: Annotated[
        bool, typer.Option("--no-send/--send", help="전송 없이 출력만")
    ] = True,
) -> None:
    """Sector Cycle Rulebook v2 — 시장 자금 흐름 사이클 분석.

    Example: uv run tele-quant sector-cycle --market KR --no-send
             uv run tele-quant sector-cycle --market US --no-send
    """
    from tele_quant.db import Store
    from tele_quant.sector_cycle import build_sector_cycle_section

    settings = _settings()
    store = Store(settings.sqlite_path)
    report = build_sector_cycle_section(market.upper(), store, settings)
    console.print(report)

    if not no_send:
        import asyncio

        from tele_quant.telegram_sender import TelegramGateway, TelegramSender

        async def _send() -> None:
            async with TelegramGateway(settings) as gateway:
                sender = TelegramSender(settings, gateway=gateway)
                await sender.send(report)

        asyncio.run(_send())
        console.print("[green]sector-cycle 전송 완료[/green]")


@app.command("output-lint")
def output_lint_cmd(
    file: Annotated[
        str, typer.Option("--file", help="검사할 리포트 파일 경로 (없으면 stdin 대기)")
    ] = "",
    html: Annotated[
        str, typer.Option("--html", help="Telegram export HTML 파일 경로")
    ] = "",
    fail_on_high: Annotated[
        bool, typer.Option("--fail-on-high", help="HIGH 이슈 발견 시 exit-code 1")
    ] = False,
    last: Annotated[
        int, typer.Option("--last", help="HTML 모드에서 최근 N개 메시지만 검사 (0=전체)")
    ] = 0,
) -> None:
    """Daily Alpha / 4H 브리핑 리포트 출력 품질 검사.

    Example: uv run tele-quant output-lint --file /tmp/daily_alpha.log
             uv run tele-quant output-lint --html /path/to/messages.html --last 20
             uv run tele-quant daily-alpha --market KR --no-send | uv run tele-quant output-lint --file /dev/stdin
    """
    import re as _re
    from pathlib import Path as _Path

    from rich.table import Table

    # ── HTML 모드: Telegram export HTML 파싱 ──────────────────────────────────
    if html:
        try:
            html_content = _Path(html).read_text(encoding="utf-8", errors="ignore")
        except FileNotFoundError:
            console.print(f"[red]HTML 파일 없음: {html}[/red]")
            raise SystemExit(1) from None

        # Extract message text from Telegram HTML export
        # Format: <div class="text">...</div> or <div class="body">...</div>
        msg_texts: list[tuple[str, str]] = []  # (msg_id_or_ts, text)
        _msg_id_re = _re.compile(r'<div class="message[^"]*"\s+id="message(\d+)"', _re.IGNORECASE)
        _text_re = _re.compile(r'<div class="text">(.*?)</div>', _re.IGNORECASE | _re.DOTALL)
        _date_re = _re.compile(r'<div class="date[^"]*"[^>]*title="([^"]+)"', _re.IGNORECASE)
        _tag_re = _re.compile(r"<[^>]+>")

        # Split by message blocks
        msg_blocks = _re.split(r'(?=<div class="message)', html_content)
        for block in msg_blocks:
            mid_m = _msg_id_re.search(block)
            msg_id = mid_m.group(1) if mid_m else "?"
            date_m = _date_re.search(block)
            ts = date_m.group(1) if date_m else ""
            text_m = _text_re.search(block)
            if text_m:
                raw = text_m.group(1)
                clean = _tag_re.sub("", raw).strip()
                if clean:
                    msg_texts.append((f"msg#{msg_id}({ts})", clean))

        if last > 0:
            msg_texts = msg_texts[-last:]

        if not msg_texts:
            console.print("[yellow]HTML에서 메시지 텍스트를 찾지 못했습니다.[/yellow]")
            return

        console.print(f"[dim]HTML 모드: {len(msg_texts)}개 메시지 검사[/dim]")
        text = "\n".join(t for _, t in msg_texts)
    elif file:
        # 검사 대상 텍스트 로드
        try:
            text = _Path(file).read_text(encoding="utf-8", errors="ignore")
        except FileNotFoundError:
            console.print(f"[red]파일 없음: {file}[/red]")
            raise SystemExit(1) from None
    else:
        import sys
        text = sys.stdin.read()

    lines_raw = text.splitlines()

    # ── 검사 규칙 ──────────────────────────────────────────────────────────────
    issues: list[dict[str, str]] = []

    def _check(severity: str, pattern: str, message: str, *, regex: bool = False) -> None:
        for ln, line in enumerate(lines_raw, 1):
            hit = (
                _re.search(pattern, line, _re.IGNORECASE)
                if regex else pattern in line
            )
            if hit:
                issues.append({
                    "severity": severity, "line": str(ln),
                    "pattern": pattern, "message": message,
                    "excerpt": line.strip()[:80],
                })

    # HIGH: 절대 출력 금지 메타 노이즈
    _check("HIGH", "Web발신", "Web발신 노이즈 잔류")
    _check("HIGH", "보고서링크:", "보고서링크 메타 잔류")
    _check("HIGH", "국장 마이너리티 리포트", "채널명 헤더 잔류")
    _check("HIGH", "안녕하세요", "브로커 인사말 잔류")
    _check("HIGH", r"IB\s*투자의견", "IB 투자의견 헤더 잔류", regex=True)
    _check("HIGH", r"글로벌\s*투자\s*구루\s*일일\s*브리핑", "글로벌 투자 구루 채널 헤더 잔류", regex=True)
    _check("HIGH", r"월가\s*주요\s*뉴스", "월가 주요 뉴스 헤더 잔류", regex=True)
    _check("HIGH", r"이익동향\s*\(\d+월\s*\d+주차\)", "이익동향 메타 헤더 잔류", regex=True)
    _check("HIGH", r"^   왜 지금: (?:치 |드 |이를 )", "왜지금 문장 조각", regex=True)
    # 줄 시작 조각 문장 — headline_cleaner를 우회한 fragment
    _check("HIGH", r"^치 후 |^드 플|^이를 정당화", "줄 시작 조각 문장 잔류", regex=True)
    # 잘못된 섹션 표기
    _check("HIGH", "숏/매도 경계 후보", "숏/매도 경계 후보 표기 오류 — SHORT 관찰 후보·관망 표기여야 함")
    _check("HIGH", "현재가 확인 불가", "현재가 확인 불가 텍스트 직접 노출 — 접힘 처리 누락")
    # unknown_price_only source가 연결고리 생성에 쓰인 경우
    _check(
        "HIGH",
        r"가격만 움직임\(이유 불명\).*연결고리",
        "unknown_price_only source가 연결고리 생성에 사용됨",
        regex=True,
    )
    # 라이브 확인 미실행 상세 반복 (2회 이상 = 접힘 처리 누락)
    _live_unconf_lines = [
        ln for ln, line_txt in enumerate(lines_raw, 1)
        if "라이브 확인 미실행 — 통계만 참고" in line_txt
    ]
    if len(_live_unconf_lines) >= 2:
        issues.append({
            "severity": "HIGH",
            "line": str(_live_unconf_lines[1]),
            "pattern": "라이브 확인 미실행 상세 반복",
            "message": f"라이브 확인 미실행 — 통계만 참고 {len(_live_unconf_lines)}회 반복 — 접힘 처리 누락",
            "excerpt": lines_raw[_live_unconf_lines[1] - 1].strip()[:80],
        })

    # HIGH: 가격 스케일 이상 (삼성전자 등 KR 대형주 과거 미분할 가격)
    for suspicious_bb in ["BB.*311,0", "BB.*2,160,", "BB.*755,6", "BB.*1,715,"]:
        _check("HIGH", suspicious_bb, "기술지표 가격 스케일 이상 (미분할 추정)", regex=True)

    # HIGH: pair-watch 방향 불일치
    _check("HIGH", r"4H -[1-9]\d?\.\d%.*급등 후", "음수 source에 급등 후 표현", regex=True)
    _check("HIGH", r"1D -[1-9]\d?\.\d%.*급등 후", "음수 1D source에 급등 후 표현", regex=True)
    _check("HIGH", r"4H \+[1-9]\d?\.\d%.*급락 후", "양수 source에 급락 후 표현", regex=True)
    _check("HIGH", r"1D \+[1-9]\d?\.\d%.*급락 후", "양수 1D source에 급락 후 표현", regex=True)

    # MEDIUM: 점수 구간 혼란 — 관망/추적 후보가 정식 후보 섹션에 없어야 함
    in_main_section = False
    for ln, line in enumerate(lines_raw, 1):
        if "LONG 관찰 후보" in line or "SHORT 관찰 후보" in line:
            in_main_section = True
        if "관망/추적 후보" in line or "⚠" in line:
            in_main_section = False
        if in_main_section:
            m = _re.search(r"최종점수:\s*(5\d+\.\d)", line)
            if m:
                issues.append({
                    "severity": "MEDIUM", "line": str(ln),
                    "pattern": "50점대 정식 후보",
                    "message": f"50점대({m.group(1)}) 후보가 정식 관찰 후보 섹션에 있음",
                    "excerpt": line.strip()[:80],
                })
            m2 = _re.search(r"최종점수:\s*6[0-9]\.\d", line)
            if m2:
                issues.append({
                    "severity": "MEDIUM", "line": str(ln),
                    "pattern": "60점대 정식 후보",
                    "message": "60점대 후보가 정식 관찰 후보 섹션에 있음 — 추적 후보여야 함",
                    "excerpt": line.strip()[:80],
                })

    # MEDIUM: 가격 스케일 불일치 후보가 정식 후보 섹션에 표시되는 경우
    _check("MEDIUM", "기술데이터 스케일 불일치", "가격 스케일 불일치 후보 출력 중", regex=False)

    # MEDIUM: 원/달러 환율 중복
    krw_lines = [ln for ln, line_text in enumerate(lines_raw, 1) if "원/달러" in line_text]
    if len(krw_lines) >= 2:
        issues.append({
            "severity": "MEDIUM", "line": str(krw_lines[1]),
            "pattern": "원/달러 환율 중복",
            "message": f"원/달러 환율 {len(krw_lines)}회 출력 — 중복 제거 필요",
            "excerpt": lines_raw[krw_lines[1] - 1].strip()[:80],
        })

    # LOW: 기타 노이즈
    _check("LOW", r"^   왜 지금: .*Report\s*\)", "Report) 메타 태그 왜지금에 잔류", regex=True)
    _check("LOW", r"근거: 약함", "증거 품질 WEAK 후보 출력 중", regex=True)
    _check("LOW", r"근거: 제거", "증거 품질 REJECT 후보 출력 중", regex=True)

    # ── 결과 출력 ──────────────────────────────────────────────────────────────
    if not issues:
        console.print("[green]output-lint: 이슈 없음[/green]")
        return

    table = Table(title="output-lint 결과", show_lines=True)
    table.add_column("심각도", style="bold", min_width=6)
    table.add_column("라인", min_width=5)
    table.add_column("메시지", min_width=25)
    table.add_column("발췌", min_width=40)

    _colors = {"HIGH": "red", "MEDIUM": "yellow", "LOW": "cyan"}
    for row in sorted(issues, key=lambda x: (x["severity"], int(x["line"]))):
        color = _colors.get(row["severity"], "white")
        table.add_row(
            f"[{color}]{row['severity']}[/{color}]",
            row["line"], row["message"], row["excerpt"],
        )
    console.print(table)

    high_count = sum(1 for i in issues if i["severity"] == "HIGH")
    console.print(f"총 이슈: {len(issues)}개 (HIGH {high_count}개)")
    if fail_on_high and high_count > 0:
        raise SystemExit(1)


@app.command("sector-cycle-audit")
def sector_cycle_audit_cmd(
    fail_on_high: Annotated[
        bool, typer.Option("--fail-on-high", help="HIGH 심각도 이슈 발견 시 exit-code 1")
    ] = False,
) -> None:
    """sector_cycle_rules.yml 심볼/이름 유효성 감사.

    Example: uv run tele-quant sector-cycle-audit
             uv run tele-quant sector-cycle-audit --fail-on-high
    """
    import csv
    import re
    from pathlib import Path as _Path

    from rich.table import Table

    from tele_quant.sector_cycle import load_sector_cycle_rules

    rules = load_sector_cycle_rules()

    # Build flat list: (cycle_id, stage, symbol, name)
    # source_symbols: [{symbol, name}, ...]
    # beneficiaries/victims: [{sector, connection, symbols: [{symbol, name}]}, ...]
    _flat_keys = [("source_symbols", "SOURCE")]
    _nested_keys = [
        ("first_order_beneficiaries", "FIRST"),
        ("second_order_beneficiaries", "SECOND"),
        ("third_order_beneficiaries", "THIRD"),
        ("victims", "VICTIM"),
    ]
    entries: list[tuple[str, str, str, str]] = []
    for rule in rules:
        cid = rule.get("cycle_id", "")
        for key, stage in _flat_keys:
            for item in rule.get(key, []):
                entries.append((cid, stage, item.get("symbol", ""), item.get("name", "")))
        for key, stage in _nested_keys:
            for sector_group in rule.get(key, []):
                for item in sector_group.get("symbols", []):
                    entries.append((cid, stage, item.get("symbol", ""), item.get("name", "")))

    # Validation passes
    issues: list[dict[str, str]] = []
    _kr_pattern = re.compile(r"^\d{6}\.(KS|KQ)$")
    _us_pattern = re.compile(r"^[A-Z]{1,5}$")

    # Track (symbol, name) mapping for duplicate-name-mismatch detection
    sym_names: dict[str, set[str]] = {}
    for _cid, _stage, sym, name in entries:
        if sym:
            sym_names.setdefault(sym, set()).add(name)

    for cid, stage, sym, name in entries:
        if not sym:
            issues.append({"severity": "HIGH", "cycle_id": cid, "stage": stage,
                           "symbol": sym, "name": name, "issue": "symbol 비어있음"})
            continue
        if not name:
            issues.append({"severity": "MEDIUM", "cycle_id": cid, "stage": stage,
                           "symbol": sym, "name": name, "issue": "name 비어있음"})

        is_kr = sym.endswith((".KS", ".KQ"))
        is_us = _us_pattern.match(sym) is not None
        if not is_kr and not is_us:
            issues.append({"severity": "HIGH", "cycle_id": cid, "stage": stage,
                           "symbol": sym, "name": name, "issue": "심볼 형식 오류 (KR: 6자리.KS/.KQ, US: 대문자)"})
        elif is_kr and not _kr_pattern.match(sym):
            issues.append({"severity": "HIGH", "cycle_id": cid, "stage": stage,
                           "symbol": sym, "name": name, "issue": "KR 심볼 6자리 아님"})

        names_for_sym = sym_names.get(sym, set())
        if len(names_for_sym) > 1:
            issues.append({"severity": "MEDIUM", "cycle_id": cid, "stage": stage,
                           "symbol": sym, "name": name,
                           "issue": f"같은 심볼에 다른 이름: {', '.join(sorted(names_for_sym))}"})

    # Display
    if not issues:
        console.print("[green]sector-cycle-audit: 이슈 없음[/green]")
        return

    table = Table(title="sector-cycle-audit 결과", show_lines=True)
    table.add_column("심각도", style="bold", min_width=6)
    table.add_column("cycle_id", min_width=20)
    table.add_column("stage", min_width=8)
    table.add_column("symbol", min_width=14)
    table.add_column("name", min_width=16)
    table.add_column("이슈", min_width=30)

    _colors = {"HIGH": "red", "MEDIUM": "yellow", "LOW": "cyan"}
    for row in sorted(issues, key=lambda x: (x["severity"], x["cycle_id"])):
        color = _colors.get(row["severity"], "white")
        table.add_row(
            f"[{color}]{row['severity']}[/{color}]",
            row["cycle_id"], row["stage"], row["symbol"], row["name"], row["issue"],
        )
    console.print(table)

    # CSV output
    out_dir = _Path("data/diagnostics")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "sector_cycle_audit_latest.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["severity", "cycle_id", "stage", "symbol", "name", "issue"])
        writer.writeheader()
        writer.writerows(issues)
    console.print(f"[dim]CSV 저장: {out_path}[/dim]")

    high_count = sum(1 for i in issues if i["severity"] == "HIGH")
    console.print(f"총 이슈: {len(issues)}개 (HIGH {high_count}개)")
    if fail_on_high and high_count > 0:
        raise SystemExit(1)


@app.command("ops-doctor")
def ops_doctor() -> None:
    """자동 실행 상태와 DB 최신성을 진단합니다.

    Example: uv run tele-quant ops-doctor
    """
    import shutil
    import subprocess
    from datetime import timedelta
    from pathlib import Path as _Path
    from zoneinfo import ZoneInfo

    from rich.table import Table

    from tele_quant.db import Store
    from tele_quant.models import utc_now

    KST = ZoneInfo("Asia/Seoul")

    def _kst(dt: object) -> str:
        from datetime import datetime as _dt

        if isinstance(dt, _dt):
            return dt.astimezone(KST).strftime("%Y-%m-%d %H:%M KST")
        return str(dt)

    def _run(cmd: list[str]) -> tuple[str, int]:
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            return r.stdout.strip() + r.stderr.strip(), r.returncode
        except Exception as exc:
            return str(exc), -1

    now_kst = _kst(utc_now())
    console.print(f"\n[bold cyan]Tele Quant Ops Doctor[/bold cyan]  {now_kst}\n")

    has_systemd = shutil.which("systemctl") is not None

    # --- Timer status ---
    timer_table = Table(title="systemd Timers")
    timer_table.add_column("Timer")
    timer_table.add_column("Active")
    timer_table.add_column("Enabled")
    timer_table.add_column("Next Trigger")
    timer_table.add_column("Status")

    _TIMERS = [
        "tele-quant-weekday.timer",
        "tele-quant-weekend-macro.timer",
        "tele-quant-weekly.timer",
        "tele-quant-pair-watch-cleanup.timer",
    ]

    timer_ok = True
    for timer in _TIMERS:
        if not has_systemd:
            timer_table.add_row(timer, "N/A", "N/A", "N/A", "[yellow]WARN: systemd 없음[/yellow]")
            timer_ok = False
            continue
        active_out, _ = _run(["systemctl", "--user", "is-active", timer])
        enabled_out, _ = _run(["systemctl", "--user", "is-enabled", timer])
        active = active_out.strip()
        enabled = enabled_out.strip()
        # Next trigger
        next_out, _ = _run(
            ["systemctl", "--user", "show", timer, "--property=NextElapseUSecRealtime"]
        )
        next_str = "알 수 없음"
        for part in next_out.split("=", 1)[1:]:
            val = part.strip()
            if val and val != "0":
                try:
                    import datetime

                    usec = int(val)
                    dt_utc = datetime.datetime(
                        1970, 1, 1, tzinfo=datetime.UTC
                    ) + datetime.timedelta(microseconds=usec)
                    next_str = _kst(dt_utc)
                except Exception:
                    next_str = val

        if active == "active" and enabled == "enabled":
            st = "[green]OK[/green]"
        elif active != "active":
            st = "[red]FAIL: inactive[/red]"
            timer_ok = False
        else:
            st = "[yellow]WARN: not enabled[/yellow]"
            timer_ok = False
        timer_table.add_row(timer, active, enabled, next_str, st)

    console.print(timer_table)

    # --- Recent service log ---
    if has_systemd:
        svc_out, _ = _run(
            ["journalctl", "--user", "-u", "tele-quant-weekday.service", "-n", "20", "--no-pager"]
        )
        if svc_out:
            console.rule("[dim]최근 weekday service 로그 (20줄)[/dim]")
            console.print(f"[dim]{svc_out[:2000]}[/dim]")

    # --- DB diagnostics ---
    settings = _settings()
    db_path = settings.sqlite_path
    db_exists = db_path.exists()

    db_table = Table(title="DB 상태")
    db_table.add_column("항목")
    db_table.add_column("값")
    db_table.add_column("판정")

    db_table.add_row(
        "SQLITE_PATH",
        str(db_path),
        "[green]exists[/green]" if db_exists else "[red]FAIL: 없음[/red]",
    )

    env_local = _Path(".env.local")
    db_table.add_row(
        ".env.local",
        "존재함" if env_local.exists() else "없음",
        "[green]OK[/green]" if env_local.exists() else "[yellow]WARN[/yellow]",
    )

    last_run_age_h: float | None = None
    run_report_status = "[red]FAIL: 없음[/red]"
    _pw_unverified: int = 0
    _pw_unverified_oldest_h: float = 0.0
    if db_exists:
        try:
            store = Store(db_path)
            since = utc_now() - timedelta(hours=168)
            reports = store.recent_run_reports(since=since, limit=5)
            if reports:
                last_rpt = reports[0]
                last_at = last_rpt.created_at
                age_h = (utc_now() - last_at).total_seconds() / 3600
                last_run_age_h = age_h
                age_label = f"{age_h:.1f}h"
                if age_h <= 6:
                    run_report_status = f"[green]OK ({age_label})[/green]"
                elif age_h <= 12:
                    run_report_status = f"[yellow]WARN ({age_label})[/yellow]"
                else:
                    run_report_status = f"[red]FAIL ({age_label})[/red]"
                db_table.add_row("마지막 run_report", _kst(last_at), run_report_status)

                # Recent 5
                for i, rpt in enumerate(reports[:5], 1):
                    db_table.add_row(
                        f"  run_report #{i}",
                        _kst(rpt.created_at),
                        rpt.mode or "unknown",
                    )
            else:
                db_table.add_row("마지막 run_report", "없음", "[red]FAIL[/red]")
        except Exception as exc:
            db_table.add_row("DB 연결", str(exc)[:60], "[red]ERROR[/red]")

        # pair_watch_history latest
        try:
            store2 = Store(db_path)
            pw_rows = store2.recent_pair_watch_signals(since=utc_now() - timedelta(hours=168))
            if pw_rows:
                from tele_quant.models import parse_dt

                pw_last = max(r.get("created_at", "") for r in pw_rows)
                pw_dt = parse_dt(pw_last)
                pw_age = (utc_now() - pw_dt).total_seconds() / 3600 if pw_dt else 999
                db_table.add_row(
                    "pair_watch_history 최근",
                    _kst(pw_dt) if pw_dt else "알 수 없음",
                    f"[dim]{pw_age:.1f}h 전[/dim]",
                )
            else:
                db_table.add_row("pair_watch_history 최근", "없음", "[dim]저장 없음[/dim]")
        except Exception:
            pass

        # pair_watch cleanup state
        try:
            store_pw = Store(db_path)
            pw_stats = store_pw.pair_watch_cleanup_stats()
            _pw_unverified = pw_stats.get("unverified_legacy", 0)
            with store_pw.connect() as _conn:
                _exact = _conn.execute(
                    "SELECT COUNT(*) FROM pair_watch_history"
                    " WHERE backfill_source='exact_date_close'"
                    " AND (archived IS NULL OR archived=0)"
                ).fetchone()[0]
                _nearest = _conn.execute(
                    "SELECT COUNT(*) FROM pair_watch_history"
                    " WHERE backfill_source='nearest_trading_day_close'"
                    " AND (archived IS NULL OR archived=0)"
                ).fetchone()[0]
                _failed = _conn.execute(
                    "SELECT COUNT(*) FROM pair_watch_history"
                    " WHERE backfill_source='failed_no_price'"
                    " AND (archived IS NULL OR archived=0)"
                ).fetchone()[0]
                _archived_cnt = _conn.execute(
                    "SELECT COUNT(*) FROM pair_watch_history WHERE archived=1"
                ).fetchone()[0]
                _oldest_unverified_row = _conn.execute(
                    "SELECT created_at FROM pair_watch_history"
                    " WHERE backfill_status='unverified_legacy_backfill'"
                    " AND (archived IS NULL OR archived=0)"
                    " ORDER BY created_at ASC LIMIT 1"
                ).fetchone()
            if _oldest_unverified_row and _oldest_unverified_row[0]:
                from tele_quant.models import parse_dt as _parse_dt2
                _ov_dt = _parse_dt2(_oldest_unverified_row[0])
                _pw_unverified_oldest_h = (
                    (utc_now() - _ov_dt).total_seconds() / 3600 if _ov_dt else 0.0
                )
            if _pw_unverified == 0:
                pw_cleanup_status = "[green]OK — unverified 0개[/green]"
            elif _pw_unverified_oldest_h <= 24:
                pw_cleanup_status = (
                    f"[yellow]WARN: unverified {_pw_unverified}개"
                    f" (최대 {_pw_unverified_oldest_h:.0f}h) — 장 마감 후 재실행[/yellow]"
                )
            else:
                pw_cleanup_status = (
                    f"[red]FAIL: unverified {_pw_unverified}개"
                    f" ({_pw_unverified_oldest_h:.0f}h 방치)[/red]"
                )
            db_table.add_row(
                "pair-watch cleanup",
                f"exact={_exact} / nearest={_nearest} / failed={_failed}"
                f" / unverified={_pw_unverified} / archived={_archived_cnt}",
                pw_cleanup_status,
            )
        except Exception:
            pass

        # scenario_history latest + 이유 진단 + WARN if stale
        _sc_age_h: float = 0.0
        _sc_warn_reason: str = ""
        try:
            store3 = Store(db_path)
            sc_rows = store3.recent_scenarios(since=utc_now() - timedelta(hours=168))
            if sc_rows:
                from tele_quant.models import parse_dt

                sc_last = max(r.get("created_at", "") for r in sc_rows)
                sc_dt = parse_dt(sc_last)
                sc_age = (utc_now() - sc_dt).total_seconds() / 3600 if sc_dt else 999
                _sc_age_h = sc_age
                sent_rows = [r for r in sc_rows if r.get("sent") == 1]
                sent_high = [r for r in sent_rows if r.get("score", 0) >= 80]
                sc_note = f"{sc_age:.1f}h 전 (전체 {len(sc_rows)}개"
                if sent_rows:
                    sc_note += f", sent={len(sent_rows)}"
                if sent_high:
                    sc_note += f", 80+ sent={len(sent_high)}"
                sc_note += ")"

                # 판정: sent=1 & 80+ 있으면 OK, 오래됐으면 WARN 이유 표시
                run_rows_24h = store3.recent_run_reports(since=utc_now() - timedelta(hours=24))
                sent_runs_24h = [
                    r for r in run_rows_24h if (getattr(r, "stats", None) or {}).get("sent")
                ]
                if sc_age > 24:
                    if not sent_runs_24h:
                        sc_status = "[dim]OK — no-send/no_llm preview만 실행됨[/dim]"
                    elif not sent_high:
                        _sc_warn_reason = "80점 이상 후보 없음 (sent 실행 있음)"
                        sc_status = f"[yellow]WARN: {sc_age:.0f}h — {_sc_warn_reason}[/yellow]"
                    else:
                        sc_status = f"[dim]{sc_note}[/dim]"
                else:
                    sc_status = f"[green]OK ({sc_age:.1f}h)[/green]"

                db_table.add_row(
                    "scenario_history 최근",
                    _kst(sc_dt) if sc_dt else "알 수 없음",
                    sc_status,
                )
            else:
                # 이유 진단: 왜 비어 있는가?
                run_rows_24h = store3.recent_run_reports(since=utc_now() - timedelta(hours=24))
                sent_runs = [
                    r for r in run_rows_24h if (getattr(r, "stats", None) or {}).get("sent")
                ]
                if not sent_runs:
                    sc_reason = "no-send 모드만 실행됨 (send=false)"
                    sc_status_empty = f"[dim]{sc_reason}[/dim]"
                else:
                    sc_reason = "80점 이상 후보 없음 (sent 실행은 있음)"
                    _sc_warn_reason = sc_reason
                    sc_status_empty = f"[yellow]WARN: {sc_reason}[/yellow]"
                db_table.add_row("scenario_history 최근", "없음", sc_status_empty)
        except Exception:
            pass

    console.print(db_table)

    # --- Recommendations ---
    console.rule("[dim]진단 결과 및 권장 조치[/dim]")
    recs: list[str] = []

    if not timer_ok:
        recs.append(
            "[red]FAIL: timer가 inactive 또는 disabled[/red]\n"
            "  권장 조치:\n"
            "    mkdir -p ~/.config/systemd/user\n"
            "    cp systemd/tele-quant-weekday.service ~/.config/systemd/user/\n"
            "    cp systemd/tele-quant-weekday.timer ~/.config/systemd/user/\n"
            "    cp systemd/tele-quant-weekend-macro.service ~/.config/systemd/user/\n"
            "    cp systemd/tele-quant-weekend-macro.timer ~/.config/systemd/user/\n"
            "    cp systemd/tele-quant-weekly.service ~/.config/systemd/user/\n"
            "    cp systemd/tele-quant-weekly.timer ~/.config/systemd/user/\n"
            "    cp systemd/tele-quant-pair-watch-cleanup.service ~/.config/systemd/user/\n"
            "    cp systemd/tele-quant-pair-watch-cleanup.timer ~/.config/systemd/user/\n"
            "    systemctl --user daemon-reload\n"
            "    systemctl --user enable --now tele-quant-weekday.timer\n"
            "    systemctl --user enable --now tele-quant-weekend-macro.timer\n"
            "    systemctl --user enable --now tele-quant-weekly.timer\n"
            "    systemctl --user enable --now tele-quant-pair-watch-cleanup.timer"
        )

    if _pw_unverified > 0 and _pw_unverified_oldest_h > 24:
        recs.append(
            f"[red]FAIL: pair-watch unverified legacy {_pw_unverified}개"
            f" ({_pw_unverified_oldest_h:.0f}h 방치)[/red]\n"
            "  pair-watch-cleanup --apply를 실행하거나 timer 동작을 확인하세요.\n"
            "  수동 실행: uv run tele-quant pair-watch-cleanup --apply"
        )
    elif _pw_unverified > 0:
        recs.append(
            f"[yellow]WARN: pair-watch unverified {_pw_unverified}개"
            f" — 장 마감 후 자동 정리 예정[/yellow]\n"
            "  장 중이거나 당일 미개장 종목일 수 있음.\n"
            "  즉시 정리: uv run tele-quant pair-watch-cleanup --apply"
        )

    if last_run_age_h is not None and last_run_age_h > 12:
        recs.append(
            "[red]FAIL: 마지막 run_report가 12시간 초과[/red]\n"
            "  수동 실행: DIGEST_MODE=no_llm uv run tele-quant once --no-send\n"
            "  WSL이 꺼져 있었다면 systemd timer missed run 가능성 있음\n"
            "  → WSL을 켜두거나 Persistent=true 확인"
        )
    elif last_run_age_h is not None and last_run_age_h > 6:
        recs.append(
            "[yellow]WARN: 마지막 run_report가 6~12시간 전[/yellow]\n"
            "  정상 범위이나 4H 주기 대비 약간 늦음. timer 상태 확인 권장"
        )

    # scenario_history WARN — sent=True 리포트 있지만 80점 이상 후보 없음이 5일+ 지속
    if _sc_warn_reason and _sc_age_h > 120:
        recs.append(
            f"[yellow]WARN: scenario_history {_sc_age_h:.0f}h ({_sc_warn_reason})[/yellow]\n"
            "  5일 이상 LONG/SHORT 80점 이상 신호 없음 — direct evidence gate 과도 가능성\n"
            "  진단: uv run tele-quant lint-report --hours 24"
        )

    # Sentiment history freshness check
    try:
        sentiment_rows = store.recent_sentiment_history(since=utc_now() - timedelta(hours=12))
        if not sentiment_rows:
            console.print("- sentiment_history: 최근 12h 없음 (fast/no_llm 미실행 또는 첫 실행)")
        else:
            latest_sent = sentiment_rows[0]
            from tele_quant.models import parse_dt
            sent_dt = parse_dt(latest_sent.get("created_at") or "")
            if sent_dt:
                sent_age_h = (utc_now() - sent_dt).total_seconds() / 3600
                sector_counts: dict[str, int] = {}
                for row in sentiment_rows:
                    sec = row.get("sector") or "Unknown"
                    sector_counts[sec] = sector_counts.get(sec, 0) + 1
                top_sectors = sorted(sector_counts, key=lambda s: -sector_counts[s])[:3]
                console.print(
                    f"- sentiment_history: {sent_age_h:.1f}h 전 업데이트 "
                    f"({len(sentiment_rows)}건, 섹터: {', '.join(top_sectors)})"
                )
                if sent_age_h > 8:
                    recs.append(
                        f"[yellow]WARN: sentiment_history {sent_age_h:.0f}h 전 (8h 초과)[/yellow]\n"
                        "  fast 모드 리포트가 실행되지 않았을 수 있음"
                    )
    except Exception:
        pass

    # --- External indicators diagnostics ---
    console.rule("[dim]외부 지표 진단[/dim]")
    ext_settings = _settings()
    # FRED API 키
    fred_key = getattr(ext_settings, "fred_api_key", "")
    if fred_key:
        console.print("[green]FRED_API_KEY: 설정됨[/green]")
    else:
        console.print("[yellow]FRED_API_KEY: 미설정 (yfinance fallback 사용 중)[/yellow]")
        recs.append(
            "[yellow]INFO: FRED_API_KEY 미설정[/yellow]\n"
            "  .env.local에 FRED_API_KEY=your_key 추가하면 연준 공식 금리/실업률 데이터 수집\n"
            "  무료 발급: https://fred.stlouisfed.org/docs/api/api_key.html"
        )
    # Fear & Greed 최근 기록
    if db_exists:
        try:
            fg_rows = store.recent_fear_greed(since=utc_now() - timedelta(hours=12))
            if fg_rows:
                from tele_quant.models import parse_dt as _parse_dt
                fg_latest_dt = _parse_dt(fg_rows[0].get("created_at") or "")
                fg_age_h = (utc_now() - fg_latest_dt).total_seconds() / 3600 if fg_latest_dt else 999
                fg_score = fg_rows[0].get("score")
                fg_rating = fg_rows[0].get("rating_ko") or fg_rows[0].get("rating") or ""
                console.print(
                    f"Fear&Greed 최근: {fg_score:.0f}/100 [{fg_rating}]"
                    f"  ({fg_age_h:.1f}h 전)"
                )
                if fg_age_h > 8:
                    recs.append(
                        f"[yellow]WARN: Fear&Greed {fg_age_h:.0f}h 전 (8h 초과)[/yellow]\n"
                        "  fear_greed_enabled=false 또는 네트워크 문제일 수 있음"
                    )
            else:
                console.print("[dim]Fear&Greed: 최근 12h 기록 없음 (첫 실행 또는 비활성화)[/dim]")
        except Exception:
            pass
    # EIA 에너지 API 키
    eia_key = getattr(ext_settings, "eia_api_key", "")
    if eia_key:
        console.print("[green]EIA_API_KEY: 설정됨 (WTI/천연가스 실시간 가격)[/green]")
    else:
        console.print("[dim]EIA_API_KEY: 미설정 (에너지 가격 비활성화)[/dim]")

    # ECOS 한국은행 API 키
    ecos_key = getattr(ext_settings, "ecos_api_key", "")
    if ecos_key:
        console.print("[green]ECOS_API_KEY: 설정됨 (한국은행 기준금리/환율)[/green]")
    else:
        console.print(
            "[dim]ECOS_API_KEY: 미설정 (한국은행 데이터 비활성화)\n"
            "  .env.local에 ECOS_API_KEY=your_key 추가 — 무료: https://ecos.bok.or.kr[/dim]"
        )

    # RSS 뉴스
    rss_ok = getattr(ext_settings, "rss_enabled", True)
    _rss_col = "green" if rss_ok else "dim"
    _rss_lbl = "활성화" if rss_ok else "비활성화"
    console.print(
        f"[{_rss_col}]RSS 뉴스: {_rss_lbl}"
        f" (PR Newswire / GlobeNewswire / BusinessWire / Google News)[/{_rss_col}]"
    )

    # SEC EDGAR
    sec_ok = getattr(ext_settings, "sec_enabled", True)
    _sec_col = "green" if sec_ok else "dim"
    _sec_lbl = "활성화" if sec_ok else "비활성화"
    console.print(
        f"[{_sec_col}]SEC EDGAR 8-K: {_sec_lbl} (미국 주식 직접증거)[/{_sec_col}]"
    )

    # ECB / Frankfurter
    ecb_ok = getattr(ext_settings, "ecb_enabled", True)
    fr_ok = getattr(ext_settings, "frankfurter_enabled", True)
    console.print(
        f"ECB 금리: {'[green]활성화[/green]' if ecb_ok else '[dim]비활성화[/dim]'}"
        f"  Frankfurter 환율: {'[green]활성화[/green]' if fr_ok else '[dim]비활성화[/dim]'}"
    )

    # OpenDART
    dart_ok = getattr(ext_settings, "opendart_enabled", True)
    dart_key = bool(getattr(ext_settings, "opendart_api_key", ""))
    if dart_ok and dart_key:
        console.print("[green]OpenDART: 활성화 + API 키 설정됨 (한국 공시)[/green]")
    elif dart_ok:
        console.print("[yellow]OpenDART: 활성화 — API 키 미설정 (OPENDART_API_KEY 필요)[/yellow]")
        recs.append("[yellow]WARN OpenDART: OPENDART_API_KEY 설정 시 한국 공시 수집 가능[/yellow]")
    else:
        console.print("[dim]OpenDART: 비활성화[/dim]")

    # Finnhub
    fh_ok = getattr(ext_settings, "finnhub_enabled", True)
    fh_key = bool(getattr(ext_settings, "finnhub_api_key", ""))
    if fh_ok and fh_key:
        console.print("[green]Finnhub: 활성화 + API 키 설정됨 (미국 뉴스 + 경제 캘린더)[/green]")
    elif fh_ok:
        console.print("[yellow]Finnhub: 활성화 — API 키 미설정 (FINNHUB_API_KEY 필요)[/yellow]")
        recs.append("[yellow]WARN Finnhub: FINNHUB_API_KEY 설정 시 미국 뉴스 + 경제 캘린더 활성화[/yellow]")
    else:
        console.print("[dim]Finnhub: 비활성화[/dim]")

    # pytrends 설치 여부
    try:
        import importlib
        importlib.import_module("pytrends")
        console.print("[green]pytrends: 설치됨 (Google Trends 활성화)[/green]")
    except ImportError:
        console.print("[dim]pytrends: 미설치 (Google Trends 비활성화 — 선택사항)[/dim]")
    # narrative_history 최근 기록
    if db_exists:
        try:
            nar_rows = store.recent_narratives(since=utc_now() - timedelta(hours=12))
            if nar_rows:
                from tele_quant.models import parse_dt as _parse_dt2
                nar_dt = _parse_dt2(nar_rows[0].get("created_at") or "")
                nar_age_h = (utc_now() - nar_dt).total_seconds() / 3600 if nar_dt else 999
                console.print(f"narrative_history 최근: {nar_age_h:.1f}h 전 ({len(nar_rows)}건/12h)")
            else:
                console.print("[dim]narrative_history: 최근 12h 없음 (smart_read 미실행)[/dim]")
        except Exception:
            pass

    if not recs:
        console.print("[green]이상 없음 — 자동 실행 정상[/green]")
    else:
        for rec in recs:
            console.print(rec)

    # Relation feed (self-computed)
    console.rule("[dim]relation feed 상태[/dim]")
    try:
        from tele_quant.relation_feed import load_relation_feed

        rf = load_relation_feed(settings)
        if not rf.available:
            console.print("  [dim]relation feed: 없음 (yfinance 오류)[/dim]")
        else:
            fb_count = len(rf.fallback_candidates)
            console.print(
                f"  [green]relation feed: OK — "
                f"스캔={rf.summary.price_rows if rf.summary else 0}개 "
                f"/ movers={len(rf.movers)} / 상관관계 후보={fb_count}[/green]"
            )
    except Exception as _rf_exc:
        console.print(f"  [dim]relation feed 확인 실패: {_rf_exc}[/dim]")

    # Alias book summary
    console.rule("[dim]alias book 상태[/dim]")
    try:
        from tele_quant.alias_audit import run_audit as _alias_run_audit
        from tele_quant.analysis.aliases import load_alias_config as _load_ac

        _book = _load_ac()
        _total_syms = len(_book.all_symbols)
        _audit_entries = _alias_run_audit()
        _high_cnt = sum(1 for e in _audit_entries if e.severity == "HIGH")
        _med_cnt = sum(1 for e in _audit_entries if e.severity == "MEDIUM")
        if _high_cnt > 0:
            console.print(
                f"  [red]WARN: alias HIGH 이슈 {_high_cnt}건[/red]"
                f" (총 {_total_syms}개 심볼)"
                " — alias-audit 명령으로 확인"
            )
        elif _med_cnt > 10:
            console.print(
                f"  [yellow]alias MEDIUM 이슈 {_med_cnt}건[/yellow]"
                f" (총 {_total_syms}개 심볼)"
            )
        else:
            console.print(f"  [green]alias book OK: {_total_syms}개 심볼, HIGH 이슈 없음[/green]")
    except Exception as _al_exc:
        console.print(f"  [dim]alias book 확인 실패: {_al_exc}[/dim]")

    console.print()
    console.print("[dim]⚠ WSL/Ubuntu가 꺼져 있으면 systemd user timer도 실행되지 않습니다.[/dim]")
    console.print(
        "[dim]  Persistent=true는 missed run을 보완하지만, WSL이 시작되어야 동작합니다.[/dim]"
    )
    console.print(
        "[dim]  7시 리포트를 반드시 받으려면 WSL을 켜두거나 Windows Task Scheduler로 WSL을 깨워야 합니다.[/dim]"
    )


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
    from tele_quant.headline_cleaner import (
        apply_final_report_cleaner,
        is_broker_header_only,
        is_low_quality_headline,
    )
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
            r"연합인포맥스|ShowHashtag|S&P\s*500\s*map|"
            r"모닝\s*브리핑|프리마켓\s*뉴스|ShowBotCommand)\b",
            _re.IGNORECASE,
        ),
    ]
    # Expanded forbidden patterns: additional noise patterns from Telegram export
    _EXTRA_NOISE_RES = [
        _re.compile(r"\btel:\s*\+?\d", _re.IGNORECASE),
        _re.compile(r"\bhref\s*=", _re.IGNORECASE),
        _re.compile(r"제목\s*:", _re.IGNORECASE),
        _re.compile(r"카테고리\s*:", _re.IGNORECASE),
        _re.compile(r"증권사\s*/?\s*출처\s*:", _re.IGNORECASE),
        _re.compile(r"원문\s*/?\s*목록\s*텍스트\s*:", _re.IGNORECASE),
    ]
    # Broker false-positive: broker name appearing as stock candidate (not as source)
    # These appear when broker name leaks into LONG/SHORT section header/reasons
    _BROKER_AS_CANDIDATE_RE = _re.compile(
        r"(?:JPMorgan\s*(?:Chase)?|Goldman\s*Sachs|Morgan\s*Stanley|"
        r"JP모건|제이피모건|골드만삭스|모건스탠리|씨티|뱅크오브아메리카|BofA|Wedbush|"
        r"Piper\s+Sandler|Jefferies|HSBC)\s*/\s*(?:JPM|GS|MS|C|BAC|DB)",
        _re.IGNORECASE,
    )
    # Broker name raw leak in digest/analysis (report body should not name brokers directly).
    # Exclude legitimate stock listings: "Morgan Stanley (MS)" or broker prefix "JP모건)" patterns.
    _BROKER_NAME_LEAK_RE = _re.compile(
        r"\b(?:JPMorgan(?:\s+Chase)?|Goldman\s+Sachs|Morgan\s+Stanley|"
        r"JP모건|제이피모건|골드만삭스|모건스탠리|뱅크오브아메리카|BofA|"
        r"Wedbush|Piper\s+Sandler|Jefferies)"
        r"(?!\s*\([A-Z]{1,5}\))"  # NOT followed by "(TICKER)" — that's a legitimate stock listing
        r"(?!\))",                 # NOT followed by ")" — that's a broker-prefix tag (already handled)
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

    global_issues: list[str] = []

    # DB freshness check
    all_recent = store.recent_run_reports(since=utc_now() - timedelta(hours=12), limit=1)
    if not all_recent:
        global_issues.append(
            "[red]FAIL: 최근 12시간 run_report 없음[/red] — timer 실패 또는 WSL 재시작 필요"
        )

    # pair_watch_history check
    pw_rows = store.recent_pair_watch_signals(since=utc_now() - timedelta(hours=24))
    if not pw_rows:
        global_issues.append("[yellow]WARN: 최근 24시간 pair_watch_history 저장 없음[/yellow]")

    # pair_watch cleanup state check
    try:
        _pw_stats = store.pair_watch_cleanup_stats()
        _pw_unverified_lint = _pw_stats.get("unverified_legacy", 0)
        if _pw_unverified_lint > 0:
            global_issues.append(
                f"[yellow]WARN: pair_watch unverified legacy {_pw_unverified_lint}개[/yellow]"
                " — 장 마감 후 pair-watch-cleanup --apply 실행 필요"
            )
        with store.connect() as _lc:
            _pw_failed_lint = _lc.execute(
                "SELECT COUNT(*) FROM pair_watch_history"
                " WHERE backfill_source='failed_no_price'"
                " AND (archived IS NULL OR archived=0)"
            ).fetchone()[0]
        if _pw_failed_lint > 0:
            global_issues.append(
                f"[yellow]WARN: pair_watch failed_no_price {_pw_failed_lint}개[/yellow]"
                " — yfinance 조회 실패. pair-watch-cleanup --apply 재실행 또는 네트워크 확인"
            )
    except Exception:
        pass

    # scenario_history check (most recent) with reason diagnosis
    sc_rows_recent = store.recent_scenarios(since=utc_now() - timedelta(hours=24))
    if not sc_rows_recent:
        run_rows_sent = store.recent_run_reports(since=utc_now() - timedelta(hours=24))
        sent_runs = [r for r in run_rows_sent if (getattr(r, "stats", None) or {}).get("sent")]
        if not sent_runs:
            sc_reason = "no-send 모드만 실행됨 — 실제 전송 시에만 저장"
        else:
            sc_reason = "80점 이상 후보 없음 (sent 실행은 있음)"
        global_issues.append(
            f"[yellow]WARN: 최근 24시간 scenario_history 저장 없음[/yellow] ({sc_reason})"
        )

    if global_issues:
        console.rule("[bold red]DB 상태 경보[/bold red]")
        for gi in global_issues:
            console.print(f"  {gi}")

    if not reports:
        console.print(f"[yellow]검사할 리포트 없음 (최근 {hours}h)[/yellow]")
        if global_issues:
            raise SystemExit(1)
        return

    console.print(f"[bold]lint-report: {len(reports)}개 리포트 검사[/bold] (최근 {hours}h)")

    # Check scenario_history for LONG ≥ 80 coverage
    scenario_rows = store.recent_scenarios(since=since, side="LONG", min_score=80)
    long80_saved = len(scenario_rows)
    long80_with_price = sum(1 for r in scenario_rows if r.get("close_price_at_report") is not None)

    total_issues = 0
    for row in reports:
        digest_raw = _read_report_field(row, "digest") or _read_report_field(row, "digest_text")
        analysis_raw = _read_report_field(row, "analysis") or _read_report_field(
            row, "analysis_text"
        )
        created_raw = _read_report_field(row, "created_at")
        created = created_raw[:16] if created_raw else "unknown"
        # Apply cleaner before checking: only patterns that BYPASS the cleaner are real bugs
        digest = apply_final_report_cleaner(digest_raw)
        analysis = apply_final_report_cleaner(analysis_raw)
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

        # 8. Extra noise patterns (제목:, 카테고리:, tel:, href=, etc.)
        for npat in _EXTRA_NOISE_RES:
            m_ex = npat.search(full_text)
            if m_ex:
                ctx_s = max(0, m_ex.start() - 10)
                ctx_e = min(len(full_text), m_ex.end() + 30)
                snippet = full_text[ctx_s:ctx_e].replace("\n", " ").strip()
                row_issues.append(f"[yellow]확장 노이즈:[/yellow] {escape(snippet[:80])}")
                break

        if row_issues:
            total_issues += 1
            console.rule(f"[bold]{created}[/bold]")
            for issue in row_issues[:12]:
                console.print(f"  {issue}")

    # Candidate scoring diagnosis — no candidates above min score 상황 진단
    console.rule("[dim]후보 점수 진단[/dim]")
    all_sc = store.recent_scenarios(since=since)
    sc_all_count = len(all_sc)
    sc_above_50 = sum(1 for r in all_sc if (r.get("score") or 0) >= 50)
    sc_above_80 = sum(1 for r in all_sc if (r.get("score") or 0) >= 80)
    sc_max_score = max((r.get("score") or 0) for r in all_sc) if all_sc else 0
    console.print(f"  기간 내 전체 후보: {sc_all_count}개")
    console.print(f"  점수 ≥50: {sc_above_50}개 / 점수 ≥80: {sc_above_80}개")
    console.print(f"  최고 점수: {sc_max_score:.0f}점")
    if sc_all_count > 0 and sc_above_50 == 0:
        console.print(
            "  [yellow]WARN: 50점 이상 후보 없음 — direct evidence gate 과도 가능성[/yellow]"
        )
        # 분류 이유 추정
        no_price = sum(
            1
            for r in all_sc
            if r.get("signal_price") is None and r.get("close_price_at_report") is None
        )
        low_evidence = sum(1 for r in all_sc if (r.get("direct_evidence_count") or 0) == 0)
        if low_evidence > 0:
            console.print(
                f"  → direct_evidence_count=0인 후보: {low_evidence}개"
                " (broker/header 제거로 직접 근거 없는 후보)"
            )
        if no_price > 0:
            console.print(f"  → 가격 없는 후보: {no_price}개")
    # score=44 전수 진단 (direct evidence gate 완전 차단 시 발생)
    sc_score_44 = sum(1 for r in all_sc if 43 <= (r.get("score") or 0) <= 45)
    if sc_all_count > 0 and sc_score_44 == sc_all_count:
        console.print(
            f"  [red]⚠ 전 후보 점수=44 ({sc_score_44}개) — direct evidence gate 완전 차단[/red]"
        )
        console.print(
            "  → ticker symbol(Pass 3) 또는 $TICKER(Pass 4) 검색 결과 확인 필요"
        )
    elif sc_above_50 == 0 and sc_all_count == 0:
        console.print("  [dim]해당 기간 scenario_history 저장 없음[/dim]")
    else:
        console.print("  [green]후보 점수 분포 정상[/green]")

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

    # Sentiment history diagnosis
    console.rule("[dim]감성 히스토리 진단[/dim]")
    try:
        sentiment_rows = store.recent_sentiment_history(since=since)
        if not sentiment_rows:
            console.print("  [dim]sentiment_history: 기간 내 없음 (fast 모드 미실행 가능성)[/dim]")
        else:
            sector_avg: dict[str, list[float]] = {}
            for sr in sentiment_rows:
                sec = sr.get("sector") or "Unknown"
                sc_val = float(sr.get("sentiment_score") or 50.0)
                sector_avg.setdefault(sec, []).append(sc_val)
            console.print(f"  sentiment_history: {len(sentiment_rows)}건")
            for sec, vals in sorted(sector_avg.items()):
                avg = sum(vals) / len(vals)
                icon = "⬆" if avg >= 60 else "⬇" if avg <= 40 else "➡"
                console.print(f"  {icon} {sec}: 평균 {avg:.0f}/100 ({len(vals)}건)")
    except Exception as _sh_exc:
        console.print(f"  [dim]sentiment_history 조회 실패: {_sh_exc}[/dim]")

    # Relation feed (self-computed)
    console.rule("[dim]relation feed 상태[/dim]")
    try:
        from tele_quant.relation_feed import load_relation_feed

        rf = load_relation_feed(settings)
        if not rf.available:
            console.print("  [dim]relation feed: 없음 (yfinance 오류)[/dim]")
        else:
            fb_count = len(rf.fallback_candidates)
            console.print(
                f"  [green]relation feed: OK — "
                f"스캔={rf.summary.price_rows if rf.summary else 0}개 "
                f"/ movers={len(rf.movers)} / 상관관계 후보={fb_count}[/green]"
            )
    except Exception as _rf_exc:
        console.print(f"  [dim]relation feed 확인 실패: {_rf_exc}[/dim]")

    # Alias book summary
    console.rule("[dim]alias book 상태[/dim]")
    try:
        from tele_quant.alias_audit import run_audit
        from tele_quant.analysis.aliases import load_alias_config

        book = load_alias_config()
        total_syms = len(book.all_symbols)
        audit_entries = run_audit()
        high_cnt_alias = sum(1 for e in audit_entries if e.severity == "HIGH")
        med_cnt_alias = sum(1 for e in audit_entries if e.severity == "MEDIUM")
        if high_cnt_alias > 0:
            console.print(
                f"  [red]WARN: alias HIGH 이슈 {high_cnt_alias}건[/red]"
                f" (총 {total_syms}개 심볼)"
                " — alias-audit 명령으로 확인"
            )
        elif med_cnt_alias > 10:
            console.print(
                f"  [yellow]alias MEDIUM 이슈 {med_cnt_alias}건[/yellow]"
                f" (총 {total_syms}개 심볼)"
            )
        else:
            console.print(f"  [green]alias book OK: {total_syms}개 심볼, HIGH 이슈 없음[/green]")
    except Exception as _al_exc:
        console.print(f"  [dim]alias book 확인 실패: {_al_exc}[/dim]")

    has_failures = total_issues > 0 or bool(global_issues)

    if total_issues == 0:
        console.print("[green]품질 이슈 없음 (문제 없음)[/green]")
    else:
        console.print(f"[bold red]{total_issues}/{len(reports)} 리포트에 품질 이슈[/bold red]")

    if has_failures:
        raise SystemExit(1)


@app.command("alias-audit")
def alias_audit_cmd(
    save: Annotated[
        bool, typer.Option("--save/--no-save", help="결과를 CSV로 저장할지 여부")
    ] = True,
    high_only: Annotated[
        bool, typer.Option("--high-only", help="HIGH 심각도 이슈만 표시"),
    ] = False,
    fail_on_high: Annotated[
        bool,
        typer.Option("--fail-on-high/--no-fail-on-high", help="HIGH 이슈 존재 시 exit(1)"),
    ] = False,
) -> None:
    """전체 alias 오탐 방지 품질 감사 (HIGH/MEDIUM/LOW 이슈 분류).

    Example: uv run tele-quant alias-audit
             uv run tele-quant alias-audit --high-only --fail-on-high
    """
    from pathlib import Path as _Path

    from tele_quant.alias_audit import audit_summary, run_audit, save_audit_csv

    entries = run_audit()

    if high_only:
        entries = [e for e in entries if e.severity == "HIGH"]

    summary = audit_summary(entries)
    console.print(f"\n{summary}\n")

    if entries:
        from rich.table import Table as _Table

        tbl = _Table(title=f"Alias Audit ({len(entries)}건)")
        tbl.add_column("심각도", style="bold")
        tbl.add_column("symbol")
        tbl.add_column("name")
        tbl.add_column("alias")
        tbl.add_column("이슈")

        _SEV_STYLE = {"HIGH": "red", "MEDIUM": "yellow", "LOW": "dim"}
        for e in entries[:50]:  # cap display
            tbl.add_row(
                f"[{_SEV_STYLE.get(e.severity, '')}]{e.severity}[/]",
                e.symbol,
                e.name,
                e.alias,
                e.issue,
            )
        console.print(tbl)
        if len(entries) > 50:
            console.print(f"  ... 및 {len(entries) - 50}건 더 (CSV 확인)")

    if save:
        out = _Path("data/diagnostics/alias_audit_latest.csv")
        save_audit_csv(entries, out)
        console.print(f"[dim]CSV 저장: {out}[/dim]")

    high_cnt = sum(1 for e in entries if e.severity == "HIGH")
    if fail_on_high and high_cnt > 0:
        raise SystemExit(1)


@app.command("daily-alpha")
def daily_alpha_cmd(
    market: Annotated[
        str, typer.Option("--market", help="시장 (KR 또는 US)")
    ] = "KR",
    send: Annotated[
        bool, typer.Option("--send/--no-send", help="실제 전송 여부 (--no-send: 미리보기만)")
    ] = False,
    top_n: Annotated[
        int, typer.Option("--top-n", help="LONG/SHORT 각 최대 후보 수")
    ] = 4,
    universe_size: Annotated[
        int, typer.Option("--universe-size", help="스크리닝 유니버스 크기")
    ] = 150,
) -> None:
    """Daily Alpha Picks 엔진 실행 (기계적 스크리닝 LONG/SHORT 관찰 후보).

    Example: uv run tele-quant daily-alpha --market KR --no-send
             uv run tele-quant daily-alpha --market US --send
    """
    from pathlib import Path as _Path

    from tele_quant.daily_alpha import (
        SESSION_KR,
        SESSION_US,
        build_daily_alpha_report,
        run_daily_alpha,
    )
    from tele_quant.db import Store as _Store

    market = market.upper()
    if market not in ("KR", "US"):
        console.print("[red]--market 은 KR 또는 US 만 허용됩니다.[/red]")
        raise SystemExit(1)

    session = SESSION_KR if market == "KR" else SESSION_US

    settings = _settings()
    store = _Store(_Path(settings.sqlite_path))

    console.print(f"[bold]Daily Alpha Picks[/bold] market={market} send={send} top_n={top_n}")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as prog:
        task = prog.add_task(f"[cyan]{market} 유니버스 스크리닝 중...", total=None)
        long_picks, short_picks = run_daily_alpha(
            market=market,
            store=store,
            top_n=top_n,
            universe_size=universe_size,
        )
        prog.update(task, description="[green]스크리닝 완료")

    report = build_daily_alpha_report(long_picks, short_picks, market, session_label=session)
    console.print("\n" + report)
    console.print(f"\n[dim]LONG {len(long_picks)}개 / SHORT {len(short_picks)}개 후보[/dim]")

    if send:
        # Save to DB (sent gate)
        all_picks = long_picks + short_picks
        n_saved = store.save_daily_alpha_picks(all_picks, session=session, market=market)
        console.print(f"[green]DB 저장: {n_saved}건 신규 (중복 제외)[/green]")

        # Send via Telegram
        async def _send() -> None:
            from tele_quant.telegram_sender import TelegramSender
            sender = TelegramSender(settings)
            await sender.send(report)

        asyncio.run(_send())
        console.print(f"[green]전송 완료 ({session})[/green]")
    else:
        console.print("[dim](--no-send: 미리보기만, DB 미저장, 전송 안 함)[/dim]")


@app.command("price-alert")
def price_alert_cmd(
    market: Annotated[
        str | None, typer.Option("--market", help="시장 필터 (KR | US | 생략 시 둘 다)")
    ] = None,
    send: Annotated[
        bool, typer.Option("--send/--no-send", help="실제 텔레그램 전송 여부")
    ] = False,
    force: Annotated[
        bool, typer.Option("--force", help="장중 시간대 체크 없이 강제 실행")
    ] = False,
) -> None:
    """목표가/무효화 레벨 도달 알림 (장중 30분마다 자동 실행).

    Example: uv run tele-quant price-alert --market KR --send
             uv run tele-quant price-alert --force --no-send  (수동 테스트)
    """
    from pathlib import Path as _Path

    from tele_quant.db import Store as _Store
    from tele_quant.price_alert import run_price_alerts

    settings = _settings()
    store = _Store(_Path(settings.sqlite_path))

    mkt_label = market.upper() if market else "KR+US"
    console.print(f"[bold]Price Alert[/bold] market={mkt_label} send={send} force={force}")

    triggered = run_price_alerts(
        store=store,
        market=market.upper() if market else None,
        send=send,
        force=force,
    )

    if triggered:
        for t in triggered:
            emoji = "🎯" if t["type"] == "TARGET" else "🚨"
            pick = t["pick"]
            console.print(
                f"  {emoji} {pick.get('side')} {pick.get('symbol')} "
                f"→ {t['type']} @ {t['price']:.2f}"
            )
        console.print(f"\n[green]{len(triggered)}건 알림 처리됨[/green]")
    else:
        console.print("[dim]트리거 없음 (장중 시간 아님이거나 도달 종목 없음)[/dim]")


@app.command("alpha-review")
def alpha_review_cmd(
    market: Annotated[
        str, typer.Option("--market", help="시장 (KR 또는 US)")
    ] = "KR",
    days: Annotated[
        int, typer.Option("--days", help="몇 일치 추천 성과를 볼지 (기본 1=당일)")
    ] = 1,
    send: Annotated[
        bool, typer.Option("--send/--no-send", help="텔레그램 전송 여부")
    ] = False,
) -> None:
    """장 마감 후 당일/최근 N일 추천 종목 성과 중간 요약.

    Example: uv run tele-quant alpha-review --market KR --send
             uv run tele-quant alpha-review --market US --days 3 --no-send
    """
    from pathlib import Path as _Path

    from tele_quant.alpha_review import build_alpha_review
    from tele_quant.db import Store as _Store

    market = market.upper()
    settings = _settings()
    store = _Store(_Path(settings.sqlite_path))

    console.print(f"[bold]Alpha Review[/bold] market={market} days={days} send={send}")

    report = build_alpha_review(store, market=market, days_back=days)

    if not report:
        console.print("[dim]성과 데이터 없음 (추천 기록 없거나 가격 조회 실패)[/dim]")
        return

    console.print("\n" + report)

    if send:
        async def _send() -> None:
            from tele_quant.telegram_sender import TelegramSender
            sender = TelegramSender(settings)
            await sender.send(report)

        asyncio.run(_send())
        console.print("[green]전송 완료[/green]")
    else:
        console.print("[dim](--no-send: 미리보기만)[/dim]")
