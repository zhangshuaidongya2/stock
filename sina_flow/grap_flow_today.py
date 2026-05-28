#!/usr/bin/env python3
"""Fetch today's A-share fund-flow snapshot from Sina and append CSV.

Examples:
  python sina_flow/grap_flow_today.py
  python sina_flow/grap_flow_today.py --code 000001,600519
  python sina_flow/grap_flow_today.py --date 0528
  python sina_flow/grap_flow_today.py --recreate
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as datetime_time
from pathlib import Path
from typing import Any

from backfill_today_flow import (
    DEFAULT_WORKERS as BACKFILL_DEFAULT_WORKERS,
    CHINA_TZ,
    DEFAULT_DELAY,
    DEFAULT_PAUSE_EVERY,
    DEFAULT_PAUSE_SECONDS,
    TODAY_DATA_DIR,
    build_direction,
    display_path,
    fetch_history_rows,
    normalize_code,
    resolve_project_path,
    resolve_symbols,
    round_or_none,
    scale_or_none,
)


pd = None
DEFAULT_WORKERS = max(BACKFILL_DEFAULT_WORKERS, 32)

OUTPUT_COLUMNS = [
    "代码",
    "名称",
    "最新价",
    "涨跌幅%",
    "换手率%",
    "主力流向",
    "主力净流入(万)",
    "资金流时间",
    "抓取时间",
]
REQUIRED_OUTPUT_COLUMNS = ["代码", "名称"]


def default_date_tag() -> str:
    return datetime.now(CHINA_TZ).strftime("%m%d")


def default_output_path(date_tag: str) -> Path:
    return TODAY_DATA_DIR / f"{date_tag}.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="抓取新浪个股资金流最新一条记录，并写入 sina_data/today/MMDD.csv。"
    )
    parser.add_argument(
        "--code",
        help="股票代码或名称，多个用逗号分隔；不传则读取 stock_symbols_cache.json 全部股票。",
    )
    parser.add_argument(
        "--date",
        default=default_date_tag(),
        help="输出文件日期标识，格式 MMDD，例如 0528；默认今天。",
    )
    parser.add_argument(
        "--output",
        help="输出 CSV 文件；不传则写入 sina_data/today/ 下 --date 对应的 MMDD.csv。",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="抓取前删除已有输出文件并重新创建。",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help=f"单线程模式下每只股票之间等待秒数，默认 {DEFAULT_DELAY}。",
    )
    parser.add_argument(
        "--pause-every",
        type=int,
        default=DEFAULT_PAUSE_EVERY,
        help=(
            "单线程模式下每抓取多少只股票后长暂停一次；"
            f"小于等于 0 表示不长暂停，默认 {DEFAULT_PAUSE_EVERY}。"
        ),
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=DEFAULT_PAUSE_SECONDS,
        help=f"单线程模式下长暂停秒数，默认 {DEFAULT_PAUSE_SECONDS}。",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"并发抓取线程数，默认 {DEFAULT_WORKERS}。",
    )
    return parser.parse_args()


def normalize_date_tag(value: str) -> str:
    date_tag = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(date_tag) != 4:
        raise SystemExit("--date 必须是 MMDD 格式，例如 0528")
    return date_tag


def require_dependencies() -> None:
    global pd
    try:
        import pandas as pandas  # type: ignore
    except ImportError as exc:
        raise SystemExit(
            "缺少依赖，请先执行：python3 -m pip install -r requirements.txt"
        ) from exc
    pd = pandas


def normalize_existing_output_df(df: Any, output_path: Path) -> Any:
    unknown_columns = [column for column in df.columns if column not in OUTPUT_COLUMNS]
    if unknown_columns:
        raise SystemExit(
            f"已有导出文件 {output_path} 包含未知字段 {', '.join(unknown_columns)}，"
            "为避免写错列，已停止。请先备份或处理旧文件。"
        )

    missing_required = [column for column in REQUIRED_OUTPUT_COLUMNS if column not in df.columns]
    if missing_required:
        raise SystemExit(
            f"已有导出文件 {output_path} 缺少关键字段 {', '.join(missing_required)}，"
            "无法继续更新。"
        )

    result = df.copy()
    for column in OUTPUT_COLUMNS:
        if column not in result.columns:
            result[column] = pd.NA
    return result[OUTPUT_COLUMNS]


def load_existing_output_df(output_path: Path) -> Any | None:
    if not output_path.exists() or output_path.stat().st_size == 0:
        return None
    try:
        existing_df = pd.read_csv(output_path, dtype={"代码": str})
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"无法读取已有导出文件 {output_path}：{exc}") from exc
    return normalize_existing_output_df(existing_df, output_path)


def read_exported_codes(output_path: Path) -> set[str]:
    existing_df = load_existing_output_df(output_path)
    if existing_df is None:
        return set()
    return {
        normalize_code(code)
        for code in existing_df["代码"].dropna()
        if normalize_code(code)
    }


def upsert_rows_csv(df: Any, output_path: Path) -> tuple[int, int]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    existing_df = load_existing_output_df(output_path)
    if existing_df is None:
        df.to_csv(output_path, index=False)
        return 0, len(df)

    incoming_rows: dict[str, Any] = {}
    for _, row in df.iterrows():
        code = normalize_code(row.get("代码"))
        if code:
            incoming_rows[code] = row[OUTPUT_COLUMNS]

    output_rows = []
    replaced_count = 0
    written_codes = set()
    for _, row in existing_df.iterrows():
        code = normalize_code(row.get("代码"))
        if code in incoming_rows:
            replaced_count += 1
            if code not in written_codes:
                output_rows.append(incoming_rows[code])
                written_codes.add(code)
            continue
        output_rows.append(row[OUTPUT_COLUMNS])

    for code, row in incoming_rows.items():
        if code not in written_codes:
            output_rows.append(row)
            written_codes.add(code)

    output_df = pd.DataFrame(output_rows, columns=OUTPUT_COLUMNS)
    output_df.to_csv(output_path, index=False)
    return replaced_count, len(incoming_rows)


def parse_row_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise RuntimeError(f"新浪返回了无法识别的日期 {value!r}") from exc


def build_target_date(date_tag: str) -> date:
    year = datetime.now(CHINA_TZ).year
    try:
        return datetime.strptime(f"{year}{date_tag}", "%Y%m%d").date()
    except ValueError as exc:
        raise SystemExit(f"--date 不是有效的 MMDD 日期：{date_tag}") from exc


def build_flow_time(target_day: date, fetch_time: datetime) -> str:
    if target_day == fetch_time.date():
        return fetch_time.isoformat(timespec="seconds")
    return datetime.combine(
        target_day,
        datetime_time(hour=15),
        tzinfo=CHINA_TZ,
    ).isoformat(timespec="seconds")


def build_today_record(
    symbol: dict[str, str],
    latest_date: date,
    history_row: dict[str, Any],
    fetch_time: datetime,
) -> dict[str, Any]:
    code = normalize_code(symbol["代码"])
    main_net_wan = scale_or_none(history_row.get("主力净流入-净额"), 10000)
    return {
        "代码": code,
        "名称": symbol.get("名称", ""),
        "最新价": round_or_none(history_row.get("收盘价")),
        "涨跌幅%": round_or_none(history_row.get("涨跌幅")),
        "换手率%": round_or_none(history_row.get("换手率")),
        "主力流向": build_direction(main_net_wan),
        "主力净流入(万)": main_net_wan,
        "资金流时间": build_flow_time(latest_date, fetch_time),
        "抓取时间": fetch_time.isoformat(timespec="seconds"),
    }


def fetch_symbol_snapshot(symbol: dict[str, str]) -> tuple[dict[str, str], date, dict[str, Any], datetime]:
    history_rows = fetch_history_rows(symbol, page_size=1)
    latest_date_key = max(history_rows)
    latest_date = parse_row_date(latest_date_key)
    fetch_time = datetime.now(CHINA_TZ).replace(microsecond=0)
    return symbol, latest_date, history_rows[latest_date_key], fetch_time


def maybe_sleep_between_requests(args: argparse.Namespace, index: int, total: int) -> None:
    if index >= total:
        return
    if args.pause_every > 0 and index % args.pause_every == 0:
        time.sleep(args.pause_seconds)
        return
    if args.delay > 0:
        time.sleep(args.delay)


def main() -> None:
    args = parse_args()
    if args.delay < 0:
        raise SystemExit("--delay 不能小于 0")
    if args.pause_seconds < 0:
        raise SystemExit("--pause-seconds 不能小于 0")
    if args.workers <= 0:
        raise SystemExit("--workers 必须大于 0")

    require_dependencies()

    date_tag = normalize_date_tag(args.date)
    output_path = resolve_project_path(args.output) if args.output else default_output_path(date_tag)
    if args.recreate and output_path.exists():
        output_path.unlink()
        print(f"已删除旧文件：{display_path(output_path)}", file=sys.stderr)

    symbols = resolve_symbols(args.code)
    update_selected_codes = bool(args.code)
    exported_codes = read_exported_codes(output_path)
    if update_selected_codes:
        pending_symbols = symbols
    else:
        pending_symbols = [
            symbol for symbol in symbols
            if normalize_code(symbol["代码"]) not in exported_codes
        ]

    total = len(symbols)
    skipped = 0 if update_selected_codes else total - len(pending_symbols)
    if update_selected_codes:
        existing_selected = sum(
            1
            for symbol in symbols
            if normalize_code(symbol["代码"]) in exported_codes
        )
        print(
            f"开始更新新浪当天资金流：文件日期 {date_tag}，"
            f"股票 {total} 只，已有 {existing_selected} 只会覆盖，"
            f"待抓取 {len(pending_symbols)} 只，输出文件 {display_path(output_path)}，"
            f"并发 {args.workers} 线程",
            file=sys.stderr,
        )
    else:
        print(
            f"开始导出新浪当天资金流：文件日期 {date_tag}，"
            f"股票 {total} 只，已存在 {skipped} 只，待抓取 {len(pending_symbols)} 只，"
            f"输出文件 {display_path(output_path)}，并发 {args.workers} 线程",
            file=sys.stderr,
        )
    if args.workers > 1 and (args.delay > 0 or args.pause_every > 0):
        print(
            "提示：启用多线程后，--delay / --pause-every / --pause-seconds 不再生效。",
            file=sys.stderr,
        )

    target_date = build_target_date(date_tag)
    fetched_records: list[dict[str, Any]] = []
    stale_count = 0
    stale_examples: list[str] = []

    def handle_result(
        result: tuple[dict[str, str], date, dict[str, Any], datetime],
        index: int,
        total_work: int,
    ) -> None:
        nonlocal stale_count
        symbol, latest_date, history_row, fetch_time = result
        code = normalize_code(symbol["代码"])
        name = symbol.get("名称", "")
        if latest_date != target_date:
            stale_count += 1
            if len(stale_examples) < 5:
                stale_examples.append(
                    f"{code} {name or ''} 最新日期 {latest_date.isoformat()}"
                )
            print(
                f"[{index}/{total_work}] 跳过 {code} {name}："
                f"最新可用日期是 {latest_date.isoformat()}，与目标文件日期不一致",
                file=sys.stderr,
            )
            return
        fetched_records.append(build_today_record(symbol, latest_date, history_row, fetch_time))
        print(
            f"[{index}/{total_work}] 完成 {code} {name}",
            file=sys.stderr,
        )

    total_work = len(pending_symbols)
    if total_work == 0:
        print("无需抓取；目标股票已全部存在。", file=sys.stderr)
    elif args.workers == 1:
        for index, symbol in enumerate(pending_symbols, start=1):
            code = normalize_code(symbol["代码"])
            name = symbol.get("名称", "")
            print(f"[{index}/{total_work}] 抓取 {code} {name}", file=sys.stderr)
            try:
                result = fetch_symbol_snapshot(symbol)
            except Exception as exc:  # noqa: BLE001
                if fetched_records:
                    write_df = pd.DataFrame(fetched_records, columns=OUTPUT_COLUMNS)
                    replaced_count, written_count = upsert_rows_csv(write_df, output_path)
                    print(
                        f"失败前已写入 {written_count} 只（覆盖 {replaced_count} 只）到 {display_path(output_path)}。",
                        file=sys.stderr,
                    )
                retry_hint = (
                    "修复或稍后重试后，会重新抓取并更新本次指定股票。"
                    if update_selected_codes
                    else "已写入的数据会保留，稍后重试会跳过当天已写入股票。"
                )
                raise SystemExit(
                    f"[{index}/{total_work}] 获取失败 {code} {name}："
                    f"{type(exc).__name__}: {exc}\n"
                    f"已停止导出；{retry_hint}"
                ) from exc
            handle_result(result, index, total_work)
            maybe_sleep_between_requests(args, index, total_work)
    else:
        future_map = {}
        with ThreadPoolExecutor(max_workers=min(args.workers, total_work)) as executor:
            for symbol in pending_symbols:
                future = executor.submit(fetch_symbol_snapshot, symbol)
                future_map[future] = symbol
            completed = 0
            for future in as_completed(future_map):
                symbol = future_map[future]
                code = normalize_code(symbol["代码"])
                name = symbol.get("名称", "")
                completed += 1
                try:
                    result = future.result()
                except Exception as exc:  # noqa: BLE001
                    for pending_future in future_map:
                        if not pending_future.done():
                            pending_future.cancel()
                    if fetched_records:
                        write_df = pd.DataFrame(fetched_records, columns=OUTPUT_COLUMNS)
                        replaced_count, written_count = upsert_rows_csv(write_df, output_path)
                        print(
                            f"失败前已写入 {written_count} 只（覆盖 {replaced_count} 只）到 {display_path(output_path)}。",
                            file=sys.stderr,
                        )
                    retry_hint = (
                        "修复或稍后重试后，会重新抓取并更新本次指定股票。"
                        if update_selected_codes
                        else "已写入的数据会保留，稍后重试会跳过当天已写入股票。"
                    )
                    raise SystemExit(
                        f"[{completed}/{total_work}] 获取失败 {code} {name}："
                        f"{type(exc).__name__}: {exc}\n"
                        f"已停止导出；{retry_hint}"
                    ) from exc
                handle_result(result, completed, total_work)

    replaced_count = 0
    written_count = 0
    if fetched_records:
        write_df = pd.DataFrame(fetched_records, columns=OUTPUT_COLUMNS)
        replaced_count, written_count = upsert_rows_csv(write_df, output_path)

    if stale_examples:
        print(
            "以下股票最新可用日期与目标文件日期不一致，已跳过：\n"
            + "\n".join(stale_examples),
            file=sys.stderr,
        )

    print(
        f"完成：写入 {written_count} 只，覆盖 {replaced_count} 只，"
        f"跳过已存在 {skipped} 只，日期不匹配跳过 {stale_count} 只；"
        f"输出文件 {display_path(output_path)}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
