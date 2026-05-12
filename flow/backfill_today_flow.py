#!/usr/bin/env python3
"""Backfill missed data/today/MMDD.csv files from historical fund-flow data.

Examples:
  python flow/backfill_today_flow.py --dates 2026-04-29,2026-04-30
  python flow/backfill_today_flow.py --dates 0429,0430
  python flow/backfill_today_flow.py --dates 2026-04-29 --code 000001,600519
  python flow/backfill_today_flow.py --dates 2026-04-29 --recreate
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import date, datetime, time as datetime_time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
from symbol_search import (
    build_suggestion_message,
    search_symbol_records,
    symbol_records_from_name_map,
)


pd = None

REQUEST_TIMEOUT = 10
FETCH_RETRY_TIMES = 3
FETCH_RETRY_BASE_SLEEP = 0.8
EASTMONEY_FUND_FLOW_DAY_URL = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
PROJECT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_DIR / "data"
TODAY_DATA_DIR = DATA_DIR / "today"
SYMBOL_CACHE_PATH = PROJECT_DIR / "stock_symbols_cache.json"
DEFAULT_DELAY = 0.2
DEFAULT_PAUSE_EVERY = 0
DEFAULT_PAUSE_SECONDS = 120
CHINA_TZ = ZoneInfo("Asia/Shanghai")

OUTPUT_COLUMNS = [
    "代码",
    "名称",
    "最新价",
    "涨跌幅%",
    "主力流向",
    "主力净流入(万)",
    "主力净占比%",
    "超大单净流入(万)",
    "超大单净占比%",
    "大单净流入(万)",
    "大单净占比%",
    "中单净流入(万)",
    "中单净占比%",
    "小单净流入(万)",
    "小单净占比%",
    "资金流时间",
    "抓取时间",
]

HISTORY_COLUMNS = [
    "日期",
    "主力净流入-净额",
    "小单净流入-净额",
    "中单净流入-净额",
    "大单净流入-净额",
    "超大单净流入-净额",
    "主力净流入-净占比",
    "小单净流入-净占比",
    "中单净流入-净占比",
    "大单净流入-净占比",
    "超大单净流入-净占比",
    "收盘价",
    "涨跌幅",
    "_",
    "__",
]


def default_year() -> int:
    return datetime.now(CHINA_TZ).year


def default_output_path(date_tag: str, output_dir: Path) -> Path:
    return output_dir / f"{date_tag}.csv"


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_DIR))
    except ValueError:
        return str(path)


def resolve_project_path(path: str | Path) -> Path:
    resolved_path = Path(path)
    if resolved_path.is_absolute():
        return resolved_path
    return PROJECT_DIR / resolved_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "用东方财富历史日线资金流补录 data/today/MMDD.csv。"
            "适合补忘记运行 grap_flow_today.py 的交易日。"
        )
    )
    parser.add_argument(
        "--dates",
        required=True,
        help=(
            "要补录的日期，多个用逗号分隔；支持 YYYY-MM-DD、YYYYMMDD、MMDD。"
            "只写 MMDD 时使用 --year，默认当前年份。"
        ),
    )
    parser.add_argument(
        "--year",
        type=int,
        default=default_year(),
        help="解析 MMDD 日期时使用的年份，默认当前年份。",
    )
    parser.add_argument(
        "--code",
        help="股票代码或名称，多个用逗号分隔；不传则读取 stock_symbols_cache.json 全部股票。",
    )
    parser.add_argument(
        "--output-dir",
        default=str(TODAY_DATA_DIR),
        help=f"输出目录，默认 {display_path(TODAY_DATA_DIR)}。",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="补录前删除目标日期已有 CSV 并重新创建。",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help=f"每只股票之间等待秒数，默认 {DEFAULT_DELAY}。",
    )
    parser.add_argument(
        "--pause-every",
        type=int,
        default=DEFAULT_PAUSE_EVERY,
        help=(
            "每抓取多少只股票后长暂停一次；小于等于 0 表示不长暂停，"
            f"默认 {DEFAULT_PAUSE_EVERY}。"
        ),
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=DEFAULT_PAUSE_SECONDS,
        help=f"长暂停秒数，默认 {DEFAULT_PAUSE_SECONDS}。",
    )
    return parser.parse_args()


def require_dependencies() -> None:
    global pd
    try:
        import pandas as pandas  # type: ignore
    except ImportError as exc:
        raise SystemExit(
            "缺少依赖，请先执行：python3 -m pip install -r requirements.txt"
        ) from exc
    pd = pandas


def normalize_code(value: Any) -> str:
    code = str(value).strip().lower()
    for prefix in ("sh", "sz", "bj"):
        if code.startswith(prefix):
            code = code[len(prefix) :]
    digits = "".join(ch for ch in code if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def detect_market(code: str) -> str:
    normalized = normalize_code(code)
    if normalized.startswith("6"):
        return "sh"
    if normalized.startswith(("4", "8", "9")):
        return "bj"
    return "sz"


def to_eastmoney_secid(code: str) -> str:
    normalized = normalize_code(code)
    market = "1" if detect_market(normalized) == "sh" else "0"
    return f"{market}.{normalized}"


def safe_float(value: Any) -> float | None:
    if value is None or value == "-":
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    if pd is not None and pd.isna(converted):
        return None
    return converted


def round_or_none(value: Any, digits: int = 2) -> float | None:
    converted = safe_float(value)
    if converted is None:
        return None
    return round(converted, digits)


def scale_or_none(value: Any, scale: float, digits: int = 2) -> float | None:
    converted = safe_float(value)
    if converted is None:
        return None
    return round(converted / scale, digits)


def build_direction(value: Any) -> str:
    converted = safe_float(value)
    if converted is None:
        return ""
    if converted > 0:
        return "流入"
    if converted < 0:
        return "流出"
    return "持平"


def normalize_dates(value: str, year: int) -> list[date]:
    dates = []
    seen = set()
    tokens = [item.strip() for item in re.split(r"[,，\s]+", value) if item.strip()]
    if not tokens:
        raise SystemExit("--dates 不能为空")

    for token in tokens:
        digits = "".join(ch for ch in token if ch.isdigit())
        try:
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", token):
                parsed = datetime.strptime(token, "%Y-%m-%d").date()
            elif len(digits) == 8:
                parsed = datetime.strptime(digits, "%Y%m%d").date()
            elif len(digits) == 4:
                parsed = datetime.strptime(f"{year}{digits}", "%Y%m%d").date()
            else:
                raise ValueError
        except ValueError as exc:
            raise SystemExit(
                f"无法解析日期 {token!r}，请使用 YYYY-MM-DD、YYYYMMDD 或 MMDD。"
            ) from exc

        if parsed > datetime.now(CHINA_TZ).date():
            raise SystemExit(f"不能补录未来日期：{parsed.isoformat()}")
        if parsed not in seen:
            dates.append(parsed)
            seen.add(parsed)

    return sorted(dates)


def date_tag(value: date) -> str:
    return value.strftime("%m%d")


def history_date_key(value: date) -> str:
    return value.strftime("%Y-%m-%d")


def read_symbol_cache() -> dict[str, str]:
    if not SYMBOL_CACHE_PATH.exists():
        return {}
    try:
        payload = json.loads(SYMBOL_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        str(name).strip(): normalize_code(code)
        for name, code in payload.items()
        if str(name).strip() and normalize_code(code)
    }


def resolve_all_symbols() -> list[dict[str, str]]:
    name_map = read_symbol_cache()
    if not name_map:
        raise SystemExit("stock_symbols_cache.json 为空或无法解析")
    symbols = [
        {"代码": code, "名称": name}
        for name, code in name_map.items()
    ]
    return sorted(symbols, key=lambda item: (detect_market(item["代码"]) == "bj", item["代码"]))


def resolve_symbols(code_arg: str | None) -> list[dict[str, str]]:
    if not code_arg:
        return resolve_all_symbols()

    tokens = [item.strip() for item in code_arg.split(",") if item.strip()]
    if not tokens:
        raise SystemExit("--code 不能为空")

    name_map = read_symbol_cache()
    code_name_map = {code: name for name, code in name_map.items()}
    symbol_records = symbol_records_from_name_map(name_map)
    resolved = []
    unresolved = []
    for token in tokens:
        code = normalize_code(token)
        if code and any(ch.isdigit() for ch in token):
            if code in code_name_map:
                resolved.append({"代码": code, "名称": code_name_map.get(code, "")})
            else:
                unresolved.append(token)
            continue

        cached_code = name_map.get(token)
        if cached_code:
            resolved.append({"代码": cached_code, "名称": token})
        else:
            unresolved.append(token)

    if unresolved:
        messages = [
            build_suggestion_message(
                token,
                search_symbol_records(symbol_records, token, top=6),
                not_found_prefix="无法解析股票：",
                include_reason=True,
            )
            for token in unresolved
        ]
        raise SystemExit("\n\n".join(messages))
    return resolved


def read_exported_codes(output_path: Path) -> set[str]:
    if not output_path.exists() or output_path.stat().st_size == 0:
        return set()
    try:
        exported_df = pd.read_csv(output_path, dtype={"代码": str})
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"无法读取已有导出文件 {output_path}：{exc}") from exc

    current_columns = list(exported_df.columns)
    if current_columns != OUTPUT_COLUMNS:
        raise SystemExit(
            f"已有导出文件 {output_path} 的表头不是 today CSV 格式，"
            "为避免追加错列，已停止。请先备份或处理旧文件。"
        )
    return {
        normalize_code(code)
        for code in exported_df["代码"].dropna()
        if normalize_code(code)
    }


def append_rows_csv(df: Any, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not output_path.exists() or output_path.stat().st_size == 0
    if output_path.exists() and output_path.stat().st_size > 0:
        with output_path.open("rb+") as file_obj:
            file_obj.seek(-1, 2)
            if file_obj.read(1) not in {b"\n", b"\r"}:
                file_obj.write(b"\n")
    df.to_csv(output_path, mode="a", index=False, header=write_header)


def upsert_rows_csv(df: Any, output_path: Path) -> tuple[int, int]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not output_path.exists() or output_path.stat().st_size == 0:
        df.to_csv(output_path, index=False)
        return 0, len(df)

    try:
        existing_df = pd.read_csv(output_path, dtype={"代码": str})
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"无法读取已有导出文件 {output_path}：{exc}") from exc

    current_columns = list(existing_df.columns)
    if current_columns != OUTPUT_COLUMNS:
        raise SystemExit(
            f"已有导出文件 {output_path} 的表头不是 today CSV 格式，"
            "为避免更新错列，已停止。请先备份或处理旧文件。"
        )

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


def fetch_history_rows(symbol: dict[str, str]) -> dict[str, dict[str, Any]]:
    import requests

    errors = []
    for retry in range(FETCH_RETRY_TIMES + 1):
        try:
            response = requests.get(
                EASTMONEY_FUND_FLOW_DAY_URL,
                params={
                    "lmt": "0",
                    "klt": "101",
                    "secid": to_eastmoney_secid(symbol["代码"]),
                    "fields1": "f1,f2,f3,f7",
                    "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
                    "ut": "b2884a393a59ad64002292a3e90d46a5",
                    "_": int(time.time() * 1000),
                },
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
                    ),
                    "Referer": "https://quote.eastmoney.com/",
                },
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
            klines = payload.get("data", {}).get("klines")
            if not isinstance(klines, list) or not klines:
                raise RuntimeError("东方财富资金流日线返回空数据")

            rows = {}
            for line in klines:
                parts = str(line).split(",")
                if len(parts) < len(HISTORY_COLUMNS):
                    parts.extend([""] * (len(HISTORY_COLUMNS) - len(parts)))
                row = dict(zip(HISTORY_COLUMNS, parts[:len(HISTORY_COLUMNS)]))
                row_date = str(row.get("日期") or "")
                if row_date:
                    rows[row_date] = row
            return rows
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{type(exc).__name__}: {exc}")
            if retry < FETCH_RETRY_TIMES:
                time.sleep(FETCH_RETRY_BASE_SLEEP * (retry + 1))

    raise RuntimeError("；".join(errors))


def build_today_record(
    symbol: dict[str, str],
    target_date: date,
    history_row: dict[str, Any],
    fetch_time: str,
) -> dict[str, Any]:
    code = normalize_code(symbol["代码"])
    main_net_wan = scale_or_none(history_row.get("主力净流入-净额"), 10000)
    flow_time = datetime.combine(
        target_date,
        datetime_time(hour=15),
        tzinfo=CHINA_TZ,
    ).isoformat(timespec="seconds")

    return {
        "代码": code,
        "名称": symbol.get("名称", ""),
        "最新价": round_or_none(history_row.get("收盘价")),
        "涨跌幅%": round_or_none(history_row.get("涨跌幅")),
        "主力流向": build_direction(main_net_wan),
        "主力净流入(万)": main_net_wan,
        "主力净占比%": round_or_none(history_row.get("主力净流入-净占比")),
        "超大单净流入(万)": scale_or_none(history_row.get("超大单净流入-净额"), 10000),
        "超大单净占比%": round_or_none(history_row.get("超大单净流入-净占比")),
        "大单净流入(万)": scale_or_none(history_row.get("大单净流入-净额"), 10000),
        "大单净占比%": round_or_none(history_row.get("大单净流入-净占比")),
        "中单净流入(万)": scale_or_none(history_row.get("中单净流入-净额"), 10000),
        "中单净占比%": round_or_none(history_row.get("中单净流入-净占比")),
        "小单净流入(万)": scale_or_none(history_row.get("小单净流入-净额"), 10000),
        "小单净占比%": round_or_none(history_row.get("小单净流入-净占比")),
        "资金流时间": flow_time,
        "抓取时间": fetch_time,
    }


def main() -> None:
    args = parse_args()
    if args.delay < 0:
        raise SystemExit("--delay 不能小于 0")
    if args.pause_seconds < 0:
        raise SystemExit("--pause-seconds 不能小于 0")

    require_dependencies()

    target_dates = normalize_dates(args.dates, args.year)
    output_dir = resolve_project_path(args.output_dir)
    output_paths = {
        target_date: default_output_path(date_tag(target_date), output_dir)
        for target_date in target_dates
    }

    if args.recreate:
        for output_path in output_paths.values():
            if output_path.exists():
                output_path.unlink()
                print(f"已删除旧文件：{display_path(output_path)}", file=sys.stderr)

    symbols = resolve_symbols(args.code)
    update_selected_codes = bool(args.code)
    exported_codes_by_date = {
        target_date: read_exported_codes(output_path)
        for target_date, output_path in output_paths.items()
    }
    processed_codes = set().union(*exported_codes_by_date.values())
    pending_symbols = []
    for symbol in symbols:
        code = normalize_code(symbol["代码"])
        if update_selected_codes or code not in processed_codes:
            pending_symbols.append(symbol)

    total = len(symbols)
    skipped = total - len(pending_symbols)
    date_text = ",".join(target_date.isoformat() for target_date in target_dates)
    file_text = ",".join(display_path(path) for path in output_paths.values())
    if update_selected_codes:
        print(
            f"开始补录指定股票历史资金流：日期 {date_text}，"
            f"股票 {total} 只，输出文件 {file_text}",
            file=sys.stderr,
        )
    else:
        print(
            f"开始补录历史资金流：日期 {date_text}，"
            f"股票 {total} 只，任一目标日期已存在 {skipped} 只，"
            f"待抓取 {len(pending_symbols)} 只，输出文件 {file_text}",
            file=sys.stderr,
        )

    success_by_date = {target_date: 0 for target_date in target_dates}
    replaced_by_date = {target_date: 0 for target_date in target_dates}
    missing_by_date = {target_date: 0 for target_date in target_dates}

    target_keys = {target_date: history_date_key(target_date) for target_date in target_dates}
    fetched_count = 0
    for index, symbol in enumerate(pending_symbols, start=1):
        code = normalize_code(symbol["代码"])
        name = symbol.get("名称", "")
        needed_dates = [
            target_date
            for target_date in target_dates
            if update_selected_codes or code not in exported_codes_by_date[target_date]
        ]
        if not needed_dates:
            continue

        print(
            f"[{index}/{len(pending_symbols)}] 补录 {code} {name} ...",
            file=sys.stderr,
        )
        try:
            history_rows = fetch_history_rows(symbol)
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(
                f"[{index}/{len(pending_symbols)}] 获取失败 {code} {name}："
                f"{type(exc).__name__}: {exc}\n"
                "已停止补录；已写入的数据会保留，稍后重试会跳过已写入股票。"
            ) from exc
        fetched_count += 1

        fetch_time = datetime.now(CHINA_TZ).isoformat(timespec="seconds")
        records_by_date: dict[date, list[dict[str, Any]]] = {
            target_date: []
            for target_date in needed_dates
        }
        for target_date in needed_dates:
            history_row = history_rows.get(target_keys[target_date])
            if history_row is None:
                missing_by_date[target_date] += 1
                continue
            records_by_date[target_date].append(
                build_today_record(symbol, target_date, history_row, fetch_time)
            )

        written_parts = []
        for target_date, records in records_by_date.items():
            if not records:
                continue
            output_df = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
            output_path = output_paths[target_date]
            if update_selected_codes:
                replaced_count, written_count = upsert_rows_csv(output_df, output_path)
                replaced_by_date[target_date] += replaced_count
            else:
                append_rows_csv(output_df, output_path)
                written_count = len(records)
            success_by_date[target_date] += written_count
            exported_codes_by_date[target_date].add(code)
            written_parts.append(f"{date_tag(target_date)} 写入 {written_count} 条")

        if written_parts:
            print(f"[{index}/{len(pending_symbols)}] " + "，".join(written_parts), file=sys.stderr)
        else:
            missing_text = ",".join(date_tag(target_date) for target_date in needed_dates)
            print(
                f"[{index}/{len(pending_symbols)}] 目标日期无历史记录：{missing_text}",
                file=sys.stderr,
            )

        if index < len(pending_symbols):
            should_long_pause = (
                args.pause_every > 0
                and args.pause_seconds > 0
                and fetched_count % args.pause_every == 0
            )
            if should_long_pause:
                print(
                    f"已抓取 {fetched_count} 只股票，暂停 {args.pause_seconds:g} 秒后继续...",
                    file=sys.stderr,
                )
                time.sleep(args.pause_seconds)
            elif args.delay > 0:
                time.sleep(args.delay)

    summary_parts = []
    for target_date in target_dates:
        part = f"{date_tag(target_date)} 新增/更新 {success_by_date[target_date]} 条"
        if update_selected_codes and replaced_by_date[target_date]:
            part += f"，覆盖 {replaced_by_date[target_date]} 条"
        if missing_by_date[target_date]:
            part += f"，无历史记录 {missing_by_date[target_date]} 只"
        summary_parts.append(part)
    print(
        f"补录完成：{'; '.join(summary_parts)}。"
        "如需更新矩阵，请执行 python flow/build_today_flow_matrix.py",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
