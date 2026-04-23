#!/usr/bin/env python3
"""Fetch today's A-share realtime main fund flow and append CSV.

Examples:
  python stock_fund_flow_today.py
  python stock_fund_flow_today.py --code 000001,600519
  python stock_fund_flow_today.py --date 0423
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any


pd = None

REQUEST_TIMEOUT = 10
FETCH_RETRY_TIMES = 3
FETCH_RETRY_BASE_SLEEP = 0.8
EASTMONEY_FUND_FLOW_URL = "https://push2.eastmoney.com/api/qt/ulist.np/get"
PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
TODAY_DATA_DIR = DATA_DIR / "today"
SYMBOL_CACHE_PATH = PROJECT_DIR / "stock_symbols_cache.json"
DEFAULT_BATCH_SIZE = 80
DEFAULT_DELAY = 0.2

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


def default_date_tag() -> str:
    return datetime.now().astimezone().strftime("%m%d")


def default_output_path(date_tag: str) -> Path:
    return TODAY_DATA_DIR / f"{date_tag}.csv"


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_DIR))
    except ValueError:
        return str(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="抓取 A 股当天实时主力资金流入流出数据，并追加写入独立 CSV。"
    )
    parser.add_argument(
        "--code",
        help="股票代码或名称，多个用逗号分隔；不传则读取 stock_symbols_cache.json 全部股票。",
    )
    parser.add_argument(
        "--date",
        default=default_date_tag(),
        help="文件日期标识，格式 MMDD，例如 0423；默认今天。",
    )
    parser.add_argument(
        "--output",
        help="输出 CSV 文件；不传则写入 data/today/ 下 --date 对应的 MMDD.csv。",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"每批请求多少只股票，默认 {DEFAULT_BATCH_SIZE}。",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help=f"批次之间等待秒数，默认 {DEFAULT_DELAY}。",
    )
    return parser.parse_args()


def normalize_date_tag(value: str) -> str:
    date_tag = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(date_tag) != 4:
        raise SystemExit("--date 必须是 MMDD 格式，例如 0423")
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
    resolved = []
    unresolved = []
    for token in tokens:
        code = normalize_code(token)
        if code and any(ch.isdigit() for ch in token):
            resolved.append({"代码": code, "名称": code_name_map.get(code, "")})
            continue

        cached_code = name_map.get(token)
        if cached_code:
            resolved.append({"代码": cached_code, "名称": token})
        else:
            unresolved.append(token)

    if unresolved:
        raise SystemExit(
            "无法解析股票名称："
            + "、".join(unresolved)
            + "。请改用股票代码，或更新 stock_symbols_cache.json。"
        )
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
            f"已有导出文件 {output_path} 的表头不是当前格式，"
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


def chunks(items: list[dict[str, str]], size: int) -> list[list[dict[str, str]]]:
    return [items[index:index + size] for index in range(0, len(items), size)]


def fetch_today_fund_flow_batch(
    symbols: list[dict[str, str]],
) -> list[dict[str, Any]]:
    import requests

    secids = ",".join(to_eastmoney_secid(symbol["代码"]) for symbol in symbols)
    code_name_map = {normalize_code(symbol["代码"]): symbol.get("名称", "") for symbol in symbols}
    errors = []

    for retry in range(FETCH_RETRY_TIMES + 1):
        try:
            response = requests.get(
                EASTMONEY_FUND_FLOW_URL,
                params={
                    "secids": secids,
                    "fields": (
                        "f2,f3,f12,f14,f62,f184,f66,f69,"
                        "f72,f75,f78,f81,f84,f87,f124"
                    ),
                    "ut": "b2884a393a59ad64002292a3e90d46a5",
                    "fltt": "2",
                    "invt": "2",
                    "np": "1",
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
            diff = payload.get("data", {}).get("diff", [])
            if not isinstance(diff, list) or not diff:
                raise RuntimeError("东方财富实时资金流返回空数据")
            break
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{type(exc).__name__}: {exc}")
            if retry < FETCH_RETRY_TIMES:
                time.sleep(FETCH_RETRY_BASE_SLEEP * (retry + 1))
    else:
        raise RuntimeError("；".join(errors))

    fetch_time = datetime.now().astimezone().isoformat(timespec="seconds")
    records = []
    for item in diff:
        code = normalize_code(item.get("f12"))
        main_net_wan = scale_or_none(item.get("f62"), 10000)
        flow_timestamp = safe_float(item.get("f124"))
        flow_time = ""
        if flow_timestamp is not None and flow_timestamp > 0:
            flow_time = datetime.fromtimestamp(
                int(flow_timestamp),
            ).astimezone().isoformat(timespec="seconds")

        records.append(
            {
                "代码": code,
                "名称": str(item.get("f14") or code_name_map.get(code, "")),
                "最新价": round_or_none(item.get("f2")),
                "涨跌幅%": round_or_none(item.get("f3")),
                "主力流向": build_direction(main_net_wan),
                "主力净流入(万)": main_net_wan,
                "主力净占比%": round_or_none(item.get("f184")),
                "超大单净流入(万)": scale_or_none(item.get("f66"), 10000),
                "超大单净占比%": round_or_none(item.get("f69")),
                "大单净流入(万)": scale_or_none(item.get("f72"), 10000),
                "大单净占比%": round_or_none(item.get("f75")),
                "中单净流入(万)": scale_or_none(item.get("f78"), 10000),
                "中单净占比%": round_or_none(item.get("f81")),
                "小单净流入(万)": scale_or_none(item.get("f84"), 10000),
                "小单净占比%": round_or_none(item.get("f87")),
                "资金流时间": flow_time,
                "抓取时间": fetch_time,
            }
        )
    return records


def main() -> None:
    args = parse_args()
    if args.batch_size <= 0:
        raise SystemExit("--batch-size 必须大于 0")
    if args.delay < 0:
        raise SystemExit("--delay 不能小于 0")

    require_dependencies()

    date_tag = normalize_date_tag(args.date)
    output_path = Path(args.output) if args.output else default_output_path(date_tag)
    symbols = resolve_symbols(args.code)
    exported_codes = read_exported_codes(output_path)
    pending_symbols = [
        symbol for symbol in symbols
        if normalize_code(symbol["代码"]) not in exported_codes
    ]

    total = len(symbols)
    skipped = total - len(pending_symbols)
    print(
        f"开始导出当天实时主力资金流：文件日期 {date_tag}，"
        f"股票 {total} 只，已存在 {skipped} 只，待抓取 {len(pending_symbols)} 只，"
        f"输出文件 {display_path(output_path)}",
        file=sys.stderr,
    )

    success_count = 0
    batches = chunks(pending_symbols, args.batch_size)
    for index, batch in enumerate(batches, start=1):
        first = batch[0]
        last = batch[-1]
        print(
            f"[{index}/{len(batches)}] 抓取 {len(batch)} 只："
            f"{first['代码']} {first.get('名称', '')} -> "
            f"{last['代码']} {last.get('名称', '')}",
            file=sys.stderr,
        )
        try:
            records = fetch_today_fund_flow_batch(batch)
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(
                f"[{index}/{len(batches)}] 获取失败：{type(exc).__name__}: {exc}\n"
                "已停止导出；已写入的数据会保留，稍后重试会跳过当天已写入股票。"
            ) from exc

        if records:
            output_df = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
            append_rows_csv(output_df, output_path)
            success_count += len(records)
            print(
                f"[{index}/{len(batches)}] 已写入 {len(records)} 条",
                file=sys.stderr,
            )

        if args.delay > 0 and index < len(batches):
            time.sleep(args.delay)

    print(
        f"导出完成：本次新增 {success_count} 条，跳过 {skipped} 条，文件 {display_path(output_path)}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
