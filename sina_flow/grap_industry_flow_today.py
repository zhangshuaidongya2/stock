#!/usr/bin/env python3
"""Fetch today's Sina industry fund-flow snapshot from the cached SW2 boards.

Examples:
  python sina_flow/grap_industry_flow_today.py
  python sina_flow/grap_industry_flow_today.py --industry 自动化设备
  python sina_flow/grap_industry_flow_today.py --date 0527
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests


REQUEST_TIMEOUT = 10
FETCH_RETRY_TIMES = 3
FETCH_RETRY_BASE_SLEEP = 0.8
DEFAULT_WORKERS = 8
CHINA_TZ = ZoneInfo("Asia/Shanghai")
PROJECT_DIR = Path(__file__).resolve().parents[1]
INDUSTRY_CACHE_PATH = PROJECT_DIR / "stock_industries_cache.json"
TODAY_DATA_DIR = PROJECT_DIR / "sina_data" / "industry" / "today"
SINA_INDUSTRY_INTRADAY_FLOW_URL = (
    "https://money.finance.sina.com.cn/quotes_service/api/jsonp_v2.php/"
    "IO.XSRV2.CallbackList%5B%27cb%27%5D/MoneyFlow.ssx_bkzj_fszs"
)
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Referer": "https://money.finance.sina.com.cn/moneyflow/",
}

OUTPUT_COLUMNS = [
    "行业代码",
    "行业名称",
    "平均价",
    "平均涨跌幅%",
    "资金流向",
    "流入金额(万)",
    "流出金额(万)",
    "净流入(万)",
    "净流入率%",
    "主力净占比%",
    "散户净占比%",
    "资金流时间",
    "抓取时间",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="抓取缓存中申万二级行业的当天价格和资金流入流出快照。"
    )
    parser.add_argument(
        "--industry",
        help="行业名称或代码，多个用逗号分隔；不传则抓取缓存中的全部行业。",
    )
    parser.add_argument(
        "--date",
        default=datetime.now(CHINA_TZ).strftime("%m%d"),
        help="输出文件日期标识，格式 MMDD；默认今天。",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="输出 CSV 文件；不传则写入 sina_data/industry/today/MMDD.csv。",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"并发请求数，默认 {DEFAULT_WORKERS}。",
    )
    return parser.parse_args()


def normalize_date_tag(value: str) -> str:
    date_tag = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(date_tag) != 4:
        raise SystemExit("--date 必须是 MMDD 格式，例如 0527")
    return date_tag


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_DIR))
    except ValueError:
        return str(path)


def read_industry_cache() -> dict[str, str]:
    if not INDUSTRY_CACHE_PATH.exists():
        raise SystemExit(f"未找到行业缓存：{INDUSTRY_CACHE_PATH.name}")
    try:
        payload = json.loads(INDUSTRY_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"行业缓存无法解析：{exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit("行业缓存格式异常")
    industries = {
        str(name).strip(): str(code).strip()
        for name, code in payload.items()
        if str(name).strip() and str(code).strip()
    }
    if not industries:
        raise SystemExit("行业缓存为空")
    return industries


def resolve_industries(
    cache: dict[str, str],
    industry_arg: str | None,
) -> list[tuple[str, str]]:
    if not industry_arg:
        return sorted(cache.items(), key=lambda item: item[1])

    code_to_name = {code: name for name, code in cache.items()}
    resolved = []
    unknown = []
    for token in (value.strip() for value in industry_arg.split(",")):
        if not token:
            continue
        if token in cache:
            resolved.append((token, cache[token]))
        elif token in code_to_name:
            resolved.append((code_to_name[token], token))
        else:
            unknown.append(token)
    if unknown:
        raise SystemExit(f"未找到行业：{','.join(unknown)}")
    if not resolved:
        raise SystemExit("--industry 不能为空")
    return resolved


def safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def round_or_none(value: Any, digits: int = 2) -> float | None:
    number = safe_float(value)
    return round(number, digits) if number is not None else None


def scale_or_none(value: Any, scale: float, digits: int = 2) -> float | None:
    number = safe_float(value)
    return round(number / scale, digits) if number is not None else None


def percent_or_none(value: Any, digits: int = 2) -> float | None:
    number = safe_float(value)
    return round(number * 100, digits) if number is not None else None


def build_direction(value: Any) -> str:
    number = safe_float(value)
    if number is None or number == 0:
        return "持平" if number == 0 else ""
    return "流入" if number > 0 else "流出"


def parse_jsonp_payload(text: str) -> list[dict[str, Any]]:
    match = re.search(r"CallbackList\[[^\]]+\]\((.*)\)\s*;?\s*$", text, re.S)
    if match is None:
        raise RuntimeError("新浪行业资金流返回格式异常")
    payload = json.loads(match.group(1))
    if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
        raise RuntimeError("新浪行业资金流返回空数据")
    return [row for row in payload[1] if isinstance(row, dict)]


def fetch_industry_row(name: str, code: str) -> dict[str, Any]:
    errors = []
    for retry in range(FETCH_RETRY_TIMES + 1):
        try:
            response = requests.get(
                SINA_INDUSTRY_INTRADAY_FLOW_URL,
                params={"bankuai": code},
                headers=REQUEST_HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            records = parse_jsonp_payload(response.text)
            if not records:
                raise RuntimeError("新浪行业资金流返回空记录")
            latest = max(
                records,
                key=lambda row: (
                    str(row.get("opendate") or ""),
                    str(row.get("ticktime") or ""),
                ),
            )
            timestamp = datetime.strptime(
                f"{latest['opendate']} {latest['ticktime']}",
                "%Y-%m-%d %H:%M:%S",
            ).replace(tzinfo=CHINA_TZ)
            fetched_at = datetime.now(CHINA_TZ).replace(microsecond=0).isoformat()
            return {
                "行业代码": code,
                "行业名称": name,
                "平均价": round_or_none(latest.get("avg_price"), 4),
                "平均涨跌幅%": percent_or_none(latest.get("avg_changeratio")),
                "资金流向": build_direction(latest.get("netamount")),
                "流入金额(万)": scale_or_none(latest.get("inamount"), 10000),
                "流出金额(万)": scale_or_none(latest.get("outamount"), 10000),
                "净流入(万)": scale_or_none(latest.get("netamount"), 10000),
                "净流入率%": percent_or_none(latest.get("ratioamount")),
                "主力净占比%": percent_or_none(latest.get("r0_ratio")),
                "散户净占比%": percent_or_none(latest.get("r3_ratio")),
                "资金流时间": timestamp.isoformat(),
                "抓取时间": fetched_at,
            }
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{type(exc).__name__}: {exc}")
            if retry < FETCH_RETRY_TIMES:
                time.sleep(FETCH_RETRY_BASE_SLEEP * (retry + 1))
    raise RuntimeError(f"{name}({code}) 获取失败：{'；'.join(errors)}")


def write_output(rows: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    if args.workers <= 0:
        raise SystemExit("--workers 必须大于 0")
    date_tag = normalize_date_tag(args.date)
    output_path = args.output or TODAY_DATA_DIR / f"{date_tag}.csv"
    industries = resolve_industries(read_industry_cache(), args.industry)

    rows = []
    failures = []
    with ThreadPoolExecutor(max_workers=min(args.workers, len(industries))) as executor:
        futures = {
            executor.submit(fetch_industry_row, name, code): (name, code)
            for name, code in industries
        }
        for future in as_completed(futures):
            name, code = futures[future]
            try:
                rows.append(future.result())
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{name}({code}): {exc}")

    rows.sort(key=lambda row: str(row["行业代码"]))
    if not rows:
        for failure in failures:
            print(failure, file=sys.stderr)
        raise SystemExit("没有抓取到任何行业数据")
    write_output(rows, output_path)
    print(f"已写入 {display_path(output_path)}：{len(rows)} 个行业")
    if failures:
        print(f"抓取失败 {len(failures)} 个行业：", file=sys.stderr)
        for failure in failures:
            print(failure, file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
