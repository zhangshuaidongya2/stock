#!/usr/bin/env python3
"""Fetch Nikkei 225 quote data."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests


REQUEST_TIMEOUT = 10
COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://stooq.com/",
}
STOOQ_QUOTE_URL = "https://stooq.com/q/?s=%5Enkx"
EASTMONEY_HOSTS = [
    "push2.eastmoney.com",
    "1.push2.eastmoney.com",
    "2.push2.eastmoney.com",
    "5.push2.eastmoney.com",
]
EASTMONEY_HEADERS = {
    **COMMON_HEADERS,
    "Referer": "https://quote.eastmoney.com/",
}
EASTMONEY_PARAMS = {
    "np": "2",
    "fltt": "1",
    "invt": "2",
    "fs": "i:100.N225",
    "fields": "f12,f13,f14,f2,f4,f3,f17,f18,f15,f16,f7,f124",
    "fid": "f3",
    "pn": "1",
    "pz": "1",
    "po": "1",
    "dect": "1",
    "wbp2u": "|0|0|0|web",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="抓取日经225指数实时价格和涨跌信息。")
    parser.add_argument(
        "--source",
        choices=["auto", "stooq", "eastmoney"],
        default="auto",
        help="数据源：auto / stooq / eastmoney。",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="输出格式：text / json。",
    )
    parser.add_argument(
        "--precision",
        type=int,
        default=2,
        help="价格和涨跌数据小数位数，默认 2。",
    )
    parser.add_argument("--output", help="输出文件路径；不传则打印到终端。")
    return parser.parse_args()


def request_json(url: str, params: dict[str, Any]) -> Any:
    response = requests.get(
        url=url,
        params=params,
        headers=EASTMONEY_HEADERS,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def request_text(url: str) -> str:
    response = requests.get(url=url, headers=COMMON_HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.text


def parse_number(raw_value: str) -> float:
    cleaned = raw_value.strip().replace(",", "")
    cleaned = cleaned.replace("%", "").replace("(", "").replace(")", "")
    if cleaned in {"", "-"}:
        raise RuntimeError(f"数值字段为空：{raw_value!r}")
    return float(cleaned)


def extract_field(html: str, pattern: str, field_name: str) -> str:
    matched = re.search(pattern, html, flags=re.IGNORECASE)
    if not matched:
        raise RuntimeError(f"未找到字段：{field_name}")
    return matched.group(1).strip()


def fetch_from_stooq() -> dict[str, Any]:
    html = request_text(STOOQ_QUOTE_URL)
    quote_date = extract_field(html, r"id=aq_\^nkx_d2>([^<]+)<", "日期")
    quote_time_only = extract_field(html, r"id=aq_\^nkx_t1>([^<]+)<", "时间")
    quote_dt = datetime.strptime(
        f"{quote_date} {quote_time_only}",
        "%Y-%m-%d %H:%M:%S",
    ).replace(tzinfo=ZoneInfo("Asia/Tokyo"))
    return {
        "asset": "Nikkei 225",
        "symbol": "N225",
        "name": "日经225",
        "price": parse_number(extract_field(html, r"id=aq_\^nkx_c2\|3>([^<]+)<", "最新价")),
        "change_amount": parse_number(
            extract_field(html, r"id=aq_\^nkx_m2>([^<]+)<", "涨跌额")
        ),
        "change_percent": parse_number(
            extract_field(html, r"id=aq_\^nkx_m3>\(([^)]+)\)<", "涨跌幅")
        ),
        "open": parse_number(extract_field(html, r"id=aq_\^nkx_o>([^<]+)<", "开盘价")),
        "high": parse_number(extract_field(html, r"id=aq_\^nkx_h>([^<]+)<", "最高价")),
        "low": parse_number(extract_field(html, r"id=aq_\^nkx_l>([^<]+)<", "最低价")),
        "previous_close": parse_number(
            extract_field(html, r"id=aq_\^nkx_p>([^<]+)<", "昨收价")
        ),
        "amplitude_percent": None,
        "quote_time": quote_dt.isoformat(),
        "source": "stooq",
    }


def _extract_quote_item(payload: dict[str, Any]) -> dict[str, Any]:
    diff = payload.get("data", {}).get("diff", {})
    if isinstance(diff, dict) and diff:
        first_item = next(iter(diff.values()))
        if isinstance(first_item, dict):
            return first_item
    raise RuntimeError("返回数据缺少行情字段 data.diff")


def fetch_from_eastmoney() -> dict[str, Any]:
    errors = []
    for host in EASTMONEY_HOSTS:
        url = f"https://{host}/api/qt/clist/get"
        try:
            payload = request_json(url=url, params=EASTMONEY_PARAMS)
            quote = _extract_quote_item(payload)
            quote_time = datetime.fromtimestamp(
                int(quote["f124"]),
                tz=timezone.utc,
            ).astimezone(ZoneInfo("Asia/Shanghai"))
            return {
                "asset": "Nikkei 225",
                "symbol": str(quote["f12"]),
                "name": str(quote["f14"]),
                "price": float(quote["f2"]) / 100,
                "change_amount": float(quote["f4"]) / 100,
                "change_percent": float(quote["f3"]) / 100,
                "open": float(quote["f17"]) / 100,
                "high": float(quote["f15"]) / 100,
                "low": float(quote["f16"]) / 100,
                "previous_close": float(quote["f18"]) / 100,
                "amplitude_percent": float(quote["f7"]) / 100,
                "quote_time": quote_time.isoformat(),
                "source": "eastmoney",
            }
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{host}: {type(exc).__name__}: {exc}")
    raise RuntimeError(f"获取日经225行情失败。错误：{'；'.join(errors)}")


def fetch_price(source: str) -> dict[str, Any]:
    errors = []
    if source == "stooq":
        attempts = ["stooq"]
    elif source == "eastmoney":
        attempts = ["eastmoney"]
    else:
        attempts = ["stooq", "eastmoney"]

    for attempt in attempts:
        try:
            if attempt == "stooq":
                return fetch_from_stooq()
            return fetch_from_eastmoney()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{attempt}: {type(exc).__name__}: {exc}")
    raise RuntimeError(f"获取日经225行情失败。错误：{'；'.join(errors)}")


def format_text(payload: dict[str, Any], precision: int) -> str:
    message = (
        f"{payload['name']}({payload['symbol']}) 当前价格："
        f"{payload['price']:,.{precision}f}"
        f"；涨跌额：{payload['change_amount']:+,.{precision}f}"
        f"；涨跌幅：{payload['change_percent']:+.{precision}f}%"
        f"；开盘：{payload['open']:,.{precision}f}"
        f"；最高：{payload['high']:,.{precision}f}"
        f"；最低：{payload['low']:,.{precision}f}"
        f"；昨收：{payload['previous_close']:,.{precision}f}"
    )
    amplitude = payload.get("amplitude_percent")
    if amplitude is not None:
        message += f"；振幅：{float(amplitude):.{precision}f}%"
    message += f"（来源：{payload['source']}，行情时间：{payload['quote_time']}）"
    return message


def emit_output(content: str, output_path: str | None) -> None:
    if output_path:
        target = Path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        print(f"已写入：{target}")
        return
    print(content)


def main() -> None:
    args = parse_args()
    if args.precision < 0:
        raise SystemExit("--precision 不能小于 0")

    try:
        result = fetch_price(args.source)
    except Exception as exc:  # noqa: BLE001
        print(f"错误：{exc}", file=sys.stderr)
        raise SystemExit(1) from None

    result["fetched_at"] = datetime.now(timezone.utc).isoformat()
    if args.format == "json":
        content = json.dumps(result, ensure_ascii=False, indent=2)
    else:
        content = format_text(result, args.precision)
    emit_output(content, args.output)


if __name__ == "__main__":
    main()
