#!/usr/bin/env python3
"""Fetch realtime quote data for the Shanghai and Shenzhen indexes.

Examples:
  python a_price.py
  python a_price.py --index sh
  python a_price.py --format text
  python a_price.py --output data/index_quotes.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests


REQUEST_TIMEOUT = 10
TENCENT_QUOTE_URL = "https://qt.gtimg.cn/q="
TENCENT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
}
INDEX_CONFIG = {
    "sh": {
        "quote_symbol": "sh000001",
        "symbol": "000001",
        "name": "上证指数",
    },
    "sz": {
        "quote_symbol": "sz399001",
        "symbol": "399001",
        "name": "深证成指",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="抓取上证指数和深证成指实时行情。")
    parser.add_argument(
        "--index",
        choices=["all", "sh", "sz"],
        default="all",
        help="要抓取的指数：all=上证+深证，sh=上证指数，sz=深证成指。",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="json",
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


def request_quote_text(symbol: str) -> str:
    response = requests.get(
        url=TENCENT_QUOTE_URL + symbol,
        headers=TENCENT_HEADERS,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.content.decode("gbk", errors="ignore")


def require_float(value: Any, field_name: str) -> float:
    if value in (None, "", "-"):
        raise RuntimeError(f"返回字段为空：{field_name}")
    return float(value)


def require_field(parts: list[str], index: int, field_name: str) -> str:
    if index >= len(parts):
        raise RuntimeError(f"返回字段缺失：{field_name}")
    value = parts[index]
    if value in ("", "-"):
        raise RuntimeError(f"返回字段为空：{field_name}")
    return value


def parse_quote_time(raw_value: str) -> str | None:
    if raw_value in ("", "-", "0"):
        return None
    quote_time = datetime.strptime(raw_value, "%Y%m%d%H%M%S").replace(
        tzinfo=ZoneInfo("Asia/Shanghai")
    )
    return quote_time.isoformat()


def extract_quote_parts(payload_text: str) -> list[str]:
    for line in payload_text.splitlines():
        if '="' not in line:
            continue
        payload = line.split('="', 1)[1].rsplit('"', 1)[0]
        parts = payload.split("~")
        if len(parts) >= 44:
            return parts
    raise RuntimeError("腾讯返回数据格式异常")


def fetch_index_quote(index_key: str) -> dict[str, Any]:
    config = INDEX_CONFIG[index_key]
    try:
        parts = extract_quote_parts(request_quote_text(config["quote_symbol"]))
        return {
            "index": index_key,
            "symbol": require_field(parts, 2, "symbol"),
            "name": require_field(parts, 1, "name"),
            "price": require_float(require_field(parts, 3, "price"), "price"),
            "change_amount": require_float(
                require_field(parts, 31, "change_amount"),
                "change_amount",
            ),
            "change_percent": require_float(
                require_field(parts, 32, "change_percent"),
                "change_percent",
            ),
            "open": require_float(require_field(parts, 5, "open"), "open"),
            "high": require_float(require_field(parts, 33, "high"), "high"),
            "low": require_float(require_field(parts, 34, "low"), "low"),
            "previous_close": require_float(
                require_field(parts, 4, "previous_close"),
                "previous_close",
            ),
            "amplitude_percent": require_float(
                require_field(parts, 43, "amplitude_percent"),
                "amplitude_percent",
            ),
            "volume": require_float(require_field(parts, 36, "volume"), "volume"),
            "amount": require_float(require_field(parts, 37, "amount"), "amount")
            * 10_000,
            "quote_time": parse_quote_time(require_field(parts, 30, "quote_time")),
            "source": "tencent",
        }
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"获取{config['name']}行情失败：{type(exc).__name__}: {exc}") from exc


def fetch_quotes(index_option: str) -> list[dict[str, Any]]:
    if index_option == "all":
        index_keys = ["sh", "sz"]
    else:
        index_keys = [index_option]
    return [fetch_index_quote(index_key) for index_key in index_keys]


def format_amount_yi(amount: float, precision: int) -> str:
    return f"{amount / 100000000:,.{precision}f}亿"


def format_volume(volume: float) -> str:
    return f"{volume:,.0f}手"


def format_text(quotes: list[dict[str, Any]], precision: int) -> str:
    lines = []
    for quote in quotes:
        quote_time = quote.get("quote_time") or "未知"
        lines.append(
            f"{quote['name']}({quote['symbol']}) 当前点位："
            f"{quote['price']:,.{precision}f}"
            f"；涨跌额：{quote['change_amount']:+,.{precision}f}"
            f"；涨跌幅：{quote['change_percent']:+.{precision}f}%"
            f"；开盘：{quote['open']:,.{precision}f}"
            f"；最高：{quote['high']:,.{precision}f}"
            f"；最低：{quote['low']:,.{precision}f}"
            f"；昨收：{quote['previous_close']:,.{precision}f}"
            f"；振幅：{quote['amplitude_percent']:.{precision}f}%"
            f"；成交量：{format_volume(float(quote['volume']))}"
            f"；成交额：{format_amount_yi(float(quote['amount']), precision)}"
            f"（来源：{quote['source']}，行情时间：{quote_time}）"
        )
    return "\n".join(lines)


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
        quotes = fetch_quotes(args.index)
    except Exception as exc:  # noqa: BLE001
        print(f"错误：{exc}", file=sys.stderr)
        raise SystemExit(1) from None

    payload = {
        "quotes": quotes,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    if args.format == "json":
        content = json.dumps(payload, ensure_ascii=False, indent=2)
    else:
        content = format_text(quotes, args.precision)
    emit_output(content, args.output)


if __name__ == "__main__":
    main()
