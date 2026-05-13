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
EASTMONEY_HOSTS = [
    "push2.eastmoney.com",
    "1.push2.eastmoney.com",
    "2.push2.eastmoney.com",
    "5.push2.eastmoney.com",
]
EASTMONEY_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://quote.eastmoney.com/",
}
INDEX_CONFIG = {
    "sh": {
        "secid": "1.000001",
        "symbol": "000001",
        "name": "上证指数",
    },
    "sz": {
        "secid": "0.399001",
        "symbol": "399001",
        "name": "深证成指",
    },
}
QUOTE_FIELDS = "f57,f58,f43,f44,f45,f46,f47,f48,f60,f169,f170,f171,f86"


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


def request_json(url: str, params: dict[str, Any]) -> Any:
    response = requests.get(
        url=url,
        params=params,
        headers=EASTMONEY_HEADERS,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def require_float(value: Any, field_name: str) -> float:
    if value in (None, "", "-"):
        raise RuntimeError(f"返回字段为空：{field_name}")
    return float(value)


def scale_field(data: dict[str, Any], key: str, scale: float = 1.0) -> float:
    return require_float(data.get(key), key) / scale


def parse_quote_time(data: dict[str, Any]) -> str | None:
    raw_value = data.get("f86")
    if raw_value in (None, "", "-", 0, "0"):
        return None
    quote_time = datetime.fromtimestamp(
        int(float(raw_value)),
        tz=timezone.utc,
    ).astimezone(ZoneInfo("Asia/Shanghai"))
    return quote_time.isoformat()


def extract_quote_item(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    if isinstance(data, dict) and data:
        return data
    raise RuntimeError("返回数据缺少 data 字段")


def fetch_index_quote(index_key: str) -> dict[str, Any]:
    config = INDEX_CONFIG[index_key]
    errors = []
    for host in EASTMONEY_HOSTS:
        url = f"https://{host}/api/qt/stock/get"
        try:
            payload = request_json(
                url=url,
                params={
                    "secid": config["secid"],
                    "fields": QUOTE_FIELDS,
                    "invt": "2",
                    "fltt": "1",
                },
            )
            quote = extract_quote_item(payload)
            return {
                "index": index_key,
                "symbol": str(quote.get("f57") or config["symbol"]),
                "name": str(quote.get("f58") or config["name"]),
                "price": scale_field(quote, "f43", 100),
                "change_amount": scale_field(quote, "f169", 100),
                "change_percent": scale_field(quote, "f170", 100),
                "open": scale_field(quote, "f46", 100),
                "high": scale_field(quote, "f44", 100),
                "low": scale_field(quote, "f45", 100),
                "previous_close": scale_field(quote, "f60", 100),
                "amplitude_percent": scale_field(quote, "f171", 100),
                "volume": scale_field(quote, "f47"),
                "amount": scale_field(quote, "f48"),
                "quote_time": parse_quote_time(quote),
                "source": "eastmoney",
            }
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{host}: {type(exc).__name__}: {exc}")
    raise RuntimeError(f"获取{config['name']}行情失败。错误：{'；'.join(errors)}")


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
