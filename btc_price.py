#!/usr/bin/env python3
"""Fetch the current Bitcoin price.

Examples:
  python btc_price.py
  python btc_price.py --currency CNY
  python btc_price.py --change-days 3 --format json
  python btc_price.py --currency USDT --source binance --format json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


REQUEST_TIMEOUT = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="抓取比特币当前价格。")
    parser.add_argument(
        "--currency",
        choices=["USD", "CNY", "USDT"],
        default="USD",
        help="报价币种：USD / CNY / USDT。",
    )
    parser.add_argument(
        "--source",
        choices=["auto", "coinbase", "coingecko", "binance"],
        default="auto",
        help="数据源：auto / coinbase / coingecko / binance。",
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
        help="价格小数位数，默认 2。",
    )
    parser.add_argument(
        "--change-days",
        type=float,
        default=1.0,
        help="涨跌统计窗口天数，支持小数（例如 0.5、1、7）。",
    )
    parser.add_argument("--output", help="输出文件路径；不传则打印到终端。")
    return parser.parse_args()


def request_json(url: str, params: dict[str, Any] | None = None) -> Any:
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def fetch_from_coinbase(currency: str) -> dict[str, Any]:
    payload = request_json(
        "https://api.coinbase.com/v2/prices/spot",
        params={"currency": currency},
    )
    data = payload.get("data", {})
    amount = float(data["amount"])
    return {"price": amount, "currency": currency, "source": "coinbase"}


def fetch_from_coingecko(currency: str) -> dict[str, Any]:
    payload = request_json(
        "https://api.coingecko.com/api/v3/simple/price",
        params={"ids": "bitcoin", "vs_currencies": currency.lower()},
    )
    btc_data = payload.get("bitcoin", {})
    amount = btc_data.get(currency.lower())
    if amount is None:
        raise RuntimeError("CoinGecko 返回数据缺少 BTC 价格字段")
    return {"price": float(amount), "currency": currency, "source": "coingecko"}


def fetch_from_binance() -> dict[str, Any]:
    payload = request_json(
        "https://api.binance.com/api/v3/ticker/price",
        params={"symbol": "BTCUSDT"},
    )
    amount = float(payload["price"])
    return {"price": amount, "currency": "USDT", "source": "binance"}


def build_change_payload(
    current_price: float,
    reference_price: float,
    change_days: float,
    source: str,
) -> dict[str, Any]:
    if reference_price == 0:
        raise RuntimeError("历史参考价为 0，无法计算涨跌幅")
    change_amount = current_price - reference_price
    change_percent = change_amount / reference_price * 100
    return {
        "change_days": change_days,
        "change_source": source,
        "change_reference_price": reference_price,
        "change_amount": change_amount,
        "change_percent": change_percent,
    }


def fetch_change_from_coingecko(
    currency: str,
    change_days: float,
    current_price: float,
) -> dict[str, Any]:
    now_ts = int(datetime.now(timezone.utc).timestamp())
    from_ts = int(now_ts - change_days * 24 * 60 * 60)
    payload = request_json(
        "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range",
        params={
            "vs_currency": currency.lower(),
            "from": from_ts,
            "to": now_ts,
        },
    )
    prices = payload.get("prices")
    if not isinstance(prices, list) or not prices:
        raise RuntimeError("CoinGecko 返回历史价格为空")
    reference_price = float(prices[0][1])
    return build_change_payload(
        current_price=current_price,
        reference_price=reference_price,
        change_days=change_days,
        source="coingecko",
    )


def pick_binance_interval(change_days: float) -> str:
    if change_days <= 1:
        return "1m"
    if change_days <= 7:
        return "5m"
    if change_days <= 31:
        return "1h"
    return "4h"


def fetch_change_from_binance(
    change_days: float,
    current_price: float,
) -> dict[str, Any]:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = now_ms - int(change_days * 24 * 60 * 60 * 1000)
    interval = pick_binance_interval(change_days)
    payload = request_json(
        "https://api.binance.com/api/v3/klines",
        params={
            "symbol": "BTCUSDT",
            "interval": interval,
            "startTime": start_ms,
            "endTime": now_ms,
            "limit": 1,
        },
    )
    if not isinstance(payload, list) or not payload:
        raise RuntimeError("Binance 返回历史 K 线为空")
    reference_price = float(payload[0][1])
    return build_change_payload(
        current_price=current_price,
        reference_price=reference_price,
        change_days=change_days,
        source="binance",
    )


def fetch_change(
    currency: str,
    source: str,
    change_days: float,
    current_price: float,
) -> dict[str, Any]:
    errors = []
    if source == "binance" and currency == "USDT":
        attempts = ["binance", "coingecko"]
    else:
        attempts = ["coingecko"]

    for attempt in attempts:
        try:
            if attempt == "binance":
                return fetch_change_from_binance(change_days, current_price)
            return fetch_change_from_coingecko(currency, change_days, current_price)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{attempt}: {type(exc).__name__}: {exc}")

    raise RuntimeError(f"获取 BTC 涨跌数据失败。错误：{'；'.join(errors)}")


def fetch_price(currency: str, source: str) -> dict[str, Any]:
    if source == "coinbase":
        if currency == "USDT":
            raise RuntimeError("coinbase 不支持 USDT 现货报价，请改用 binance 或 auto")
        return fetch_from_coinbase(currency)

    if source == "coingecko":
        return fetch_from_coingecko(currency)

    if source == "binance":
        result = fetch_from_binance()
        if currency != "USDT":
            raise RuntimeError("binance 脚本当前仅支持 USDT 报价")
        return result

    errors = []
    attempts: list[tuple[str, str]]
    if currency == "USDT":
        attempts = [("binance", "USDT"), ("coingecko", "USDT")]
    elif currency == "CNY":
        attempts = [("coinbase", "CNY"), ("coingecko", "CNY")]
    else:
        attempts = [("coinbase", "USD"), ("coingecko", "USD"), ("binance", "USDT")]

    for name, quote in attempts:
        try:
            if name == "coinbase":
                return fetch_from_coinbase(quote)
            if name == "coingecko":
                return fetch_from_coingecko(quote)
            return fetch_from_binance()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{name}: {type(exc).__name__}: {exc}")

    raise RuntimeError(f"获取 BTC 价格失败。错误：{'；'.join(errors)}")


def format_days(days: Any) -> str:
    try:
        value = float(days)
    except (TypeError, ValueError):
        return str(days)
    if value.is_integer():
        return str(int(value))
    return f"{value:g}"


def format_text(payload: dict[str, Any], precision: int) -> str:
    timestamp = payload["fetched_at"]
    price = round(float(payload["price"]), precision)
    currency = payload["currency"]
    source = payload["source"]
    message = (
        f"BTC 当前价格：{price:,.{precision}f} {currency}"
        f"（来源：{source}，时间：{timestamp}）"
    )
    if "change_percent" in payload and "change_amount" in payload:
        days = format_days(payload.get("change_days", 1))
        change_percent = float(payload["change_percent"])
        change_amount = float(payload["change_amount"])
        change_source = payload.get("change_source", "unknown")
        message += (
            f"；近{days}天涨跌幅：{change_percent:+.{precision}f}%"
            f"；涨跌额：{change_amount:+,.{precision}f} {currency}"
            f"（变化来源：{change_source}）"
        )
    elif "change_error" in payload:
        message += f"；涨跌数据获取失败：{payload['change_error']}"
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
    if args.change_days <= 0:
        raise SystemExit("--change-days 必须大于 0")

    try:
        result = fetch_price(args.currency, args.source)
    except Exception as exc:  # noqa: BLE001
        print(f"错误：{exc}", file=sys.stderr)
        raise SystemExit(1) from None

    result["asset"] = "BTC"
    result["fetched_at"] = datetime.now(timezone.utc).isoformat()
    try:
        change = fetch_change(
            currency=result["currency"],
            source=result["source"],
            change_days=args.change_days,
            current_price=float(result["price"]),
        )
        result.update(change)
    except Exception as exc:  # noqa: BLE001
        result["change_days"] = args.change_days
        result["change_error"] = f"{type(exc).__name__}: {exc}"

    if args.format == "json":
        content = json.dumps(result, ensure_ascii=False, indent=2)
    else:
        content = format_text(result, args.precision)

    emit_output(content, args.output)


if __name__ == "__main__":
    main()
