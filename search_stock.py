#!/usr/bin/env python3
"""Search stocks from stock_symbols_cache.json only.

Examples:
  python search_stock.py 平安
  python search_stock.py 000001
  python search_stock.py 万科A
  python search_stock.py --top 20 招商
  python search_stock.py --format json 平安
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from flow.symbol_search import load_symbol_records, render_symbol_table, search_symbol_records


CACHE_PATH = Path(__file__).with_name("stock_symbols_cache.json")
DEFAULT_TOP = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从 stock_symbols_cache.json 模糊搜索股票代码和名称。"
    )
    parser.add_argument(
        "query",
        nargs="+",
        help="搜索关键词，支持股票名称、名称片段、代码、代码片段。",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP,
        help=f"最多返回多少条结果，默认 {DEFAULT_TOP}。",
    )
    parser.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="输出格式：table / json，默认 table。",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.top <= 0:
        raise SystemExit("--top 必须大于 0")

    query = " ".join(args.query).strip()
    if not query:
        raise SystemExit("搜索关键词不能为空")

    symbols = load_symbol_records(CACHE_PATH)
    results = search_symbol_records(symbols, query, args.top)
    if not results:
        raise SystemExit(f"在 {CACHE_PATH.name} 中未找到匹配：{query}")

    payload = [{key: value for key, value in row.items() if not key.startswith("_")} for row in results]
    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    print(render_symbol_table(payload))
    if len(payload) == 1:
        print(f"\n--code 可直接使用：{payload[0]['代码']}", file=sys.stderr)


if __name__ == "__main__":
    main()
