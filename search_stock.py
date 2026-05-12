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
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any


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


def normalize_text(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value)).strip().lower()
    return "".join(ch for ch in text if not ch.isspace())


def normalize_code(value: object) -> str:
    text = normalize_text(value)
    for prefix in ("sh", "sz", "bj"):
        if text.startswith(prefix):
            text = text[len(prefix) :]
            break
    return "".join(ch for ch in text if ch.isdigit())


def load_symbols() -> list[dict[str, str]]:
    if not CACHE_PATH.exists():
        raise SystemExit(f"文件不存在：{CACHE_PATH}")
    try:
        payload = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"读取缓存失败：{exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit("stock_symbols_cache.json 格式错误：期望顶层为对象")

    symbols = []
    for name, code in payload.items():
        normalized_name = str(name).strip()
        normalized_code = normalize_code(code)
        if not normalized_name or not normalized_code:
            continue
        symbols.append({"代码": normalized_code.zfill(6), "名称": normalized_name})
    return symbols


def display_width(text: str) -> int:
    width = 0
    for ch in text:
        width += 2 if unicodedata.east_asian_width(ch) in {"W", "F"} else 1
    return width


def pad_text(text: str, width: int) -> str:
    padding = max(width - display_width(text), 0)
    return text + (" " * padding)


def match_record(record: dict[str, str], query: str) -> tuple[int, str] | None:
    name = record["名称"]
    code = record["代码"]
    normalized_name = normalize_text(name)
    normalized_query = normalize_text(query)
    query_code = normalize_code(query)

    candidates: list[tuple[int, str]] = []

    if query_code:
        if code == query_code.zfill(6):
            candidates.append((1000, "代码完全匹配"))
        elif code.startswith(query_code):
            candidates.append((920 - max(len(code) - len(query_code), 0), "代码前缀匹配"))
        elif query_code in code:
            candidates.append((860 - code.index(query_code), "代码片段匹配"))

    if normalized_query:
        if normalized_name == normalized_query:
            candidates.append((980, "名称完全匹配"))
        elif normalized_name.startswith(normalized_query):
            candidates.append(
                (900 - max(len(normalized_name) - len(normalized_query), 0), "名称前缀匹配")
            )
        elif normalized_query in normalized_name:
            candidates.append((840 - normalized_name.index(normalized_query), "名称片段匹配"))
        elif len(normalized_query) >= 2:
            ratio = SequenceMatcher(None, normalized_query, normalized_name).ratio()
            if ratio >= 0.55:
                candidates.append((int(ratio * 100) + 500, "名称近似匹配"))

    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])


def search_symbols(symbols: list[dict[str, str]], query: str, top: int) -> list[dict[str, Any]]:
    matched = []
    for record in symbols:
        result = match_record(record, query)
        if result is None:
            continue
        score, reason = result
        matched.append(
            {
                "代码": record["代码"],
                "名称": record["名称"],
                "匹配方式": reason,
                "_score": score,
            }
        )

    matched.sort(
        key=lambda item: (-item["_score"], len(normalize_text(item["名称"])), item["代码"])
    )
    return matched[:top]


def render_table(rows: list[dict[str, Any]]) -> str:
    columns = ["代码", "名称", "匹配方式"]
    widths = {
        column: max(display_width(column), *(display_width(str(row[column])) for row in rows))
        for column in columns
    }
    lines = []
    lines.append("  ".join(pad_text(column, widths[column]) for column in columns))
    lines.append("  ".join("-" * widths[column] for column in columns))
    for row in rows:
        lines.append("  ".join(pad_text(str(row[column]), widths[column]) for column in columns))
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    if args.top <= 0:
        raise SystemExit("--top 必须大于 0")

    query = " ".join(args.query).strip()
    if not query:
        raise SystemExit("搜索关键词不能为空")

    symbols = load_symbols()
    results = search_symbols(symbols, query, args.top)
    if not results:
        raise SystemExit(f"在 {CACHE_PATH.name} 中未找到匹配：{query}")

    payload = [{key: value for key, value in row.items() if not key.startswith("_")} for row in results]
    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    print(render_table(payload))
    if len(payload) == 1:
        print(f"\n--code 可直接使用：{payload[0]['代码']}", file=sys.stderr)


if __name__ == "__main__":
    main()
