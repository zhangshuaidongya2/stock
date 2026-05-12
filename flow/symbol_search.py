#!/usr/bin/env python3
"""Shared stock symbol search helpers."""

from __future__ import annotations

import json
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable, Mapping


def normalize_text(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value)).strip().lower()
    return "".join(ch for ch in text if not ch.isspace())


def normalize_code(value: object) -> str:
    text = normalize_text(value)
    for prefix in ("sh", "sz", "bj"):
        if text.startswith(prefix):
            text = text[len(prefix) :]
            break
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def load_symbol_name_map(cache_path: Path) -> dict[str, str]:
    if not cache_path.exists():
        return {}
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        str(name).strip(): normalize_code(code)
        for name, code in payload.items()
        if str(name).strip() and normalize_code(code)
    }


def symbol_records_from_name_map(name_map: Mapping[str, object]) -> list[dict[str, str]]:
    records = []
    for name, code in name_map.items():
        normalized_name = str(name).strip()
        normalized_code = normalize_code(code)
        if not normalized_name or not normalized_code:
            continue
        records.append({"代码": normalized_code, "名称": normalized_name})
    return records


def symbol_records_from_rows(rows: Iterable[Mapping[str, object]]) -> list[dict[str, str]]:
    deduped: dict[str, str] = {}
    for row in rows:
        code = normalize_code(row.get("代码", ""))
        name = str(row.get("名称", "")).strip()
        if code and name:
            deduped[code] = name
    return [{"代码": code, "名称": name} for code, name in deduped.items()]


def load_symbol_records(cache_path: Path) -> list[dict[str, str]]:
    return symbol_records_from_name_map(load_symbol_name_map(cache_path))


def match_symbol_record(record: Mapping[str, str], query: str) -> tuple[int, str] | None:
    name = record["名称"]
    code = record["代码"]
    normalized_name = normalize_text(name)
    normalized_query = normalize_text(query)
    query_code = normalize_code(query)

    candidates: list[tuple[int, str]] = []
    if query_code:
        if code == query_code:
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


def search_symbol_records(
    records: Iterable[Mapping[str, str]],
    query: str,
    top: int = 10,
) -> list[dict[str, Any]]:
    matched = []
    for record in records:
        result = match_symbol_record(record, query)
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


def format_symbol_matches(
    matches: Iterable[Mapping[str, object]],
    *,
    include_reason: bool = False,
) -> str:
    lines = []
    for match in matches:
        line = f"{match['代码']} {match['名称']}"
        if include_reason and match.get("匹配方式"):
            line += f"（{match['匹配方式']}）"
        lines.append(line)
    return "\n".join(lines)


def build_suggestion_message(
    query: str,
    matches: list[Mapping[str, object]],
    *,
    not_found_prefix: str,
    include_reason: bool = False,
) -> str:
    if not matches:
        return f"{not_found_prefix}{query}；未找到接近项。"
    return (
        f"{not_found_prefix}{query}。可参考：\n"
        f"{format_symbol_matches(matches, include_reason=include_reason)}"
    )


def display_width(text: str) -> int:
    width = 0
    for ch in text:
        width += 2 if unicodedata.east_asian_width(ch) in {"W", "F"} else 1
    return width


def pad_text(text: str, width: int) -> str:
    padding = max(width - display_width(text), 0)
    return text + (" " * padding)


def render_symbol_table(rows: list[Mapping[str, object]]) -> str:
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
