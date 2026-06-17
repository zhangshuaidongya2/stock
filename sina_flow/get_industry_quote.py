#!/usr/bin/env python3
"""Fetch one Sina industry/module realtime quote as JSON.

Examples:
  python sina_flow/get_industry_quote.py 元件
  python sina_flow/get_industry_quote.py sw2_270200
  python sina_flow/get_industry_quote.py HY0025
  python sina_flow/get_industry_quote.py HY0025 --output data/module_quote_hy0025.json
"""

from __future__ import annotations

import argparse
import unicodedata
import json
import subprocess
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from grap_industry_flow_today import (
    CHINA_TZ,
    REQUEST_HEADERS,
    SINA_INDUSTRY_INTRADAY_FLOW_URL,
    build_direction,
    display_path,
    parse_jsonp_payload,
    percent_or_none,
    read_industry_cache,
    resolve_industries,
    round_or_none,
    scale_or_none,
)


LEGACY_INDUSTRY_ALIAS_MAP = {
    "HY0025": "元件",
    "元器件": "元件",
    "电子元器件": "元件",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "抓取单个行业/模块的新浪实时行情 JSON。"
            "支持行业名称、当前 sw2 行业代码，以及少量旧别名代码。"
        )
    )
    parser.add_argument(
        "industry",
        help="行业/模块名称或代码，例如 元件、sw2_270200、HY0025。",
    )
    parser.add_argument("--output", type=Path, help="输出 JSON 文件；不传则打印到终端。")
    return parser.parse_args()


def normalize_token(value: str) -> str:
    return str(value).strip()


def normalize_text(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value)).strip().lower()
    return "".join(ch for ch in text if not ch.isspace())


def build_industry_records(cache: dict[str, str]) -> list[dict[str, str]]:
    return [
        {"代码": code, "名称": name}
        for name, code in sorted(cache.items(), key=lambda item: item[1])
    ]


def match_industry_record(record: dict[str, str], query: str) -> tuple[int, str] | None:
    name = record["名称"]
    code = record["代码"]
    normalized_name = normalize_text(name)
    normalized_code = normalize_text(code)
    normalized_query = normalize_text(query)
    if not normalized_query:
        return None

    candidates: list[tuple[int, str]] = []
    if normalized_code == normalized_query:
        candidates.append((1000, "代码完全匹配"))
    elif normalized_code.startswith(normalized_query):
        candidates.append((920 - max(len(normalized_code) - len(normalized_query), 0), "代码前缀匹配"))
    elif normalized_query in normalized_code:
        candidates.append((860 - normalized_code.index(normalized_query), "代码片段匹配"))

    if normalized_name == normalized_query:
        candidates.append((980, "名称完全匹配"))
    elif normalized_name.startswith(normalized_query):
        candidates.append((900 - max(len(normalized_name) - len(normalized_query), 0), "名称前缀匹配"))
    elif normalized_query in normalized_name:
        candidates.append((840 - normalized_name.index(normalized_query), "名称片段匹配"))
    elif len(normalized_query) >= 2:
        ratio = SequenceMatcher(None, normalized_query, normalized_name).ratio()
        if ratio >= 0.55:
            candidates.append((int(ratio * 100) + 500, "名称近似匹配"))

    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])


def search_industry_records(records: list[dict[str, str]], query: str, top: int = 8) -> list[dict[str, str]]:
    matched: list[dict[str, str]] = []
    for record in records:
        result = match_industry_record(record, query)
        if result is None:
            continue
        score, reason = result
        matched.append(
            {
                "代码": record["代码"],
                "名称": record["名称"],
                "匹配方式": reason,
                "_score": str(score),
            }
        )
    matched.sort(key=lambda item: (-int(item["_score"]), len(normalize_text(item["名称"])), item["代码"]))
    return matched[:top]


def build_industry_suggestion_message(query: str, matches: list[dict[str, str]]) -> str:
    if not matches:
        return f"未找到行业：{query}；未找到接近项。"
    lines = [f"{match['代码']} {match['名称']}（{match['匹配方式']}）" for match in matches]
    return f"未找到行业：{query}。可参考：\n" + "\n".join(lines)


def resolve_industry_query(cache: dict[str, str], query: str) -> tuple[str, str, str | None]:
    token = normalize_token(query)
    alias_target = LEGACY_INDUSTRY_ALIAS_MAP.get(token)
    resolved_token = alias_target or token
    try:
        resolved = resolve_industries(cache, resolved_token)
    except SystemExit as exc:
        message = str(exc)
        if not message.startswith("未找到行业："):
            raise
        suggestions = search_industry_records(build_industry_records(cache), resolved_token, top=8)
        raise SystemExit(build_industry_suggestion_message(query, suggestions)) from None
    if len(resolved) != 1:
        raise SystemExit(f"查询必须只命中一个行业，当前命中 {len(resolved)} 个。")
    name, code = resolved[0]
    return name, code, alias_target


def build_result(
    *,
    query: str,
    alias_target: str | None,
    row: dict[str, Any],
) -> dict[str, Any]:
    result = {
        "查询输入": query,
        "解析结果": {
            "行业名称": row["行业名称"],
            "行业代码": row["行业代码"],
        },
        "实时行情": row,
    }
    if alias_target is not None:
        result["解析结果"]["别名映射"] = f"{query} -> {alias_target}"
    return result


def fetch_industry_row_with_curl(name: str, code: str) -> dict[str, Any]:
    command = [
        "curl",
        "-sS",
        "-H",
        f"User-Agent: {REQUEST_HEADERS['User-Agent']}",
        "-H",
        f"Referer: {REQUEST_HEADERS['Referer']}",
        f"{SINA_INDUSTRY_INTRADAY_FLOW_URL}?bankuai={code}",
    ]
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode("utf-8", "ignore").strip()
        raise RuntimeError(f"{name}({code}) 获取失败：{stderr or exc}") from exc

    text = result.stdout.decode("gbk", "ignore")
    records = parse_jsonp_payload(text)
    if not records:
        raise RuntimeError(f"{name}({code}) 返回空记录")

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


def write_output(result: dict[str, Any], output_path: Path | None) -> None:
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    if output_path is None:
        print(payload)
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(payload + "\n", encoding="utf-8")
    print(f"已写入 {display_path(output_path)}")


def main() -> None:
    args = parse_args()
    cache = read_industry_cache()
    name, code, alias_target = resolve_industry_query(cache, args.industry)
    row = fetch_industry_row_with_curl(name, code)
    result = build_result(
        query=args.industry,
        alias_target=alias_target,
        row=row,
    )
    write_output(result, args.output)


if __name__ == "__main__":
    main()
