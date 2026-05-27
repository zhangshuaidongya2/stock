#!/usr/bin/env python3
"""Rank a Sina industry fund-flow snapshot by net inflow as JSON.

Examples:
  python sina_flow/rank_industry_flow_today.py
  python sina_flow/rank_industry_flow_today.py --top 10
  python sina_flow/rank_industry_flow_today.py --bottom 10
  python sina_flow/rank_industry_flow_today.py --date 0527
  python sina_flow/rank_industry_flow_today.py --input sina_data/industry/today/0527.csv
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parents[1]
TODAY_DATA_DIR = PROJECT_DIR / "sina_data" / "industry" / "today"
DEFAULT_TOP = 20
RANK_COLUMN = "净流入(万)"
NUMERIC_COLUMNS = [
    "平均价",
    "平均涨跌幅%",
    "流入金额(万)",
    "流出金额(万)",
    "净流入(万)",
    "净流入率%",
    "主力净占比%",
    "散户净占比%",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="读取行业资金流 CSV，按净流入从高到低输出 JSON 排行。"
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="输入 CSV 文件；不传则读取 sina_data/industry/today/ 下 --date 对应文件。",
    )
    parser.add_argument(
        "--date",
        default=datetime.now().astimezone().strftime("%m%d"),
        help="文件日期标识，格式 MMDD，例如 0527；默认今天。",
    )
    count_group = parser.add_mutually_exclusive_group()
    count_group.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP,
        help=f"输出前多少位，默认 {DEFAULT_TOP}。",
    )
    count_group.add_argument(
        "--bottom",
        type=int,
        help="输出净流入倒数多少位，按净流入从低到高排列。",
    )
    return parser.parse_args()


def normalize_date_tag(value: str) -> str:
    date_tag = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(date_tag) != 4:
        raise SystemExit("--date 必须是 MMDD 格式，例如 0527")
    return date_tag


def resolve_input_path(input_path: Path | None, date_tag: str) -> Path:
    if input_path is None:
        return TODAY_DATA_DIR / f"{date_tag}.csv"
    return input_path if input_path.is_absolute() else PROJECT_DIR / input_path


def read_industry_csv(input_path: Path) -> pd.DataFrame:
    if not input_path.exists():
        raise SystemExit(f"输入文件不存在：{input_path}")
    try:
        df = pd.read_csv(input_path, encoding="utf-8-sig")
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"读取 CSV 失败：{exc}") from exc
    if df.empty:
        raise SystemExit(f"输入文件为空：{input_path}")
    missing = [column for column in ("行业代码", "行业名称", RANK_COLUMN) if column not in df.columns]
    if missing:
        raise SystemExit("CSV 缺少字段：" + "、".join(missing))
    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def clean_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def main() -> None:
    args = parse_args()
    if args.top is not None and args.top <= 0:
        raise SystemExit("--top 必须大于 0")
    if args.bottom is not None and args.bottom <= 0:
        raise SystemExit("--bottom 必须大于 0")

    date_tag = normalize_date_tag(args.date)
    input_path = resolve_input_path(args.input, date_tag)
    df = read_industry_csv(input_path)
    bottom = args.bottom is not None
    limit = args.bottom if bottom else args.top
    ranked = (
        df.dropna(subset=[RANK_COLUMN])
        .sort_values(RANK_COLUMN, ascending=bottom)
        .head(limit)
    )
    records = [
        {str(column): clean_value(value) for column, value in row.items()}
        for row in ranked.to_dict(orient="records")
    ]
    print(json.dumps(records, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
