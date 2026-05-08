#!/usr/bin/env python3
"""Rank stocks by recent total main net inflow.

Examples:
  python flow/rank_inflow.py
  python flow/rank_inflow.py --days 8 --top 20
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from analyze_flow_price import (
    DEFAULT_FLOW_PATH,
    DEFAULT_PRICE_PATH,
    WINDOW_NET_INFLOW_SUM_FIELD,
    build_merged_window,
    build_record,
    build_working_df,
    display_path,
    read_matrix,
    select_dates,
    window_label,
)


DEFAULT_TOP = 20


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="按最近 N 天主力净流入合计排序，输出流入排行。"
    )
    parser.add_argument(
        "--flow-input",
        default=str(DEFAULT_FLOW_PATH),
        help=f"净流入矩阵 CSV，默认 {display_path(DEFAULT_FLOW_PATH)}。",
    )
    parser.add_argument(
        "--price-input",
        default=str(DEFAULT_PRICE_PATH),
        help=f"价格矩阵 CSV，默认 {display_path(DEFAULT_PRICE_PATH)}。",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="排行最近多少天的主力净流入合计，默认 1。",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP,
        help=f"返回前多少只股票，默认 {DEFAULT_TOP}。",
    )
    return parser.parse_args()


def limit_records(records: list[dict[str, Any]], top: int) -> list[dict[str, Any]]:
    return records[:top]


def main() -> None:
    args = parse_args()
    if args.days <= 0:
        raise SystemExit("--days 必须大于 0")
    if args.top <= 0:
        raise SystemExit("--top 必须大于 0")

    flow_df = read_matrix(Path(args.flow_input))
    price_df = read_matrix(Path(args.price_input))
    selected_dates = select_dates(flow_df, price_df, args.days)
    merged_df = build_merged_window(flow_df, price_df, selected_dates)
    working_df = build_working_df(merged_df, selected_dates)

    days = len(selected_dates)
    label = window_label(days)
    ranked_df = working_df.sort_values(
        WINDOW_NET_INFLOW_SUM_FIELD,
        ascending=False,
    )

    records = [build_record(row, selected_dates, days) for _, row in ranked_df.iterrows()]
    result = {
        "分析参数": {
            "最近天数": days,
            "返回数量": args.top,
            "日期范围": selected_dates,
        },
        "排序说明": f"{label}主力净流入合计从高到低",
        "样本信息": {
            "完整样本数": int(len(working_df)),
        },
        "结果": limit_records(records, args.top),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
