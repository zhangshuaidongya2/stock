#!/usr/bin/env python3
"""Analyze stocks whose recent total main net inflow exceeds a threshold.

Examples:
  python analyze_flow_threshold_only.py
  python analyze_flow_threshold_only.py --days 3 --money 2
  python analyze_flow_threshold_only.py --days 5 --money 1.5 --top 20
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from analyze_flow_price import (
    DEFAULT_FLOW_PATH,
    DEFAULT_PRICE_PATH,
    DEFAULT_THRESHOLD_YI,
    build_merged_window,
    build_record,
    build_working_df,
    display_path,
    read_matrix,
    select_dates,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "分析 data/flow.csv 和 data/price.csv，"
            "输出最近 N 天主力净流入合计超过阈值的股票 JSON。"
        )
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
        help="分析最近多少天，默认 1。",
    )
    parser.add_argument(
        "--money",
        type=float,
        default=DEFAULT_THRESHOLD_YI,
        help=f"最近 N 天主力净流入合计阈值，单位亿元，默认 {DEFAULT_THRESHOLD_YI:g}。",
    )
    parser.add_argument(
        "--top",
        type=int,
        help="最多返回多少只股票；不传则返回全部。",
    )
    return parser.parse_args()


def limit_records(records: list[dict[str, Any]], top: int | None) -> list[dict[str, Any]]:
    if top is None or top <= 0:
        return records
    return records[:top]


def main() -> None:
    args = parse_args()
    if args.days <= 0:
        raise SystemExit("--days 必须大于 0")
    if args.money < 0:
        raise SystemExit("--money 不能小于 0")

    flow_df = read_matrix(Path(args.flow_input))
    price_df = read_matrix(Path(args.price_input))
    selected_dates = select_dates(flow_df, price_df, args.days)
    merged_df = build_merged_window(flow_df, price_df, selected_dates)
    working_df = build_working_df(merged_df, selected_dates)

    threshold_wan = args.money * 10000
    threshold_df = working_df[working_df["最近N天主力净流入合计(万)"] > threshold_wan].copy()
    threshold_df = threshold_df.sort_values("最近N天主力净流入合计(万)", ascending=False)

    records = [build_record(row, selected_dates) for _, row in threshold_df.iterrows()]
    result = {
        "分析参数": {
            "最近天数": len(selected_dates),
            "日期范围": selected_dates,
            "主力净流入阈值(亿)": args.money,
        },
        "筛选说明": "独立筛选：最近N天主力净流入合计超过阈值",
        "样本信息": {
            "完整样本数": int(len(working_df)),
            "命中个数": int(len(threshold_df)),
        },
        "最近N天主力净流入合计超过阈值": limit_records(records, args.top),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
