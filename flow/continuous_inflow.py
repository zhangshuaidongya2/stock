#!/usr/bin/env python3
"""Analyze stocks with positive main net inflow on every recent day.

Examples:
  python flow/continuous_inflow.py
  python flow/continuous_inflow.py --days 3
  python flow/continuous_inflow.py --days 5 --top 20
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "分析 data/flow.csv 和 data/price.csv，"
            "输出指定天数每天主力净流入都大于 0 的股票 JSON。"
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

    flow_df = read_matrix(Path(args.flow_input))
    price_df = read_matrix(Path(args.price_input))
    selected_dates = select_dates(flow_df, price_df, args.days)
    merged_df = build_merged_window(flow_df, price_df, selected_dates)
    working_df = build_working_df(merged_df, selected_dates)
    days = len(selected_dates)
    label = window_label(days)

    flow_columns = [f"{date_tag}_flow" for date_tag in selected_dates]
    continuous_df = working_df[working_df[flow_columns].gt(0).all(axis=1)].copy()
    continuous_df = continuous_df.sort_values(WINDOW_NET_INFLOW_SUM_FIELD, ascending=False)

    records = [build_record(row, selected_dates, days) for _, row in continuous_df.iterrows()]
    result = {
        "分析参数": {
            "最近天数": days,
            "日期范围": selected_dates,
        },
        "筛选说明": f"独立筛选：{label}每天主力净流入都大于0",
        "样本信息": {
            "完整样本数": int(len(working_df)),
            "命中个数": int(len(continuous_df)),
        },
        f"{label}每天主力净流入都大于0": limit_records(records, args.top),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
