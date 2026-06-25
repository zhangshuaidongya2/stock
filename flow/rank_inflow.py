#!/usr/bin/env python3
"""Rank stocks by recent total main net inflow.

Examples:
  python flow/rank_inflow.py
  python flow/rank_inflow.py --days 8 --top 20
  python flow/rank_inflow.py --days 8 --bottom 20
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
    date_columns,
    display_path,
    read_matrix,
    select_dates,
)


DEFAULT_TOP = 10


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
        help="排行最近多少天的主力净流入合计，默认使用全部可共同分析日期。",
    )
    count_group = parser.add_mutually_exclusive_group()
    count_group.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP,
        help=f"返回前多少只股票，默认 {DEFAULT_TOP}。",
    )
    count_group.add_argument(
        "--bottom",
        type=int,
        help="返回主力净流入合计倒数多少只股票，按数值从低到高排列。",
    )
    return parser.parse_args()


def limit_records(records: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return records[:limit]


def amount_to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    if converted != converted:
        return None
    return converted


def format_amount(value: Any) -> str | None:
    converted = amount_to_float(value)
    if converted is None:
        return None
    return f"{converted:.2f}"


def display_amount(value: Any) -> str:
    return format_amount(value) or "--"


def build_daily_flow_price_map(
    flow_map: dict[str, Any],
    price_map: dict[str, Any],
    selected_dates: list[str],
) -> dict[str, str]:
    flow_strings = {
        date_tag: display_amount(flow_map.get(date_tag))
        for date_tag in selected_dates
    }
    price_strings = {
        date_tag: display_amount(price_map.get(date_tag))
        for date_tag in selected_dates
    }
    running_total_strings: dict[str, str] = {}
    running_inflow_wan = 0.0
    has_running_inflow = False
    for date_tag in selected_dates:
        flow_value = amount_to_float(flow_map.get(date_tag))
        if flow_value is not None:
            running_inflow_wan = round(running_inflow_wan + flow_value, 2)
            has_running_inflow = True
        running_total_strings[date_tag] = display_amount(
            running_inflow_wan if has_running_inflow else None
        )
    flow_width = max((len(value) for value in flow_strings.values()), default=0)
    price_width = max((len(value) for value in price_strings.values()), default=0)
    current_total_width = max((len(value) for value in running_total_strings.values()), default=0)
    gap_width = 4
    return {
        date_tag: (
            f"{flow_strings[date_tag]:<{flow_width + gap_width}}"
            f"价格: {price_strings[date_tag]:<{price_width + gap_width}}"
            f"当前余额: {running_total_strings[date_tag]:<{current_total_width}}"
        )
        for date_tag in selected_dates
    }


def build_rank_record(row: Any, selected_dates: list[str], days: int) -> dict[str, Any]:
    record = build_record(row, selected_dates, days)
    days_label = f"最近{days}天"
    flow_field = f"{days_label}每日净流入(万)"
    price_field = f"{days_label}每日价格"
    flow_map = record.get(flow_field, {})
    price_map = record.pop(price_field, {})
    if isinstance(flow_map, dict) and isinstance(price_map, dict):
        record[flow_field] = build_daily_flow_price_map(
            flow_map,
            price_map,
            selected_dates,
        )
    return record


def select_all_common_dates(flow_df: Any, price_df: Any) -> list[str]:
    flow_dates = date_columns(flow_df)
    price_dates = set(date_columns(price_df))
    common_dates = [column for column in flow_dates if column in price_dates]
    if not common_dates:
        raise SystemExit("flow.csv 和 price.csv 没有可共同分析的日期列。")
    return common_dates


def main() -> None:
    args = parse_args()
    if args.days is not None and args.days <= 0:
        raise SystemExit("--days 必须大于 0")
    if args.top is not None and args.top <= 0:
        raise SystemExit("--top 必须大于 0")
    if args.bottom is not None and args.bottom <= 0:
        raise SystemExit("--bottom 必须大于 0")

    flow_df = read_matrix(Path(args.flow_input))
    price_df = read_matrix(Path(args.price_input))
    selected_dates = (
        select_all_common_dates(flow_df, price_df)
        if args.days is None
        else select_dates(flow_df, price_df, args.days)
    )
    merged_df = build_merged_window(flow_df, price_df, selected_dates)
    working_df = build_working_df(merged_df, selected_dates)

    days = len(selected_dates)
    bottom = args.bottom is not None
    limit = args.bottom if bottom else args.top
    ranked_df = working_df.sort_values(
        WINDOW_NET_INFLOW_SUM_FIELD,
        ascending=bottom,
    )

    records = [build_rank_record(row, selected_dates, days) for _, row in ranked_df.iterrows()]
    result = {
        "分析参数": {
            "最近天数": days,
            "返回数量": limit,
        },
        "结果": limit_records(records, limit),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
