#!/usr/bin/env python3
"""Rank stocks by recent total main net inflow.

Examples:
  python flow/rank_inflow.py
  python flow/rank_inflow.py --days 8 --top 20
  python flow/rank_inflow.py --date 0630 --days 8 --top 20
  python flow/rank_inflow.py --days 8 --bottom 20
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
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
    round_or_none,
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
        help="排行多少天的主力净流入合计；不传则使用截止日期及之前全部可共同分析日期。",
    )
    parser.add_argument(
        "--date",
        help="窗口截止日期，支持 MMDD、YYYYMMDD、YYYY-MM-DD；不传则使用最新可共同分析日期。",
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


def build_next_day_change(
    target_price: Any,
    next_price: Any,
) -> tuple[str | None, float | None]:
    target = amount_to_float(target_price)
    next_value = amount_to_float(next_price)
    if target is None or target == 0 or next_value is None:
        return None, None

    change_pct = round(next_value / target * 100 - 100, 2)
    if change_pct > 0:
        return "上涨", change_pct
    if change_pct < 0:
        return "下跌", change_pct
    return "不变", change_pct


def build_rank_record(
    row: Any,
    selected_dates: list[str],
    days: int,
    next_day_prices: dict[str, tuple[str | None, Any, Any]] | None = None,
) -> dict[str, Any]:
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
    if next_day_prices is not None:
        code = str(row.get("代码", "")).zfill(6)
        next_date, target_price, next_price = next_day_prices.get(code, (None, None, None))
        next_day_flag, next_day_change_pct = build_next_day_change(target_price, next_price)
        record["第二天日期"] = next_date
        record["第二天价格"] = round_or_none(next_price)
        record["第二天涨跌"] = next_day_flag
        record["第二天涨跌幅%"] = next_day_change_pct
    return record


def select_all_common_dates(flow_df: Any, price_df: Any) -> list[str]:
    flow_dates = date_columns(flow_df)
    price_dates = set(date_columns(price_df))
    common_dates = [column for column in flow_dates if column in price_dates]
    if not common_dates:
        raise SystemExit("flow.csv 和 price.csv 没有可共同分析的日期列。")
    return common_dates


def normalize_date_tag(value: str) -> str:
    raw = str(value).strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 4:
        return digits
    if len(digits) == 8:
        try:
            parsed = datetime.strptime(digits, "%Y%m%d")
        except ValueError as exc:
            raise SystemExit(f"无法解析日期：{value}") from exc
        return parsed.strftime("%m%d")
    raise SystemExit("--date 必须是 MMDD、YYYYMMDD 或 YYYY-MM-DD，例如 0630 或 2026-06-30")


def select_common_dates_until(
    flow_df: Any,
    price_df: Any,
    days: int | None,
    end_date: str | None,
) -> list[str]:
    common_dates = select_all_common_dates(flow_df, price_df)
    target_date = normalize_date_tag(end_date) if end_date else common_dates[-1]
    if target_date not in common_dates:
        raise SystemExit(
            f"--date={target_date} 不在 flow.csv 和 price.csv 的共同日期列中。"
            f"当前可用日期：{'、'.join(common_dates)}"
        )

    end_index = common_dates.index(target_date)
    if days is None:
        return common_dates[: end_index + 1]

    start_index = end_index - days + 1
    if start_index < 0:
        available_days = end_index + 1
        raise SystemExit(
            f"--date={target_date} 往前只有 {available_days} 个可共同分析日期，"
            f"不足 --days={days}。"
        )
    return common_dates[start_index : end_index + 1]


def next_price_date(price_df: Any, target_date: str) -> str | None:
    price_dates = date_columns(price_df)
    if target_date not in price_dates:
        return None
    target_index = price_dates.index(target_date)
    if target_index + 1 >= len(price_dates):
        return None
    return price_dates[target_index + 1]


def build_next_day_price_map(
    price_df: Any,
    target_date: str,
    next_date: str | None,
) -> dict[str, tuple[str | None, Any, Any]]:
    if next_date is None:
        return {
            str(row.get("代码", "")).zfill(6): (None, row.get(target_date), None)
            for _, row in price_df.iterrows()
        }
    return {
        str(row.get("代码", "")).zfill(6): (
            next_date,
            row.get(target_date),
            row.get(next_date),
        )
        for _, row in price_df.iterrows()
    }


def build_next_day_summary(records: list[dict[str, Any]]) -> dict[str, int]:
    summary = {
        "可比较个数": 0,
        "第二天上涨个数": 0,
        "第二天下跌个数": 0,
        "第二天不变个数": 0,
        "缺少第二天价格个数": 0,
    }
    for record in records:
        flag = record.get("第二天涨跌")
        if flag is None:
            summary["缺少第二天价格个数"] += 1
            continue
        summary["可比较个数"] += 1
        if flag == "上涨":
            summary["第二天上涨个数"] += 1
        elif flag == "下跌":
            summary["第二天下跌个数"] += 1
        elif flag == "不变":
            summary["第二天不变个数"] += 1
    return summary


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
    selected_dates = select_common_dates_until(
        flow_df,
        price_df,
        args.days,
        args.date,
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

    next_day_prices = None
    next_date = None
    if args.date:
        next_date = next_price_date(price_df, selected_dates[-1])
        next_day_prices = build_next_day_price_map(price_df, selected_dates[-1], next_date)

    records = [
        build_rank_record(row, selected_dates, days, next_day_prices)
        for _, row in ranked_df.iterrows()
    ]
    limited_records = limit_records(records, limit)
    result = {
        "分析参数": {
            "最近天数": days,
            "截止日期": selected_dates[-1],
            "返回数量": limit,
        },
        "结果": limited_records,
    }
    if args.date:
        result["分析参数"]["第二天日期"] = next_date
        result["分析参数"]["第二天表现统计"] = build_next_day_summary(limited_records)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
