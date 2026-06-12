#!/usr/bin/env python3
"""Find stocks whose MA5 crosses above MA20 on a given trading day.

Examples:
  python flow/find_golden_cross.py --date 0611
  python flow/find_golden_cross.py --date 2026-06-11
  python flow/find_golden_cross.py --date 20260611 --output data/golden_cross_0611.json
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from analyze_flow_price import (
    DEFAULT_PRICE_PATH,
    date_columns,
    display_path,
    read_matrix,
    resolve_project_path,
    round_or_none,
)


SHORT_WINDOW = 5
LONG_WINDOW = 20


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "读取本地 data/price.csv，"
            "筛出指定交易日出现 5 日线上穿 20 日线黄金交叉的股票，并输出 JSON。"
        )
    )
    parser.add_argument(
        "--date",
        required=True,
        help="目标日期，支持 MMDD、YYYYMMDD、YYYY-MM-DD，例如 0611。",
    )
    parser.add_argument(
        "--price-input",
        default=str(DEFAULT_PRICE_PATH),
        help=f"价格矩阵 CSV，默认 {display_path(DEFAULT_PRICE_PATH)}。",
    )
    parser.add_argument("--output", help="输出 JSON 文件；不传则打印到终端。")
    return parser.parse_args()


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
    raise SystemExit("--date 必须是 MMDD、YYYYMMDD 或 YYYY-MM-DD，例如 0611 或 2026-06-11")


def calculate_moving_average(row: pd.Series, price_tags: list[str], end_index: int, window: int) -> float | None:
    start_index = end_index - window + 1
    if start_index < 0:
        return None
    values = [row[price_tags[index]] for index in range(start_index, end_index + 1)]
    if any(pd.isna(value) for value in values):
        return None
    return round(sum(float(value) for value in values) / window, 2)


def build_records(price_df: pd.DataFrame, target_date: str) -> tuple[str, list[dict[str, Any]], dict[str, int]]:
    price_tags = date_columns(price_df)
    if target_date not in price_tags:
        raise SystemExit(
            f"--date={target_date} 不在价格矩阵日期列中。"
            f"当前可用日期：{'、'.join(price_tags)}"
        )

    target_index = price_tags.index(target_date)
    if target_index == 0:
        raise SystemExit(f"--date={target_date} 前面没有上一个交易日，无法判断黄金交叉。")
    if target_index < LONG_WINDOW:
        raise SystemExit(
            f"--date={target_date} 之前历史不足，无法同时比较前一日和当日的 {SHORT_WINDOW}/{LONG_WINDOW} 日线。"
            f"最早可分析日期：{price_tags[LONG_WINDOW]}"
        )

    previous_date = price_tags[target_index - 1]
    records: list[dict[str, Any]] = []
    eligible_count = 0

    for _, row in price_df.iterrows():
        prev_ma_short = calculate_moving_average(row, price_tags, target_index - 1, SHORT_WINDOW)
        prev_ma_long = calculate_moving_average(row, price_tags, target_index - 1, LONG_WINDOW)
        curr_ma_short = calculate_moving_average(row, price_tags, target_index, SHORT_WINDOW)
        curr_ma_long = calculate_moving_average(row, price_tags, target_index, LONG_WINDOW)

        if None in (prev_ma_short, prev_ma_long, curr_ma_short, curr_ma_long):
            continue

        eligible_count += 1
        if prev_ma_short > prev_ma_long or curr_ma_short <= curr_ma_long:
            continue

        prev_diff = round(prev_ma_short - prev_ma_long, 2)
        curr_diff = round(curr_ma_short - curr_ma_long, 2)
        records.append(
            {
                "代码": row["代码"],
                "名称": row["名称"],
                "前一交易日": previous_date,
                "目标日期": target_date,
                "目标日价格": round_or_none(row[target_date]),
                "前一日5日均线": prev_ma_short,
                "前一日20日均线": prev_ma_long,
                "当日5日均线": curr_ma_short,
                "当日20日均线": curr_ma_long,
                "前一日均线差": prev_diff,
                "当日均线差": curr_diff,
            }
        )

    records.sort(
        key=lambda item: (
            -(item["当日均线差"] or 0),
            item["前一日均线差"] or 0,
            item["代码"],
        )
    )
    stats = {
        "总股票数": int(len(price_df)),
        "可比较样本数": int(eligible_count),
        "历史不足或缺价格样本数": int(len(price_df) - eligible_count),
        "黄金交叉数量": int(len(records)),
    }
    return previous_date, records, stats


def build_result(
    *,
    price_input: Path,
    target_date: str,
    previous_date: str,
    stats: dict[str, int],
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "分析参数": {
            "目标日期": target_date,
            "前一交易日": previous_date,
            "价格文件": display_path(price_input),
            "黄金交叉定义": "前一交易日5日均线<=20日均线，且目标日期5日均线>20日均线",
        },
        "样本信息": stats,
        "结果": records,
    }


def write_output(result: dict[str, Any], output_path: Path | None) -> None:
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    if output_path is None:
        print(payload)
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(payload, encoding="utf-8")


def main() -> None:
    args = parse_args()
    price_input = resolve_project_path(args.price_input)
    output_path = resolve_project_path(args.output) if args.output else None
    target_date = normalize_date_tag(args.date)

    price_df = read_matrix(price_input)
    previous_date, records, stats = build_records(price_df, target_date)
    result = build_result(
        price_input=price_input,
        target_date=target_date,
        previous_date=previous_date,
        stats=stats,
        records=records,
    )
    write_output(result, output_path)


if __name__ == "__main__":
    main()
