#!/usr/bin/env python3
"""Filter stocks by latest local price and print JSON.

Examples:
  python flow/filter_latest_price.py 10
  python flow/filter_latest_price.py 10 --tolerance 0.3
  python flow/filter_latest_price.py 10 --price-input data/price.csv --output data/price_10.json
"""

from __future__ import annotations

import argparse
import json
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


DEFAULT_TOLERANCE = 0.5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "读取本地 data/price.csv，"
            "按给定价格和偏差筛选本地最新价落在区间内的股票，并输出 JSON。"
        )
    )
    parser.add_argument(
        "price",
        type=float,
        help="目标价格，例如 10 或 10.5。",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=DEFAULT_TOLERANCE,
        help=f"允许偏差，单位元，默认 {DEFAULT_TOLERANCE:g}。",
    )
    parser.add_argument(
        "--price-input",
        default=str(DEFAULT_PRICE_PATH),
        help=f"价格矩阵 CSV，默认 {display_path(DEFAULT_PRICE_PATH)}。",
    )
    parser.add_argument("--output", help="输出 JSON 文件；不传则打印到终端。")
    return parser.parse_args()


def build_latest_price_df(price_df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    price_tags = date_columns(price_df)
    if not price_tags:
        raise SystemExit("price.csv 中没有可用的日期列。")

    long_df = price_df[["代码", "名称", *price_tags]].melt(
        id_vars=["代码", "名称"],
        value_vars=price_tags,
        var_name="最新价格日期",
        value_name="最新价格",
    )
    long_df = long_df.dropna(subset=["最新价格"]).copy()
    if long_df.empty:
        raise SystemExit("price.csv 中没有任何有效价格。")

    date_order = {date_tag: index for index, date_tag in enumerate(price_tags)}
    long_df["日期序号"] = long_df["最新价格日期"].map(date_order)
    long_df = long_df.sort_values(["代码", "日期序号"], kind="stable")
    latest_df = long_df.drop_duplicates(subset=["代码"], keep="last").copy()
    latest_df = latest_df.drop(columns=["日期序号"]).reset_index(drop=True)
    latest_df["名称"] = latest_df["名称"].fillna("").astype(str).str.strip()
    latest_df["最新价格"] = pd.to_numeric(latest_df["最新价格"], errors="coerce")
    latest_df = latest_df.dropna(subset=["最新价格"]).copy()
    return latest_df, price_tags


def build_result(
    latest_df: pd.DataFrame,
    *,
    target_price: float,
    tolerance: float,
    price_input: Path,
    latest_column: str,
) -> dict[str, Any]:
    min_price = target_price - tolerance
    max_price = target_price + tolerance

    matched_df = latest_df[
        latest_df["最新价格"].between(min_price, max_price, inclusive="both")
    ].copy()
    matched_df["价差"] = (matched_df["最新价格"] - target_price).abs()
    matched_df = matched_df.sort_values(["价差", "最新价格", "代码"], kind="stable")

    records = [
        {
            "代码": row["代码"],
            "名称": row["名称"],
            "最新价格": round_or_none(row["最新价格"]),
            "最新价格日期": row["最新价格日期"],
            "价差": round_or_none(row["价差"]),
        }
        for _, row in matched_df.iterrows()
    ]

    return {
        "查询价格": round_or_none(target_price),
        "允许偏差": round_or_none(tolerance),
        "价格区间": {
            "最小值": round_or_none(min_price),
            "最大值": round_or_none(max_price),
        },
        "价格文件": display_path(price_input),
        "价格矩阵最新列": latest_column,
        "有效样本数": int(len(latest_df)),
        "匹配数量": int(len(records)),
        "股票": records,
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
    if args.price < 0:
        raise SystemExit("目标价格不能小于 0")
    if args.tolerance < 0:
        raise SystemExit("--tolerance 不能小于 0")

    price_input = resolve_project_path(args.price_input)
    output_path = None if not args.output else resolve_project_path(args.output)

    price_df = read_matrix(price_input)
    latest_df, price_tags = build_latest_price_df(price_df)
    result = build_result(
        latest_df,
        target_price=args.price,
        tolerance=args.tolerance,
        price_input=price_input,
        latest_column=price_tags[-1],
    )
    write_output(result, output_path)


if __name__ == "__main__":
    main()
