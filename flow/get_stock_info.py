#!/usr/bin/env python3
"""Analyze one stock over the latest daily files under data/today.

Examples:
  python flow/get_stock_info.py --code 603233 --days 3
  python flow/get_stock_info.py --code 大参林 --days 5
  python flow/get_stock_info.py --code 603233 --days 3 --output data/dcl.json
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from build_today_flow_matrix import (
    TODAY_DATA_DIR,
    display_path,
    list_daily_files,
    read_daily_snapshot,
    resolve_project_path,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "读取 data/today/ 下最近 days 个交易日文件，"
            "输出指定股票的区间净流入和价格 JSON。"
        )
    )
    parser.add_argument(
        "--code",
        required=True,
        help="股票代码或名称，例如 603233、sh603233、大参林。",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=3,
        help="读取最近多少个交易日文件，默认 3。",
    )
    parser.add_argument(
        "--input-dir",
        default=str(TODAY_DATA_DIR),
        help=f"输入目录，默认 {display_path(TODAY_DATA_DIR)}。",
    )
    parser.add_argument("--output", help="输出 JSON 文件；不传则打印到终端。")
    return parser.parse_args()


def normalize_code(value: object) -> str:
    code = str(value).strip().lower()
    for prefix in ("sh", "sz", "bj"):
        if code.startswith(prefix):
            code = code[len(prefix) :]
    digits = "".join(ch for ch in code if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def normalize_name(value: object) -> str:
    return re.sub(r"\s+", "", str(value).strip()).lower()


def round_or_none(value: object, digits: int = 2) -> float | None:
    if value is None:
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(converted):
        return None
    return round(converted, digits)


def read_all_snapshots(input_dir: Path) -> tuple[list[str], pd.DataFrame]:
    files = list_daily_files(input_dir)
    snapshots = [read_daily_snapshot(file_path) for file_path in files]
    valid_snapshots = [snapshot for snapshot in snapshots if not snapshot.empty]
    if not valid_snapshots:
        raise SystemExit("没有读取到任何有效股票记录。")
    all_df = pd.concat(valid_snapshots, ignore_index=True)
    all_df["名称"] = all_df["名称"].fillna("").astype(str).str.strip()
    return [file_path.stem for file_path in files], all_df


def resolve_stock_identity(all_df: pd.DataFrame, token: str) -> tuple[str, str]:
    normalized_input_code = normalize_code(token)
    if normalized_input_code:
        matched = all_df[all_df["代码"].map(normalize_code) == normalized_input_code].copy()
        if matched.empty:
            raise SystemExit(f"today 目录数据中未找到股票代码：{normalized_input_code}")
        matched = matched.sort_values("日期", kind="stable")
        latest = matched.iloc[-1]
        return normalized_input_code, str(latest.get("名称", "")).strip()

    normalized_input_name = normalize_name(token)
    matched = all_df[all_df["名称"].map(normalize_name) == normalized_input_name].copy()
    if matched.empty:
        raise SystemExit(f"today 目录数据中未找到股票名称：{token}")

    candidates = (
        matched.sort_values(["日期", "代码"], kind="stable")
        .drop_duplicates(subset=["代码"], keep="last")[["代码", "名称"]]
    )
    if len(candidates) > 1:
        candidate_text = "；".join(
            f"{row.名称}({row.代码})" for row in candidates.itertuples(index=False)
        )
        raise SystemExit(f"股票名称匹配到多个结果，请改用代码：{candidate_text}")

    latest = candidates.iloc[0]
    return normalize_code(latest["代码"]), str(latest["名称"]).strip()


def build_daily_lookup(stock_df: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        str(row["日期"]): row
        for _, row in stock_df.sort_values("日期", kind="stable").iterrows()
    }


def build_result(
    stock_code: str,
    stock_name: str,
    stock_df: pd.DataFrame,
    selected_dates: list[str],
    days: int,
) -> dict[str, Any]:
    day_map = build_daily_lookup(stock_df)
    flow_map: dict[str, float | None] = {}
    price_map: dict[str, float | None] = {}
    flow_values: list[float] = []
    days_label = f"最近{days}天"

    for date_tag in selected_dates:
        row = day_map.get(date_tag)
        flow_value = round_or_none(None if row is None else row.get("主力净流入(万)"))
        price_value = round_or_none(None if row is None else row.get("最新价"))
        flow_map[date_tag] = flow_value
        price_map[date_tag] = price_value
        if flow_value is not None:
            flow_values.append(flow_value)

    start_price = price_map[selected_dates[0]]
    latest_price = price_map[selected_dates[-1]]
    price_change_pct = None
    if start_price not in (None, 0) and latest_price is not None:
        price_change_pct = round(latest_price / start_price * 100 - 100, 2)

    total_inflow_wan = round(sum(flow_values), 2) if flow_values else None
    positive_days = sum(
        1 for date_tag in selected_dates if flow_map.get(date_tag) is not None and flow_map[date_tag] > 0
    )

    return {
        "代码": stock_code,
        "名称": stock_name,
        "分析区间": f"{selected_dates[0]}-{selected_dates[-1]}",
        "区间起始价格": start_price,
        "最新价格": latest_price,
        "区间涨跌幅%": price_change_pct,
        f"{days_label}主力净流入合计(万)": total_inflow_wan,
        f"{days_label}主力净流入合计(亿)": round(total_inflow_wan / 10000, 2) if total_inflow_wan is not None else None,
        f"{days_label}净流入为正天数": positive_days,
        f"{days_label}每日净流入(万)": flow_map,
        f"{days_label}每日价格": price_map,
    }


def write_output(result: dict[str, Any], output_path: Path | None) -> None:
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    if output_path is None:
        print(payload)
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(payload, encoding="utf-8")
    print(f"已写入 {display_path(output_path)}")


def main() -> None:
    args = parse_args()
    if args.days <= 0:
        raise SystemExit("--days 必须大于 0")

    input_dir = resolve_project_path(args.input_dir)
    all_dates, all_df = read_all_snapshots(input_dir)
    if args.days > len(all_dates):
        raise SystemExit(
            f"--days={args.days} 超过可用交易日文件数 {len(all_dates)}。"
            f"当前可用日期：{'、'.join(all_dates)}"
        )

    selected_dates = all_dates[-args.days :]
    stock_code, stock_name = resolve_stock_identity(all_df, args.code)
    stock_df = all_df[
        (all_df["代码"].map(normalize_code) == stock_code)
        & (all_df["日期"].isin(selected_dates))
    ].copy()
    if stock_df.empty:
        raise SystemExit(
            f"最近 {args.days} 个交易日文件中未找到股票 {stock_name or stock_code} 的记录。"
        )

    result = build_result(stock_code, stock_name, stock_df, selected_dates, args.days)
    output_path = resolve_project_path(args.output) if args.output else None
    write_output(result, output_path)


if __name__ == "__main__":
    main()
