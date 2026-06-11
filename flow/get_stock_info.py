#!/usr/bin/env python3
"""Analyze one stock over the latest daily files under data/today.

Examples:
  python flow/get_stock_info.py --code 603233 --days 3
  python flow/get_stock_info.py --code 大参林 --days 5
  python flow/get_stock_info.py --code 603233
  python flow/get_stock_info.py --code 603233 --output data/dcl.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from analyze_flow_price import DEFAULT_TURNOVER_PATH, date_columns, read_matrix
from symbol_search import build_suggestion_message, search_symbol_records, symbol_records_from_rows

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
            "输出指定股票的区间净流入、价格、换手率和均线 JSON。"
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
        help="读取最近多少个交易日文件；不传则使用当前可用的最大天数。",
    )
    parser.add_argument(
        "--input-dir",
        default=str(TODAY_DATA_DIR),
        help=f"输入目录，默认 {display_path(TODAY_DATA_DIR)}。",
    )
    parser.add_argument(
        "--turnover-input",
        default=str(DEFAULT_TURNOVER_PATH),
        help=f"换手率矩阵 CSV，默认 {display_path(DEFAULT_TURNOVER_PATH)}。",
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


def format_amount(value: float | None) -> str | None:
    if value is None:
        return None
    return f"{value:.2f}"


def display_amount(value: float | None) -> str:
    return format_amount(value) or "--"


def read_all_snapshots(input_dir: Path) -> tuple[list[str], pd.DataFrame]:
    files = list_daily_files(input_dir)
    snapshots = [read_daily_snapshot(file_path) for file_path in files]
    valid_snapshots = [snapshot for snapshot in snapshots if not snapshot.empty]
    if not valid_snapshots:
        raise SystemExit("没有读取到任何有效股票记录。")
    all_df = pd.concat(valid_snapshots, ignore_index=True)
    all_df["名称"] = all_df["名称"].fillna("").astype(str).str.strip()
    return [file_path.stem for file_path in files], all_df


def build_turnover_map(
    turnover_df: pd.DataFrame,
    stock_code: str,
    selected_dates: list[str],
) -> dict[str, float | None]:
    matched = turnover_df[turnover_df["代码"].map(normalize_code) == stock_code].copy()
    if matched.empty:
        return {date_tag: None for date_tag in selected_dates}

    row = matched.sort_values("代码", kind="stable").iloc[-1]
    return {
        date_tag: round_or_none(row.get(date_tag)) if date_tag in turnover_df.columns else None
        for date_tag in selected_dates
    }


def resolve_unique_symbol_match(
    all_df: pd.DataFrame,
    token: str,
    symbol_records: list[dict[str, str]],
) -> tuple[str | None, str, list[dict[str, Any]]]:
    matches = search_symbol_records(symbol_records, token, top=8)
    if len(matches) != 1:
        return None, "", matches

    code = normalize_code(matches[0].get("代码", ""))
    if not code:
        return None, "", matches

    matched = all_df[all_df["代码"].map(normalize_code) == code].copy()
    if matched.empty:
        return None, "", matches

    matched = matched.sort_values("日期", kind="stable")
    latest = matched.iloc[-1]
    name = str(latest.get("名称", "")).strip()
    if not name:
        name = str(matches[0].get("名称", "")).strip()
    return code, name, matches


def resolve_stock_identity(all_df: pd.DataFrame, token: str) -> tuple[str, str]:
    symbol_records = symbol_records_from_rows(
        all_df[["代码", "名称"]].fillna("").astype(str).to_dict(orient="records")
    )
    normalized_input_code = normalize_code(token)
    if normalized_input_code:
        matched = all_df[all_df["代码"].map(normalize_code) == normalized_input_code].copy()
        if matched.empty:
            resolved_code, resolved_name, suggestions = resolve_unique_symbol_match(
                all_df, token, symbol_records
            )
            if resolved_code:
                print(
                    f'提示：未精确命中 "{token}"，已自动使用唯一候选 '
                    f"{resolved_code} {resolved_name}。",
                    file=sys.stderr,
                )
                return resolved_code, resolved_name
            raise SystemExit(
                build_suggestion_message(
                    token,
                    suggestions,
                    not_found_prefix="today 目录数据中未找到股票：",
                    include_reason=True,
                )
            )
        matched = matched.sort_values("日期", kind="stable")
        latest = matched.iloc[-1]
        return normalized_input_code, str(latest.get("名称", "")).strip()

    normalized_input_name = normalize_name(token)
    matched = all_df[all_df["名称"].map(normalize_name) == normalized_input_name].copy()
    if matched.empty:
        resolved_code, resolved_name, suggestions = resolve_unique_symbol_match(
            all_df, token, symbol_records
        )
        if resolved_code:
            print(
                f'提示：未精确命中 "{token}"，已自动使用唯一候选 '
                f"{resolved_code} {resolved_name}。",
                file=sys.stderr,
            )
            return resolved_code, resolved_name
        raise SystemExit(
            build_suggestion_message(
                token,
                suggestions,
                not_found_prefix="today 目录数据中未找到股票：",
                include_reason=True,
            )
        )

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


def build_price_by_date(
    stock_history_df: pd.DataFrame,
    all_dates: list[str],
) -> dict[str, float | None]:
    date_order = {date_tag: index for index, date_tag in enumerate(all_dates)}
    history_df = stock_history_df.copy()
    history_df["日期"] = history_df["日期"].astype(str)
    history_df = history_df[history_df["日期"].isin(all_dates)].copy()
    if history_df.empty:
        return {}

    history_df["_date_order"] = history_df["日期"].map(date_order)
    history_df = (
        history_df.sort_values("_date_order", kind="stable")
        .drop_duplicates(subset=["日期"], keep="last")
    )
    return {
        str(row["日期"]): round_or_none(row.get("最新价"))
        for _, row in history_df.iterrows()
    }


def build_moving_average_map_for_date(
    price_by_date: dict[str, float | None],
    all_dates: list[str],
    end_date: str,
    windows: tuple[int, ...] = (5, 10, 20),
) -> dict[str, float | None]:
    date_order = {date_tag: index for index, date_tag in enumerate(all_dates)}
    if end_date not in date_order:
        return {f"{window}日均线": None for window in windows}

    ordered_dates = all_dates[: date_order[end_date] + 1]
    result: dict[str, float | None] = {}
    for window in windows:
        field_name = f"{window}日均线"
        if len(ordered_dates) < window:
            result[field_name] = None
            continue
        window_dates = ordered_dates[-window:]
        window_prices = [price_by_date.get(date_tag) for date_tag in window_dates]
        if any(price is None for price in window_prices):
            result[field_name] = None
            continue
        result[field_name] = round(sum(window_prices) / window, 2)
    return result


def build_daily_moving_average_map(
    price_by_date: dict[str, float | None],
    all_dates: list[str],
    target_dates: list[str],
    windows: tuple[int, ...] = (5, 10, 20),
) -> dict[str, dict[str, float | None]]:
    return {
        date_tag: build_moving_average_map_for_date(price_by_date, all_dates, date_tag, windows)
        for date_tag in target_dates
    }


def build_result(
    stock_code: str,
    stock_name: str,
    stock_df: pd.DataFrame,
    turnover_map: dict[str, float | None],
    selected_dates: list[str],
    days: int,
    moving_average_map: dict[str, float | None],
    daily_moving_average_map: dict[str, dict[str, float | None]],
) -> dict[str, Any]:
    day_map = build_daily_lookup(stock_df)
    flow_map: dict[str, str] = {}
    price_map: dict[str, float | None] = {}
    flow_values: list[float] = []
    flow_value_strings: dict[str, str] = {}
    price_strings: dict[str, str] = {}
    turnover_strings: dict[str, str] = {}
    ma5_strings: dict[str, str] = {}
    ma10_strings: dict[str, str] = {}
    ma20_strings: dict[str, str] = {}
    running_total_map: dict[str, float | None] = {}
    days_label = f"最近{days}天"
    running_inflow_wan = 0.0
    has_running_inflow = False
    positive_days = 0

    for date_tag in selected_dates:
        row = day_map.get(date_tag)
        flow_value = round_or_none(None if row is None else row.get("主力净流入(万)"))
        price_value = round_or_none(None if row is None else row.get("最新价"))
        turnover_value = turnover_map.get(date_tag)
        price_map[date_tag] = price_value
        if flow_value is not None:
            flow_values.append(flow_value)
            running_inflow_wan = round(running_inflow_wan + flow_value, 2)
            has_running_inflow = True
            if flow_value > 0:
                positive_days += 1
        flow_value_strings[date_tag] = display_amount(flow_value)
        price_strings[date_tag] = display_amount(price_value)
        turnover_strings[date_tag] = display_amount(turnover_value)
        moving_average_for_day = daily_moving_average_map.get(date_tag, {})
        ma5_strings[date_tag] = display_amount(moving_average_for_day.get("5日均线"))
        ma10_strings[date_tag] = display_amount(moving_average_for_day.get("10日均线"))
        ma20_strings[date_tag] = display_amount(moving_average_for_day.get("20日均线"))
        running_total_map[date_tag] = round(running_inflow_wan, 2) if has_running_inflow else None

    flow_value_width = max((len(value) for value in flow_value_strings.values()), default=0)
    price_width = max((len(value) for value in price_strings.values()), default=0)
    turnover_width = max((len(value) for value in turnover_strings.values()), default=0)
    ma5_width = max((len(value) for value in ma5_strings.values()), default=0)
    ma10_width = max((len(value) for value in ma10_strings.values()), default=0)
    ma20_width = max((len(value) for value in ma20_strings.values()), default=0)
    current_total_width = max((len(display_amount(value)) for value in running_total_map.values()), default=0)
    gap_width = 4
    for date_tag in selected_dates:
        flow_text = flow_value_strings[date_tag]
        price_text = price_strings[date_tag]
        ma5_text = ma5_strings[date_tag]
        ma10_text = ma10_strings[date_tag]
        ma20_text = ma20_strings[date_tag]
        current_total_text = display_amount(running_total_map[date_tag])
        turnover_text = turnover_strings[date_tag]
        flow_map[date_tag] = (
            f"{flow_text:<{flow_value_width + gap_width}}"
            f"价格: {price_text:<{price_width + gap_width}}"
            f"当前余额: {current_total_text:<{current_total_width + gap_width}}"
            f"换手率: {turnover_text:<{turnover_width + gap_width}}"
            f"5日线: {ma5_text:<{ma5_width + gap_width}}"
            f"10日线: {ma10_text:<{ma10_width + gap_width}}"
            f"20日线: {ma20_text}"
        )

    start_price = price_map[selected_dates[0]]
    latest_price = price_map[selected_dates[-1]]
    price_change_pct = None
    if start_price not in (None, 0) and latest_price is not None:
        price_change_pct = round(latest_price / start_price * 100 - 100, 2)

    total_inflow_wan = round(sum(flow_values), 2) if flow_values else None
    result = {
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
    }
    result.update(
        {
            field_name: value
            for field_name, value in moving_average_map.items()
            if value is not None
        }
    )
    return result


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
    if args.days is not None and args.days <= 0:
        raise SystemExit("--days 必须大于 0")

    input_dir = resolve_project_path(args.input_dir)
    turnover_input_path = resolve_project_path(args.turnover_input)
    all_dates, all_df = read_all_snapshots(input_dir)
    effective_days = len(all_dates) if args.days is None else args.days
    if effective_days > len(all_dates):
        raise SystemExit(
            f"--days={effective_days} 超过可用交易日文件数 {len(all_dates)}。"
            f"当前可用日期：{'、'.join(all_dates)}"
        )

    selected_dates = all_dates[-effective_days:]
    turnover_df = read_matrix(turnover_input_path)
    stock_code, stock_name = resolve_stock_identity(all_df, args.code)
    stock_history_df = all_df[all_df["代码"].map(normalize_code) == stock_code].copy()
    stock_df = all_df[
        (all_df["代码"].map(normalize_code) == stock_code)
        & (all_df["日期"].isin(selected_dates))
    ].copy()
    if stock_df.empty:
        raise SystemExit(
            f"最近 {effective_days} 个交易日文件中未找到股票 {stock_name or stock_code} 的记录。"
        )

    turnover_map = build_turnover_map(turnover_df, stock_code, selected_dates)
    price_by_date = build_price_by_date(
        stock_history_df,
        all_dates,
    )
    moving_average_map = build_moving_average_map_for_date(price_by_date, all_dates, selected_dates[-1])
    daily_moving_average_map = build_daily_moving_average_map(price_by_date, all_dates, selected_dates)
    result = build_result(
        stock_code,
        stock_name,
        stock_df,
        turnover_map,
        selected_dates,
        effective_days,
        moving_average_map,
        daily_moving_average_map,
    )
    output_path = resolve_project_path(args.output) if args.output else None
    write_output(result, output_path)


if __name__ == "__main__":
    main()
