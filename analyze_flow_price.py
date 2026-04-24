#!/usr/bin/env python3
"""Analyze flow.csv and price.csv, then print JSON to stdout.

Examples:
  python analyze_flow_price.py
  python analyze_flow_price.py --days 3 --money 2
  python analyze_flow_price.py --days 5 --money 1.5 --max-change 8 --top 20
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
DEFAULT_FLOW_PATH = DATA_DIR / "flow.csv"
DEFAULT_PRICE_PATH = DATA_DIR / "price.csv"
DATE_TAG_PATTERN = re.compile(r"^\d{4}$")
DEFAULT_DAYS = 1
DEFAULT_THRESHOLD_YI = 1.0


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_DIR))
    except ValueError:
        return str(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "分析 data/flow.csv 和 data/price.csv，"
            "输出最近 N 天主力净流入和价格变化的 JSON 结果。"
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
        default=DEFAULT_DAYS,
        help=f"分析最近多少天，默认 {DEFAULT_DAYS}。",
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
        help="每类结果最多返回多少只股票；不传则返回全部。",
    )
    parser.add_argument(
        "--max-change",
        type=float,
        help=(
            "区间涨幅上限，单位百分比。"
            "例如 5 表示只保留最近 N 天主力净流入合计超过阈值，"
            "且区间涨跌幅不超过 5%% 的股票。"
        ),
    )
    return parser.parse_args()


def normalize_code(value: object) -> str:
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def require_columns(df: pd.DataFrame, path: Path) -> None:
    missing = [column for column in ["代码", "名称"] if column not in df.columns]
    if missing:
        raise SystemExit(f"文件缺少字段 {'、'.join(missing)}：{display_path(path)}")


def date_columns(df: pd.DataFrame) -> list[str]:
    return [str(column) for column in df.columns if DATE_TAG_PATTERN.fullmatch(str(column))]


def read_matrix(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"输入文件不存在：{path}")
    try:
        df = pd.read_csv(path, dtype={"代码": str})
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"读取 CSV 失败 {display_path(path)}：{exc}") from exc

    require_columns(df, path)
    result = df.copy()
    result["代码"] = result["代码"].map(normalize_code)
    result["名称"] = result["名称"].fillna("").astype(str).str.strip()
    result = result[result["代码"] != ""].copy()

    columns = date_columns(result)
    if not columns:
        raise SystemExit(f"文件中没有找到 MMDD 日期列：{display_path(path)}")

    for column in columns:
        result[column] = pd.to_numeric(result[column], errors="coerce")

    result = result.drop_duplicates(subset=["代码"], keep="last")
    return result


def select_dates(flow_df: pd.DataFrame, price_df: pd.DataFrame, days: int) -> list[str]:
    flow_dates = date_columns(flow_df)
    price_dates = set(date_columns(price_df))
    common_dates = [column for column in flow_dates if column in price_dates]
    if not common_dates:
        raise SystemExit("flow.csv 和 price.csv 没有可共同分析的日期列。")
    if days > len(common_dates):
        raise SystemExit(
            f"--days={days} 超过可用日期数 {len(common_dates)}。"
            f"当前可用日期：{'、'.join(common_dates)}"
        )
    return common_dates[-days:]


def build_merged_window(
    flow_df: pd.DataFrame,
    price_df: pd.DataFrame,
    selected_dates: list[str],
) -> pd.DataFrame:
    flow_columns = ["代码", "名称", *selected_dates]
    price_columns = ["代码", "名称", *selected_dates]
    flow_window = flow_df[flow_columns].copy()
    price_window = price_df[price_columns].copy()

    merged_df = flow_window.merge(
        price_window,
        on="代码",
        how="inner",
        suffixes=("_flow", "_price"),
    )
    if merged_df.empty:
        raise SystemExit("flow.csv 和 price.csv 没有可共同分析的股票。")

    merged_df["名称"] = merged_df["名称_flow"].where(
        merged_df["名称_flow"].astype(str).str.strip() != "",
        merged_df["名称_price"],
    )
    merged_df = merged_df.drop(columns=["名称_flow", "名称_price"])

    for date_tag in selected_dates:
        merged_df[f"{date_tag}_flow"] = pd.to_numeric(merged_df[f"{date_tag}_flow"], errors="coerce")
        merged_df[f"{date_tag}_price"] = pd.to_numeric(merged_df[f"{date_tag}_price"], errors="coerce")

    return merged_df


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


def build_daily_map(row: pd.Series, selected_dates: list[str], suffix: str) -> dict[str, float | None]:
    return {
        date_tag: round_or_none(row.get(f"{date_tag}_{suffix}"))
        for date_tag in selected_dates
    }


def build_record(row: pd.Series, selected_dates: list[str]) -> dict[str, Any]:
    start_date = selected_dates[0]
    end_date = selected_dates[-1]
    start_price = row.get(f"{start_date}_price")
    end_price = row.get(f"{end_date}_price")
    net_inflow_wan = row.get("最近N天主力净流入合计(万)")
    positive_days = row.get("最近N天净流入为正天数")

    price_change_pct = round_or_none(row.get("区间涨跌幅%"))
    if price_change_pct is None:
        try:
            if start_price is not None and end_price is not None and not pd.isna(start_price) and start_price != 0:
                price_change_pct = round(float(end_price) / float(start_price) * 100 - 100, 2)
        except (TypeError, ValueError, ZeroDivisionError):
            price_change_pct = None

    return {
        "代码": row.get("代码"),
        "名称": row.get("名称"),
        "分析区间": f"{start_date}-{end_date}",
        "区间起始价格": round_or_none(start_price),
        "最新价格": round_or_none(end_price),
        "区间涨跌幅%": price_change_pct,
        "最近N天主力净流入合计(万)": round_or_none(net_inflow_wan),
        "最近N天主力净流入合计(亿)": round_or_none(
            None if net_inflow_wan is None or pd.isna(net_inflow_wan) else float(net_inflow_wan) / 10000
        ),
        "最近N天净流入为正天数": int(positive_days) if pd.notna(positive_days) else None,
        "最近N天每日净流入(万)": build_daily_map(row, selected_dates, "flow"),
        "最近N天每日价格": build_daily_map(row, selected_dates, "price"),
    }


def limit_records(records: list[dict[str, Any]], top: int | None) -> list[dict[str, Any]]:
    if top is None or top <= 0:
        return records
    return records[:top]


def build_working_df(merged_df: pd.DataFrame, selected_dates: list[str]) -> pd.DataFrame:
    flow_columns = [f"{date_tag}_flow" for date_tag in selected_dates]
    price_columns = [f"{date_tag}_price" for date_tag in selected_dates]
    full_window_mask = merged_df[flow_columns + price_columns].notna().all(axis=1)
    working_df = merged_df[full_window_mask].copy()

    if working_df.empty:
        raise SystemExit("最近 N 天没有同时具备完整流向和价格数据的股票。")

    working_df["最近N天主力净流入合计(万)"] = working_df[flow_columns].sum(axis=1)
    working_df["最近N天净流入为正天数"] = working_df[flow_columns].gt(0).sum(axis=1)
    start_date = selected_dates[0]
    end_date = selected_dates[-1]
    start_prices = working_df[f"{start_date}_price"]
    end_prices = working_df[f"{end_date}_price"]
    working_df["区间涨跌幅%"] = ((end_prices / start_prices) * 100 - 100).round(2)
    working_df.loc[start_prices == 0, "区间涨跌幅%"] = pd.NA
    return working_df


def analyze(merged_df: pd.DataFrame, selected_dates: list[str], threshold_yi: float, top: int | None) -> dict[str, Any]:
    working_df = build_working_df(merged_df, selected_dates)
    flow_columns = [f"{date_tag}_flow" for date_tag in selected_dates]

    threshold_wan = threshold_yi * 10000
    threshold_df = working_df[working_df["最近N天主力净流入合计(万)"] > threshold_wan].copy()
    threshold_df = threshold_df.sort_values("最近N天主力净流入合计(万)", ascending=False)

    continuous_df = working_df[working_df[flow_columns].gt(0).all(axis=1)].copy()
    continuous_df = continuous_df.sort_values("最近N天主力净流入合计(万)", ascending=False)

    threshold_records = [build_record(row, selected_dates) for _, row in threshold_df.iterrows()]
    continuous_records = [build_record(row, selected_dates) for _, row in continuous_df.iterrows()]

    return {
        "分析参数": {
            "最近天数": len(selected_dates),
            "日期范围": selected_dates,
            "主力净流入阈值(亿)": threshold_yi,
        },
        "筛选说明": {
            "条件1": "独立筛选：最近N天主力净流入合计超过阈值",
            "条件2": "独立筛选：最近N天每天主力净流入都大于0",
        },
        "样本信息": {
            "完整样本数": int(len(working_df)),
            "主力净流入合计超过阈值个数": int(len(threshold_df)),
            "最近N天每天净流入都大于0个数": int(len(continuous_df)),
        },
        "最近N天主力净流入合计超过阈值": limit_records(threshold_records, top),
        "最近N天每天主力净流入都大于0": limit_records(continuous_records, top),
    }


def analyze_with_max_change(
    merged_df: pd.DataFrame,
    selected_dates: list[str],
    threshold_yi: float,
    max_change: float,
    top: int | None,
) -> dict[str, Any]:
    base_result = analyze(
        merged_df=merged_df,
        selected_dates=selected_dates,
        threshold_yi=threshold_yi,
        top=top,
    )
    working_df = build_working_df(merged_df, selected_dates)

    threshold_wan = threshold_yi * 10000
    max_change_df = working_df[
        (working_df["最近N天主力净流入合计(万)"] > threshold_wan)
        & working_df["区间涨跌幅%"].notna()
        & (working_df["区间涨跌幅%"] <= max_change)
    ].copy()
    max_change_df = max_change_df.sort_values("最近N天主力净流入合计(万)", ascending=False)
    max_change_records = [build_record(row, selected_dates) for _, row in max_change_df.iterrows()]

    base_result["分析参数"]["区间涨幅上限%"] = max_change
    base_result["样本信息"]["主力净流入合计超过阈值且区间涨幅不超过上限个数"] = int(len(max_change_df))
    base_result["最近N天主力净流入合计超过阈值且区间涨幅不超过上限"] = limit_records(
        max_change_records,
        top,
    )
    return base_result


def main() -> None:
    args = parse_args()
    if args.days <= 0:
        raise SystemExit("--days 必须大于 0")
    if args.money < 0:
        raise SystemExit("--money 不能小于 0")
    if args.max_change is not None and args.max_change < 0:
        raise SystemExit("--max-change 不能小于 0")

    flow_df = read_matrix(Path(args.flow_input))
    price_df = read_matrix(Path(args.price_input))
    selected_dates = select_dates(flow_df, price_df, args.days)
    merged_df = build_merged_window(flow_df, price_df, selected_dates)
    if args.max_change is None:
        result = analyze(
            merged_df=merged_df,
            selected_dates=selected_dates,
            threshold_yi=args.money,
            top=args.top,
        )
    else:
        result = analyze_with_max_change(
            merged_df=merged_df,
            selected_dates=selected_dates,
            threshold_yi=args.money,
            max_change=args.max_change,
            top=args.top,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
