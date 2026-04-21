#!/usr/bin/env python3
"""Analyze exported stock fund-flow summary CSV.

Examples:
  python analyze_stock_fund_flow.py
  python analyze_stock_fund_flow.py --min-net-inflow-yi 10
  python analyze_stock_fund_flow.py --format json
  python analyze_stock_fund_flow.py --output strong_inflow.csv
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_INPUT_PATH = Path(__file__).with_name("stock_fund_flow_30d_summary.csv")
DEFAULT_OUTPUT_PATH = Path(__file__).with_name("stock_fund_flow_strong_inflow.csv")
DEFAULT_MIN_NET_INFLOW_YI = 8.0

NET_INFLOW_COLUMN = "主力净流入合计(万)"
OUTPUT_COLUMNS = [
    "代码",
    "名称",
    "查询天数",
    "统计交易日数",
    "起始日期",
    "开始股价",
    "结束日期",
    "结束股价",
    "主力净流入合计(万)",
    "主力净流入合计(亿)",
    "主力流入总和(万)",
    "主力流出总和(万)",
    "主力流入天数",
    "主力流出天数",
    "结论",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="分析资金流汇总 CSV，筛选主力净流入超过指定金额的股票。"
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT_PATH),
        help=f"输入 CSV 文件，默认 {DEFAULT_INPUT_PATH.name}。",
    )
    parser.add_argument(
        "--min-net-inflow-yi",
        type=float,
        default=DEFAULT_MIN_NET_INFLOW_YI,
        help=f"最小主力净流入阈值，单位亿元，默认 {DEFAULT_MIN_NET_INFLOW_YI:g}。",
    )
    parser.add_argument(
        "--format",
        choices=["table", "json", "csv"],
        default="csv",
        help="输出格式：table / json / csv；默认 csv。",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help=f"输出文件路径；默认 {DEFAULT_OUTPUT_PATH.name}。传空字符串则打印到终端。",
    )
    return parser.parse_args()


def read_summary_csv(input_path: Path) -> pd.DataFrame:
    if not input_path.exists():
        raise SystemExit(f"输入文件不存在：{input_path}")
    try:
        df = pd.read_csv(input_path, dtype={"代码": str})
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"读取 CSV 失败：{exc}") from exc
    if NET_INFLOW_COLUMN not in df.columns:
        raise SystemExit(f"CSV 缺少字段：{NET_INFLOW_COLUMN}")
    return df


def filter_strong_inflow(df: pd.DataFrame, min_net_inflow_yi: float) -> pd.DataFrame:
    threshold_wan = min_net_inflow_yi * 10000
    result = df.copy()
    result[NET_INFLOW_COLUMN] = pd.to_numeric(
        result[NET_INFLOW_COLUMN],
        errors="coerce",
    )
    result = result[result[NET_INFLOW_COLUMN] > threshold_wan].copy()
    result["主力净流入合计(亿)"] = (result[NET_INFLOW_COLUMN] / 10000).round(2)
    result = result.sort_values(NET_INFLOW_COLUMN, ascending=False)
    columns = [column for column in OUTPUT_COLUMNS if column in result.columns]
    return result[columns]


def emit_result(df: pd.DataFrame, output_format: str, output_path: str) -> None:
    if output_format == "json":
        payload = json.dumps(
            df.where(pd.notna(df), None).to_dict(orient="records"),
            ensure_ascii=False,
            indent=2,
        )
    elif output_format == "table":
        payload = df.to_string(index=False)
    else:
        payload = df.to_csv(index=False)

    if output_path:
        target = Path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")
        print(f"已写入：{target}")
        return
    print(payload)


def main() -> None:
    args = parse_args()
    source_df = read_summary_csv(Path(args.input))
    result_df = filter_strong_inflow(source_df, args.min_net_inflow_yi)
    print(
        f"输入 {len(source_df)} 条，筛选主力净流入 > "
        f"{args.min_net_inflow_yi:g} 亿：{len(result_df)} 条"
    )
    emit_result(result_df, args.format, args.output)


if __name__ == "__main__":
    main()
