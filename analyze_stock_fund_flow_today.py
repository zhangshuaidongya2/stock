#!/usr/bin/env python3
"""Analyze today's realtime stock fund-flow CSV.

Examples:
  python analyze_stock_fund_flow_today.py
  python analyze_stock_fund_flow_today.py --top 20
  python analyze_stock_fund_flow_today.py --rank-by super --top 20
  python analyze_stock_fund_flow_today.py --date 0423
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
TODAY_DATA_DIR = DATA_DIR / "today"
DEFAULT_TOP = 10

RANK_COLUMNS = [
    "代码",
    "名称",
    "最新价",
    "涨跌幅%",
    "主力流向",
    "主力净流入(万)",
    "主力净占比%",
    "大单净流入(万)",
    "大单净占比%",
    "超大单净流入(万)",
    "中单净流入(万)",
    "小单净流入(万)",
    "资金流时间",
    "抓取时间",
]


def default_date_tag() -> str:
    return datetime.now().astimezone().strftime("%m%d")


def default_input_path(date_tag: str) -> Path:
    return TODAY_DATA_DIR / f"{date_tag}.csv"


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_DIR))
    except ValueError:
        return str(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="分析当天实时主力资金流 CSV，输出指定资金流入排名 JSON。"
    )
    parser.add_argument(
        "--input",
        help="输入 CSV 文件；不传则读取 data/today/ 下 --date 对应的 MMDD.csv。",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP,
        help=f"输出前多少位，默认 {DEFAULT_TOP}。",
    )
    parser.add_argument(
        "--date",
        default=default_date_tag(),
        help="文件日期标识，格式 MMDD，例如 0423；默认今天。",
    )
    parser.add_argument(
        "--rank-by",
        choices=["main", "super"],
        default="main",
        help="排行类型：main=主力净流入，super=超大单/特大单净流入；默认 main。",
    )
    return parser.parse_args()


def normalize_date_tag(value: str) -> str:
    date_tag = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(date_tag) != 4:
        raise SystemExit("--date 必须是 MMDD 格式，例如 0423")
    return date_tag


def read_today_csv(input_path: Path) -> pd.DataFrame:
    if not input_path.exists():
        raise SystemExit(f"输入文件不存在：{input_path}")
    try:
        df = pd.read_csv(input_path, dtype={"代码": str})
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"读取 CSV 失败：{exc}") from exc
    if df.empty:
        raise SystemExit(f"输入文件为空：{input_path}")
    return df


def require_columns(df: pd.DataFrame, columns: list[str]) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise SystemExit("CSV 缺少字段：" + "、".join(missing))


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    numeric_columns = [
        "最新价",
        "涨跌幅%",
        "主力净流入(万)",
        "主力净占比%",
        "大单净流入(万)",
        "大单净占比%",
        "超大单净流入(万)",
        "超大单净占比%",
        "中单净流入(万)",
        "小单净流入(万)",
    ]
    for column in numeric_columns:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    if "代码" in result.columns:
        result["代码"] = result["代码"].astype(str).str.zfill(6)
    return result


def sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): sanitize(val) for key, val in value.items()}
    if isinstance(value, list):
        return [sanitize(item) for item in value]
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:  # noqa: BLE001
            pass
    return value


def rank_by(df: pd.DataFrame, column: str, top: int) -> list[dict[str, Any]]:
    require_columns(df, [column])
    result = df.dropna(subset=[column]).copy()
    result = result.sort_values(column, ascending=False).head(top)
    columns = [item for item in RANK_COLUMNS if item in result.columns]
    return sanitize(result[columns].to_dict(orient="records"))


def rank_config(rank_by_name: str) -> tuple[str, str]:
    if rank_by_name == "super":
        return "超大单净流入最多", "超大单净流入(万)"
    return "主力净流入最多", "主力净流入(万)"


def build_report(
    df: pd.DataFrame,
    date_tag: str,
    input_path: Path,
    top: int,
    rank_by_name: str,
) -> dict[str, Any]:
    title, column = rank_config(rank_by_name)
    return {
        "日期": date_tag,
        "文件": display_path(input_path),
        "top": top,
        "样本数": int(len(df)),
        "排行类型": rank_by_name,
        "排序字段": column,
        title: rank_by(df, column, top),
    }


def main() -> None:
    args = parse_args()
    if args.top <= 0:
        raise SystemExit("--top 必须大于 0")

    date_tag = normalize_date_tag(args.date)
    input_path = Path(args.input) if args.input else default_input_path(date_tag)
    source_df = normalize_df(read_today_csv(input_path))
    report = build_report(source_df, date_tag, input_path, args.top, args.rank_by)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
