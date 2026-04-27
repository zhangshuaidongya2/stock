#!/usr/bin/env python3
"""Rank today's A-share fund-flow records by net inflow.

Examples:
  python flow/rank_today_flow.py
  python flow/rank_today_flow.py --top 20
  python flow/rank_today_flow.py --date 0423
  python flow/rank_today_flow.py --rank-by super --top 30 --format json
  python flow/rank_today_flow.py --positive-only
  python flow/rank_today_flow.py --max-change 5
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_DIR / "data"
TODAY_DATA_DIR = DATA_DIR / "today"
DEFAULT_TOP = 20

OUTPUT_COLUMNS = [
    "代码",
    "名称",
    "最新价",
    "涨跌幅%",
    "主力流向",
    "主力净流入(万)",
    "主力净占比%",
    "超大单净流入(万)",
    "超大单净占比%",
    "大单净流入(万)",
    "大单净占比%",
    "中单净流入(万)",
    "中单净占比%",
    "小单净流入(万)",
    "小单净占比%",
    "资金流时间",
    "抓取时间",
]

RANK_FIELD_MAP = {
    "main": "主力净流入(万)",
    "super": "超大单净流入(万)",
    "large": "大单净流入(万)",
    "main_ratio": "主力净占比%",
    "change": "涨跌幅%",
}


def default_date_tag() -> str:
    return datetime.now().astimezone().strftime("%m%d")


def default_input_path(date_tag: str) -> Path:
    return TODAY_DATA_DIR / f"{date_tag}.csv"


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_DIR))
    except ValueError:
        return str(path)


def resolve_project_path(path: str | Path) -> Path:
    resolved_path = Path(path)
    if resolved_path.is_absolute():
        return resolved_path
    return PROJECT_DIR / resolved_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="读取当日资金流 CSV，输出今日资金流入排行前 N 名。"
    )
    parser.add_argument(
        "--input",
        help="输入 CSV 文件；不传则读取 data/today/ 下 --date 对应的 MMDD.csv。",
    )
    parser.add_argument(
        "--date",
        default=default_date_tag(),
        help="文件日期标识，格式 MMDD，例如 0423；默认今天。",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP,
        help=f"输出前多少位，默认 {DEFAULT_TOP}。",
    )
    parser.add_argument(
        "--rank-by",
        choices=sorted(RANK_FIELD_MAP),
        default="main",
        help="排序字段：main=主力净流入，super=超大单净流入，large=大单净流入，main_ratio=主力净占比，change=涨跌幅。",
    )
    parser.add_argument(
        "--positive-only",
        action="store_true",
        help="只保留排序字段大于 0 的股票。",
    )
    parser.add_argument(
        "--max-change",
        type=float,
        help="涨跌幅上限，单位百分比；例如 5 表示只保留涨跌幅小于 5%% 的股票。",
    )
    parser.add_argument(
        "--format",
        choices=["table", "json", "csv"],
        default="json",
        help="输出格式：table / json / csv；默认 json。",
    )
    parser.add_argument("--output", help="输出文件路径；不传则打印到终端。")
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
        "超大单净流入(万)",
        "超大单净占比%",
        "大单净流入(万)",
        "大单净占比%",
        "中单净流入(万)",
        "中单净占比%",
        "小单净流入(万)",
        "小单净占比%",
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


def build_rank_df(
    df: pd.DataFrame,
    rank_by_name: str,
    top: int,
    positive_only: bool,
    max_change_pct: float | None,
) -> pd.DataFrame:
    column = RANK_FIELD_MAP[rank_by_name]
    require_columns(df, [column])
    ranked = df.dropna(subset=[column]).copy()
    if positive_only:
        ranked = ranked[ranked[column] > 0].copy()
    if max_change_pct is not None:
        require_columns(ranked, ["涨跌幅%"])
        ranked = ranked.dropna(subset=["涨跌幅%"]).copy()
        ranked = ranked[ranked["涨跌幅%"] < max_change_pct].copy()
    ranked = ranked.sort_values(column, ascending=False).head(top)
    columns = [item for item in OUTPUT_COLUMNS if item in ranked.columns]
    return ranked[columns]


def render_table(df: pd.DataFrame) -> str:
    return df.to_string(index=False)


def render_json(df: pd.DataFrame) -> str:
    return json.dumps(sanitize(df.to_dict(orient="records")), ensure_ascii=False, indent=2)


def write_output(df: pd.DataFrame, output_format: str, output_path: Path | None) -> None:
    if output_path is None:
        if output_format == "table":
            print(render_table(df))
            return
        if output_format == "json":
            print(render_json(df))
            return
        df.to_csv(sys.stdout, index=False)
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_format == "table":
        output_path.write_text(render_table(df), encoding="utf-8")
    elif output_format == "json":
        output_path.write_text(render_json(df), encoding="utf-8")
    else:
        df.to_csv(output_path, index=False)
    print(f"已写入 {display_path(output_path)}")


def main() -> None:
    args = parse_args()
    if args.top <= 0:
        raise SystemExit("--top 必须大于 0")

    date_tag = normalize_date_tag(args.date)
    input_path = resolve_project_path(args.input) if args.input else default_input_path(date_tag)
    source_df = normalize_df(read_today_csv(input_path))
    rank_df = build_rank_df(
        df=source_df,
        rank_by_name=args.rank_by,
        top=args.top,
        positive_only=args.positive_only,
        max_change_pct=args.max_change,
    )
    output_path = resolve_project_path(args.output) if args.output else None
    write_output(rank_df, args.format, output_path)


if __name__ == "__main__":
    main()
