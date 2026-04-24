#!/usr/bin/env python3
"""Build stock-by-day matrices from data/today/*.csv.

Examples:
  python build_today_flow_matrix.py
  python build_today_flow_matrix.py --input-dir data/today
  python build_today_flow_matrix.py --flow-output data/flow.csv --price-output data/price.csv
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
TODAY_DATA_DIR = DATA_DIR / "today"
DEFAULT_FLOW_OUTPUT_PATH = DATA_DIR / "flow.csv"
DEFAULT_PRICE_OUTPUT_PATH = DATA_DIR / "price.csv"
DATE_TAG_PATTERN = re.compile(r"^\d{4}$")
REQUIRED_COLUMNS = ["代码", "名称", "主力净流入(万)", "最新价"]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_DIR))
    except ValueError:
        return str(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "整理 data/today/ 下按 MMDD 命名的每日主力资金 CSV，"
            "输出按股票为行、按日期为列的净流入矩阵和价格矩阵。"
        )
    )
    parser.add_argument(
        "--input-dir",
        default=str(TODAY_DATA_DIR),
        help=f"输入目录，默认 {display_path(TODAY_DATA_DIR)}。",
    )
    parser.add_argument(
        "--output",
        "--flow-output",
        dest="flow_output",
        default=str(DEFAULT_FLOW_OUTPUT_PATH),
        help=f"净流入矩阵输出文件，默认 {display_path(DEFAULT_FLOW_OUTPUT_PATH)}。",
    )
    parser.add_argument(
        "--price-output",
        default=str(DEFAULT_PRICE_OUTPUT_PATH),
        help=f"价格矩阵输出文件，默认 {display_path(DEFAULT_PRICE_OUTPUT_PATH)}。",
    )
    return parser.parse_args()


def normalize_code(value: object) -> str:
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def list_daily_files(input_dir: Path) -> list[Path]:
    if not input_dir.exists():
        raise SystemExit(f"输入目录不存在：{input_dir}")
    if not input_dir.is_dir():
        raise SystemExit(f"输入路径不是目录：{input_dir}")

    files = sorted(
        path
        for path in input_dir.iterdir()
        if path.is_file() and path.suffix.lower() == ".csv" and DATE_TAG_PATTERN.fullmatch(path.stem)
    )
    if not files:
        raise SystemExit(f"目录下没有找到 MMDD.csv 文件：{input_dir}")
    return files


def require_columns(df: pd.DataFrame, file_path: Path) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise SystemExit(
            f"文件缺少字段 { '、'.join(missing) }：{display_path(file_path)}"
        )


def read_daily_snapshot(file_path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(file_path, dtype={"代码": str})
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"读取 CSV 失败 {display_path(file_path)}：{exc}") from exc

    require_columns(df, file_path)
    if df.empty:
        return pd.DataFrame(columns=["代码", "名称", "日期", "主力净流入(万)", "最新价"])

    result = df.copy()
    result["代码"] = result["代码"].map(normalize_code)
    result["名称"] = result["名称"].fillna("").astype(str).str.strip()
    result["主力净流入(万)"] = pd.to_numeric(result["主力净流入(万)"], errors="coerce")
    result["最新价"] = pd.to_numeric(result["最新价"], errors="coerce")
    result = result[result["代码"] != ""].copy()

    if "抓取时间" in result.columns:
        result = result.sort_values("抓取时间", kind="stable")

    result = result.drop_duplicates(subset=["代码"], keep="last")
    result["日期"] = file_path.stem
    return result[["代码", "名称", "日期", "主力净流入(万)", "最新价"]]


def read_all_snapshots(files: list[Path]) -> pd.DataFrame:
    snapshots = [read_daily_snapshot(file_path) for file_path in files]
    valid_snapshots = [snapshot for snapshot in snapshots if not snapshot.empty]
    if not valid_snapshots:
        raise SystemExit("没有读取到任何有效股票记录。")
    return pd.concat(valid_snapshots, ignore_index=True)


def build_matrix(merged_df: pd.DataFrame, value_column: str) -> pd.DataFrame:
    if value_column not in merged_df.columns:
        raise SystemExit(f"汇总数据缺少字段：{value_column}")

    name_df = (
        merged_df[["代码", "名称", "日期"]]
        .sort_values(["日期", "代码"], kind="stable")
        .drop_duplicates(subset=["代码"], keep="last")
        [["代码", "名称"]]
    )
    pivot_df = merged_df.pivot(
        index="代码",
        columns="日期",
        values=value_column,
    )
    date_columns = sorted(str(column) for column in pivot_df.columns)
    pivot_df = pivot_df.reindex(columns=date_columns)

    output_df = pivot_df.reset_index().merge(name_df, on="代码", how="left")
    output_df = output_df[["代码", "名称", *date_columns]]
    output_df = output_df.sort_values("代码", kind="stable").reset_index(drop=True)
    return output_df


def write_output(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    flow_output_path = Path(args.flow_output)
    price_output_path = Path(args.price_output)

    files = list_daily_files(input_dir)
    merged_df = read_all_snapshots(files)
    flow_df = build_matrix(merged_df, "主力净流入(万)")
    price_df = build_matrix(merged_df, "最新价")
    write_output(flow_df, flow_output_path)
    write_output(price_df, price_output_path)

    date_columns = [column for column in flow_df.columns if DATE_TAG_PATTERN.fullmatch(str(column))]
    print(
        f"已生成 {display_path(flow_output_path)} 和 {display_path(price_output_path)}；"
        f"处理 {len(files)} 个日文件，"
        f"汇总 {len(flow_df)} 只股票，"
        f"日期列 {len(date_columns)} 个。"
    )


if __name__ == "__main__":
    main()
