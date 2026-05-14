#!/usr/bin/env python3
"""Summarize daily total main fund inflow/outflow from data/today/*.csv as JSON.

Examples:
  python flow/sum_daily_main_flow.py
  python flow/sum_daily_main_flow.py --input-dir data/today
  python flow/sum_daily_main_flow.py --output data/daily_main_flow_summary.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_DIR / "data"
TODAY_DATA_DIR = DATA_DIR / "today"
DATE_TAG_PATTERN = re.compile(r"^\d{4}$")
MAIN_FLOW_COLUMN = "主力净流入(万)"
REQUIRED_COLUMNS = ["代码", MAIN_FLOW_COLUMN]


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
        description=(
            "统计 data/today/ 下每个 MMDD.csv 的全市场主力资金合计，"
            "包括主力净额、流入合计、流出合计和对应股票数。"
        )
    )
    parser.add_argument(
        "--input-dir",
        default=str(TODAY_DATA_DIR),
        help=f"输入目录，默认 {display_path(TODAY_DATA_DIR)}。",
    )
    parser.add_argument(
        "--output",
        help="输出 JSON 文件；不传则直接打印 JSON 到终端。",
    )
    return parser.parse_args()


def normalize_code(value: object) -> str:
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def round_yi(value: float) -> float:
    return round(float(value) / 10000, 4)


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
            f"文件缺少字段 {'、'.join(missing)}：{display_path(file_path)}"
        )


def read_daily_main_flow(file_path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(file_path, dtype={"代码": str})
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"读取 CSV 失败 {display_path(file_path)}：{exc}") from exc

    require_columns(df, file_path)
    if df.empty:
        return pd.DataFrame(columns=["代码", MAIN_FLOW_COLUMN])

    result = df.copy()
    result["代码"] = result["代码"].map(normalize_code)
    result[MAIN_FLOW_COLUMN] = pd.to_numeric(result[MAIN_FLOW_COLUMN], errors="coerce")
    result = result[result["代码"] != ""].copy()

    if "抓取时间" in result.columns:
        result = result.sort_values("抓取时间", kind="stable")

    result = result.drop_duplicates(subset=["代码"], keep="last")
    return result[["代码", MAIN_FLOW_COLUMN]]


def summarize_daily_file(file_path: Path) -> dict[str, float | int | str]:
    df = read_daily_main_flow(file_path)
    main_flow = df[MAIN_FLOW_COLUMN].dropna()
    positive_flow = main_flow[main_flow > 0]
    negative_flow = main_flow[main_flow < 0]

    net_wan = float(main_flow.sum())
    inflow_wan = float(positive_flow.sum())
    outflow_wan = float(negative_flow.sum())
    outflow_abs_wan = abs(outflow_wan)

    return {
        "日期": file_path.stem,
        "股票数": int(len(main_flow)),
        "主力净额合计(亿)": round_yi(net_wan),
        "主力流入合计(亿)": round_yi(inflow_wan),
        "主力流出合计(亿)": round_yi(outflow_wan),
        "主力流出金额合计(亿)": round_yi(outflow_abs_wan),
        "主力流入股票数": int((main_flow > 0).sum()),
        "主力流出股票数": int((main_flow < 0).sum()),
        "主力持平股票数": int((main_flow == 0).sum()),
    }


def build_payload(files: list[Path]) -> dict[str, object]:
    rows = [summarize_daily_file(file_path) for file_path in files]
    date_tags = [str(row["日期"]) for row in rows]
    return {
        "统计口径": "每个 MMDD.csv 按股票代码去重后，汇总字段：主力净流入(万)，输出金额单位：亿",
        "样本信息": {
            "日文件数": len(files),
            "日期范围": [date_tags[0], date_tags[-1]],
        },
        "每日主力资金汇总": rows,
    }


def render_json(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def write_output(payload: dict[str, object], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_json(payload) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    input_dir = resolve_project_path(args.input_dir)

    files = list_daily_files(input_dir)
    payload = build_payload(files)
    if args.output:
        output_path = resolve_project_path(args.output)
        write_output(payload, output_path)
        print(f"已写入 {display_path(output_path)}", file=sys.stderr)
        return

    print(render_json(payload))


if __name__ == "__main__":
    main()
