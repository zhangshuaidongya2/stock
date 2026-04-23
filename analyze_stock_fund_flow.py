#!/usr/bin/env python3
"""Analyze exported stock fund-flow summary CSV.

Examples:
  python analyze_stock_fund_flow.py
  python analyze_stock_fund_flow.py --days 15 --money 10
  python analyze_stock_fund_flow.py --days 15 --money 10 --max-change 5
  python analyze_stock_fund_flow.py --days 7 --money 5
  python analyze_stock_fund_flow.py --days 3 --format table
  python analyze_stock_fund_flow.py --days 30 --output strong_inflow.json
  python analyze_stock_fund_flow.py --code 000001
  python analyze_stock_fund_flow.py --input stock_fund_flow_30d_summary.csv --code 000001
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
DEFAULT_INPUT_PATH = DATA_DIR / "fund.csv"
DEFAULT_OUTPUT_PATH = DATA_DIR / "stock_fund_flow_strong_inflow.csv"
DEFAULT_DAYS = 30
DEFAULT_MIN_NET_INFLOW_YI = 8.0
DEFAULT_FORMAT = "csv"

NET_INFLOW_FIELD = "主力净流入合计(万)"
WINDOW_NET_INFLOW_PATTERN = re.compile(r"^(\d+)日主力净流入合计\(万\)$")
WINDOW_FIELD_MAP = {
    "统计交易日数": "统计交易日数",
    "起始股价": "起始股价",
    "股价涨跌幅%": "股价涨跌幅%",
    "主力流入总和(万)": "主力流入总和(万)",
    "主力流出总和(万)": "主力流出总和(万)",
    "主力净流入合计(万)": NET_INFLOW_FIELD,
    "主力流入天数": "主力流入天数",
    "主力流出天数": "主力流出天数",
}
OUTPUT_COLUMNS = [
    "代码",
    "名称",
    "最新股价",
    "查询天数",
    "统计交易日数",
    "起始股价",
    "股价涨跌幅%",
    "主力净流入合计(万)",
    "主力净流入合计(亿)",
    "主力流入总和(万)",
    "主力流出总和(万)",
    "主力流入天数",
    "主力流出天数",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_DIR))
    except ValueError:
        return str(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="分析 data/fund.csv 多周期资金流汇总，按天数和主力净流入金额筛选股票。"
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT_PATH),
        help=f"输入 CSV 文件，默认 {display_path(DEFAULT_INPUT_PATH)}。",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"筛选哪个周期的资金流数据，例如 30 / 15 / 7 / 3，默认 {DEFAULT_DAYS}。",
    )
    parser.add_argument(
        "--money",
        type=float,
        default=DEFAULT_MIN_NET_INFLOW_YI,
        help=f"最小主力净流入阈值，单位亿元，默认 {DEFAULT_MIN_NET_INFLOW_YI:g}。",
    )
    parser.add_argument(
        "--max-change",
        type=float,
        help="股价涨跌幅上限，单位百分比；例如 5 表示只保留涨跌幅小于 5%% 的股票。",
    )
    parser.add_argument(
        "--code",
        help=(
            "指定股票代码，输出该股票的 JSON 摘要："
            "指定天数净流入（保留正负）与股价涨跌幅。"
        ),
    )
    parser.add_argument(
        "--format",
        choices=["table", "json", "csv"],
        default=DEFAULT_FORMAT,
        help=f"普通筛选模式输出格式：table / json / csv；默认 {DEFAULT_FORMAT}。指定 --code 时固定输出 json。",
    )
    parser.add_argument(
        "--output",
        help=(
            "输出文件路径。普通筛选模式默认 "
            f"{display_path(DEFAULT_OUTPUT_PATH)}；指定 --code 时默认打印到终端。"
        ),
    )
    return parser.parse_args()


def read_summary_csv(input_path: Path) -> pd.DataFrame:
    if not input_path.exists():
        raise SystemExit(f"输入文件不存在：{input_path}")
    try:
        df = pd.read_csv(input_path, dtype={"代码": str})
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"读取 CSV 失败：{exc}") from exc
    return df


def normalize_code(value: object) -> str:
    code = str(value).strip().lower()
    for prefix in ("sh", "sz", "bj"):
        if code.startswith(prefix):
            code = code[len(prefix) :]
    digits = "".join(ch for ch in code if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(converted):
        return None
    return converted


def round_or_none(value: object, digits: int = 2) -> float | None:
    converted = safe_float(value)
    if converted is None:
        return None
    return round(converted, digits)


def sanitize(value: object) -> object:
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


def available_window_days(df: pd.DataFrame) -> list[int]:
    days = []
    for column in df.columns:
        match = WINDOW_NET_INFLOW_PATTERN.match(column)
        if match:
            days.append(int(match.group(1)))
    return sorted(set(days), reverse=True)


def select_window(df: pd.DataFrame, days: int) -> pd.DataFrame:
    prefix = f"{days}日"
    net_column = f"{prefix}{NET_INFLOW_FIELD}"

    if net_column in df.columns:
        rename_map = {
            f"{prefix}{source_field}": output_field
            for source_field, output_field in WINDOW_FIELD_MAP.items()
            if f"{prefix}{source_field}" in df.columns
        }
        result = df.rename(columns=rename_map).copy()
        result["查询天数"] = days
        return result

    if NET_INFLOW_FIELD in df.columns:
        result = df.copy()
        if "查询天数" not in result.columns:
            result["查询天数"] = days
        return result

    available_days = available_window_days(df)
    available_text = "、".join(str(item) for item in available_days) or "无"
    raise SystemExit(
        f"CSV 缺少 {days} 日资金流字段：{net_column}。"
        f"当前文件可用天数：{available_text}。"
    )


def filter_strong_inflow(
    df: pd.DataFrame,
    days: int,
    min_net_inflow_yi: float,
    max_change_pct: float | None = None,
) -> pd.DataFrame:
    threshold_wan = min_net_inflow_yi * 10000
    result = select_window(df, days)
    result[NET_INFLOW_FIELD] = pd.to_numeric(
        result[NET_INFLOW_FIELD],
        errors="coerce",
    )
    result = result[result[NET_INFLOW_FIELD] > threshold_wan].copy()
    if max_change_pct is not None:
        if "股价涨跌幅%" not in result.columns:
            raise SystemExit("CSV 缺少字段：股价涨跌幅%")
        result["股价涨跌幅%"] = pd.to_numeric(
            result["股价涨跌幅%"],
            errors="coerce",
        )
        result = result[result["股价涨跌幅%"] < max_change_pct].copy()
    result["主力净流入合计(亿)"] = (result[NET_INFLOW_FIELD] / 10000).round(2)
    result = result.sort_values(NET_INFLOW_FIELD, ascending=False)
    columns = [column for column in OUTPUT_COLUMNS if column in result.columns]
    return result[columns]


def find_stock_row(df: pd.DataFrame, code: str) -> pd.Series:
    if "代码" not in df.columns:
        raise SystemExit("CSV 缺少字段：代码")
    normalized_target = normalize_code(code)
    if not normalized_target:
        raise SystemExit("请输入有效的股票代码，例如 000001。")
    matched = df[df["代码"].map(normalize_code) == normalized_target]
    if matched.empty:
        raise SystemExit(f"CSV 未找到股票代码：{normalized_target}")
    return matched.iloc[0]


def flow_direction(net_inflow_wan: float | None) -> str:
    if net_inflow_wan is None:
        return "未知"
    if net_inflow_wan > 0:
        return "净流入"
    if net_inflow_wan < 0:
        return "净流出"
    return "持平"


def price_direction(change_pct: float | None) -> str:
    if change_pct is None:
        return "未知"
    if change_pct > 0:
        return "上涨"
    if change_pct < 0:
        return "下跌"
    return "持平"


def compute_price_change_pct(start_price: object, end_price: object) -> float | None:
    start = safe_float(start_price)
    end = safe_float(end_price)
    if start in (None, 0) or end is None:
        return None
    return round((end - start) / start * 100, 2)


def build_period_summary(
    row: pd.Series,
    days: int,
    *,
    net_inflow_field: str,
    stat_days_field: str,
    start_price_field: str,
    price_change_pct_field: str | None,
    end_price_for_fallback: object,
    inflow_total_field: str,
    outflow_total_field: str,
    inflow_days_field: str,
    outflow_days_field: str,
) -> dict[str, object]:
    net_inflow_wan = round_or_none(row.get(net_inflow_field))
    net_inflow_yi = (
        round(net_inflow_wan / 10000, 2)
        if net_inflow_wan is not None
        else None
    )
    start_price = round_or_none(row.get(start_price_field))
    change_pct = (
        round_or_none(row.get(price_change_pct_field))
        if price_change_pct_field
        else None
    )
    if change_pct is None:
        change_pct = compute_price_change_pct(start_price, end_price_for_fallback)

    return sanitize(
        {
            "周期天数": days,
            "统计交易日数": row.get(stat_days_field),
            "主力净流入合计(万)": net_inflow_wan,
            "主力净流入合计(亿)": net_inflow_yi,
            "净流向": flow_direction(net_inflow_wan),
            "股价涨跌幅%": change_pct,
            "股价方向": price_direction(change_pct),
            "起始股价": start_price,
            "主力流入总和(万)": round_or_none(row.get(inflow_total_field)),
            "主力流出总和(万)": round_or_none(row.get(outflow_total_field)),
            "主力流入天数": row.get(inflow_days_field),
            "主力流出天数": row.get(outflow_days_field),
        }
    )


def build_stock_summary(df: pd.DataFrame, code: str, requested_days: int) -> dict[str, object]:
    row = find_stock_row(df, code)
    periods: list[dict[str, object]] = []
    windows = available_window_days(df)

    if windows:
        latest_price = round_or_none(row.get("最新股价"))
        for days in windows:
            prefix = f"{days}日"
            periods.append(
                build_period_summary(
                    row=row,
                    days=days,
                    net_inflow_field=f"{prefix}{NET_INFLOW_FIELD}",
                    stat_days_field=f"{prefix}统计交易日数",
                    start_price_field=f"{prefix}起始股价",
                    price_change_pct_field=f"{prefix}股价涨跌幅%",
                    end_price_for_fallback=latest_price,
                    inflow_total_field=f"{prefix}主力流入总和(万)",
                    outflow_total_field=f"{prefix}主力流出总和(万)",
                    inflow_days_field=f"{prefix}主力流入天数",
                    outflow_days_field=f"{prefix}主力流出天数",
                )
            )
    else:
        row_days = int(safe_float(row.get("查询天数")) or requested_days)
        periods.append(
            build_period_summary(
                row=row,
                days=row_days,
                net_inflow_field=NET_INFLOW_FIELD,
                stat_days_field="统计交易日数",
                start_price_field="开始股价" if "开始股价" in row.index else "起始股价",
                price_change_pct_field="股价涨跌幅%" if "股价涨跌幅%" in row.index else None,
                end_price_for_fallback=row.get("结束股价"),
                inflow_total_field="主力流入总和(万)",
                outflow_total_field="主力流出总和(万)",
                inflow_days_field="主力流入天数",
                outflow_days_field="主力流出天数",
            )
        )

    requested_period = None
    for period in periods:
        if int(period.get("周期天数") or -1) == requested_days:
            requested_period = period
            break
    if requested_period is None:
        available_text = "、".join(str(item.get("周期天数")) for item in periods)
        raise SystemExit(
            f"该股票无 {requested_days} 日汇总数据，可用天数：{available_text}"
        )

    latest_price = (
        round_or_none(row.get("最新股价"))
        if "最新股价" in row.index
        else round_or_none(row.get("结束股价"))
    )
    return sanitize(
        {
            "代码": row.get("代码"),
            "名称": row.get("名称"),
            "最新股价": latest_price,
            "指定天数": requested_days,
            "指定周期摘要": requested_period,
            "全部周期摘要": periods,
        }
    )


def to_json_records(df: pd.DataFrame) -> list[dict[str, object]]:
    sanitized = df.astype(object).where(pd.notna(df), None)
    return sanitized.to_dict(orient="records")


def write_payload(payload: str, output_path: str | None) -> None:
    if output_path:
        target = Path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target.unlink()
        target.write_text(payload, encoding="utf-8")
        print(f"已写入：{target}")
        return
    print(payload)


def emit_stock_record(
    summary: dict[str, object],
    output_path: str | None,
) -> None:
    payload = json.dumps(summary, ensure_ascii=False, indent=2)
    write_payload(payload, output_path)


def emit_result(df: pd.DataFrame, output_format: str, output_path: str) -> None:
    if output_format == "json":
        payload = json.dumps(
            to_json_records(df),
            ensure_ascii=False,
            indent=2,
        )
    elif output_format == "table":
        payload = df.to_string(index=False)
    else:
        payload = df.to_csv(index=False)

    write_payload(payload, output_path)


def main() -> None:
    args = parse_args()
    if args.days <= 0:
        raise SystemExit("--days 必须大于 0")

    source_df = read_summary_csv(Path(args.input))
    if args.code:
        summary = build_stock_summary(source_df, args.code, args.days)
        emit_stock_record(summary, args.output)
        return

    result_df = filter_strong_inflow(
        source_df,
        days=args.days,
        min_net_inflow_yi=args.money,
        max_change_pct=args.max_change,
    )
    output_path = args.output or str(DEFAULT_OUTPUT_PATH)
    print(
        f"输入 {len(source_df)} 条，筛选 {args.days} 日主力净流入 > "
        f"{args.money:g} 亿"
        + (
            f"，股价涨跌幅 < {args.max_change:g}%"
            if args.max_change is not None
            else ""
        )
        + f"：{len(result_df)} 条",
        file=sys.stderr,
    )
    emit_result(result_df, args.format, output_path)


if __name__ == "__main__":
    main()
