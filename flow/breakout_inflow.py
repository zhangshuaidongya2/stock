#!/usr/bin/env python3
"""Find volume breakouts confirmed by main fund inflow.

Examples:
  python flow/breakout_inflow.py
  python flow/breakout_inflow.py --date 0513 --top 30
  python flow/breakout_inflow.py --candidate-top 0 --min-volume-ratio 1.8
  python flow/breakout_inflow.py --flow-confirm both --min-main-inflow-wan 1000
  python flow/breakout_inflow.py --format csv --output data/breakout_inflow.csv
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


pd = None

PROJECT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_DIR / "data"
TODAY_DATA_DIR = DATA_DIR / "today"
REQUEST_TIMEOUT = 10
CHINA_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_TOP = 50
DEFAULT_CANDIDATE_TOP = 300

NUMERIC_COLUMNS = [
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

OUTPUT_COLUMNS = [
    "代码",
    "名称",
    "K线日期",
    "收盘价",
    "前20日最高",
    "突破幅度%",
    "成交量(手)",
    "20日均量(手)",
    "量比20日均量",
    "成交额(亿)",
    "涨跌幅%",
    "主力净流入(万)",
    "主力净占比%",
    "超大单净流入(万)",
    "大单净流入(万)",
    "最新价",
    "资金流时间",
]


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


def current_year() -> int:
    return datetime.now(CHINA_TZ).year


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "筛选“20日高点突破 + 成交量放大 + 主力净流入确认”的股票。"
            "默认读取 data/today/ 下最新的 MMDD.csv。"
        )
    )
    parser.add_argument(
        "--input",
        help="资金流 CSV；不传则读取 data/today/ 下 --date 对应文件或最新文件。",
    )
    parser.add_argument(
        "--date",
        help="资金流文件日期标识，格式 MMDD，例如 0513；不传则自动使用最新 today CSV。",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=current_year(),
        help="把 --date 转换成历史行情结束日期时使用的年份，默认当前年份。",
    )
    parser.add_argument(
        "--breakout-window",
        type=int,
        default=20,
        help="突破窗口，默认 20，即收盘价突破前 20 个交易日最高价。",
    )
    parser.add_argument(
        "--min-volume-ratio",
        type=float,
        default=1.5,
        help="当日成交量 / 前 N 日均量的最小倍数，默认 1.5。",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=90,
        help="获取并保留最近多少个交易日用于计算，默认 90。",
    )
    parser.add_argument(
        "--adjust",
        choices=["", "qfq", "hfq"],
        default="qfq",
        help="历史行情复权方式，默认 qfq 前复权。",
    )
    parser.add_argument(
        "--flow-confirm",
        choices=["any", "main", "ratio", "both"],
        default="any",
        help=(
            "主力确认方式：any=净流入或净占比满足阈值；main=只看净流入；"
            "ratio=只看净占比；both=两者都满足。默认 any。"
        ),
    )
    parser.add_argument(
        "--min-main-inflow-wan",
        type=float,
        default=0.0,
        help="主力净流入阈值，单位万元，默认 0。",
    )
    parser.add_argument(
        "--min-main-ratio",
        type=float,
        default=0.0,
        help="主力净占比阈值，单位百分比，默认 0。",
    )
    parser.add_argument(
        "--min-amount-yi",
        type=float,
        default=0.0,
        help="成交额下限，单位亿元，默认 0。",
    )
    parser.add_argument(
        "--require-up-day",
        action="store_true",
        help="要求当日收盘价高于上一交易日收盘价。",
    )
    parser.add_argument(
        "--require-positive-candle",
        action="store_true",
        help="要求当日为阳线，即收盘价高于开盘价。",
    )
    parser.add_argument(
        "--allow-stale-history",
        action="store_true",
        help="允许历史 K 线最新日期早于资金流文件日期；默认跳过这种错位数据。",
    )
    parser.add_argument(
        "--candidate-top",
        type=int,
        default=DEFAULT_CANDIDATE_TOP,
        help=(
            "按主力净流入预筛后最多拉取多少只股票的 K 线，默认 300；"
            "传 0 表示扫描全部资金确认候选。"
        ),
    )
    parser.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP,
        help=f"最多输出多少只命中股票，默认 {DEFAULT_TOP}；传 0 表示全部输出。",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.15,
        help="每只股票历史行情请求后的等待秒数，默认 0.15。",
    )
    parser.add_argument(
        "--format",
        choices=["table", "json", "csv"],
        default="json",
        help="输出格式：table / json / csv，默认 json。",
    )
    parser.add_argument("--output", help="输出文件路径；不传则打印到终端。")
    return parser.parse_args()


def require_dependencies() -> Any:
    global pd
    try:
        import akshare as ak  # type: ignore
        import pandas as pandas  # type: ignore
    except ImportError as exc:
        raise SystemExit(
            "缺少依赖，请先执行：python3 -m pip install -r requirements.txt"
        ) from exc
    pd = pandas
    return ak


def normalize_code(value: Any) -> str:
    code = str(value).strip().lower()
    for prefix in ("sh", "sz", "bj"):
        if code.startswith(prefix):
            code = code[len(prefix) :]
    digits = "".join(ch for ch in code if ch.isdigit())
    return digits.zfill(6)[-6:] if digits else ""


def normalize_date_tag(value: str) -> str:
    date_tag = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(date_tag) != 4:
        raise SystemExit("--date 必须是 MMDD 格式，例如 0513")
    return date_tag


def infer_date_tag(path: Path) -> str | None:
    stem = path.stem
    return stem if len(stem) == 4 and stem.isdigit() else None


def latest_today_file() -> Path:
    files = sorted(
        path
        for path in TODAY_DATA_DIR.glob("*.csv")
        if len(path.stem) == 4 and path.stem.isdigit()
    )
    if not files:
        raise SystemExit(f"没有找到 today CSV：{display_path(TODAY_DATA_DIR)}")
    return files[-1]


def resolve_input_path(args: argparse.Namespace) -> tuple[Path, str | None]:
    if args.input:
        input_path = resolve_project_path(args.input)
        return input_path, normalize_date_tag(args.date) if args.date else infer_date_tag(input_path)

    if args.date:
        date_tag = normalize_date_tag(args.date)
        return TODAY_DATA_DIR / f"{date_tag}.csv", date_tag

    input_path = latest_today_file()
    return input_path, infer_date_tag(input_path)


def build_end_date(date_tag: str | None, year: int) -> str | None:
    if not date_tag:
        return None
    try:
        return datetime.strptime(f"{year}{date_tag}", "%Y%m%d").strftime("%Y%m%d")
    except ValueError as exc:
        raise SystemExit(f"无法解析日期：{year}{date_tag}") from exc


def safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    if pd is not None and pd.isna(converted):
        return None
    return converted


def round_or_none(value: Any, digits: int = 2) -> float | None:
    converted = safe_float(value)
    if converted is None:
        return None
    return round(converted, digits)


def read_today_csv(input_path: Path) -> Any:
    if not input_path.exists():
        raise SystemExit(f"输入文件不存在：{display_path(input_path)}")
    try:
        df = pd.read_csv(input_path, dtype={"代码": str})
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"读取 CSV 失败 {display_path(input_path)}：{exc}") from exc
    if df.empty:
        raise SystemExit(f"输入文件为空：{display_path(input_path)}")
    return df


def normalize_today_df(df: Any) -> Any:
    result = df.copy()
    if "代码" not in result.columns:
        raise SystemExit("CSV 缺少字段：代码")
    if "名称" not in result.columns:
        raise SystemExit("CSV 缺少字段：名称")

    result["代码"] = result["代码"].map(normalize_code)
    result = result[result["代码"] != ""].copy()
    for column in NUMERIC_COLUMNS:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    result = result.drop_duplicates(subset=["代码"], keep="last")
    return result


def flow_confirmed(row: Any, args: argparse.Namespace) -> bool:
    main_inflow = safe_float(row.get("主力净流入(万)"))
    main_ratio = safe_float(row.get("主力净占比%"))
    main_ok = main_inflow is not None and main_inflow > args.min_main_inflow_wan
    ratio_ok = main_ratio is not None and main_ratio > args.min_main_ratio

    if args.flow_confirm == "main":
        return main_ok
    if args.flow_confirm == "ratio":
        return ratio_ok
    if args.flow_confirm == "both":
        return main_ok and ratio_ok
    return main_ok or ratio_ok


def build_candidate_df(df: Any, args: argparse.Namespace) -> Any:
    mask = df.apply(lambda row: flow_confirmed(row, args), axis=1)
    candidates = df[mask].copy()
    if candidates.empty:
        return candidates

    sort_columns = [
        column
        for column in ["主力净流入(万)", "主力净占比%"]
        if column in candidates.columns
    ]
    if sort_columns:
        candidates = candidates.sort_values(sort_columns, ascending=False)
    if args.candidate_top > 0:
        candidates = candidates.head(args.candidate_top)
    return candidates


def fetch_history(ak: Any, code: str, end_date: str | None, days: int, adjust: str) -> Any:
    if end_date is None:
        end_dt = datetime.now(CHINA_TZ)
        end_date = end_dt.strftime("%Y%m%d")
    else:
        end_dt = datetime.strptime(end_date, "%Y%m%d")
    start_date = (end_dt - timedelta(days=max(90, days * 3))).strftime("%Y%m%d")

    hist_df = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
        timeout=REQUEST_TIMEOUT,
    )
    if hist_df is None or hist_df.empty:
        raise RuntimeError("历史行情为空")
    return hist_df.tail(days).copy()


def normalize_history_date(value: Any) -> str:
    try:
        return pd.to_datetime(value).strftime("%Y%m%d")
    except Exception:  # noqa: BLE001
        return str(value)


def analyze_breakout(
    hist_df: Any,
    expected_end_date: str | None,
    args: argparse.Namespace,
) -> dict[str, Any] | None:
    required = {"日期", "开盘", "收盘", "最高", "成交量", "成交额"}
    missing = required - set(hist_df.columns)
    if missing:
        raise RuntimeError("历史行情缺少字段：" + "、".join(sorted(missing)))

    df = hist_df.copy()
    for column in ["开盘", "收盘", "最高", "成交量", "成交额"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=["开盘", "收盘", "最高", "成交量"])
    if len(df) < args.breakout_window + 1:
        return None

    latest_date = normalize_history_date(df["日期"].iloc[-1])
    if (
        expected_end_date
        and not args.allow_stale_history
        and latest_date != expected_end_date
    ):
        return None

    close = df["收盘"].astype(float)
    open_ = df["开盘"].astype(float)
    high = df["最高"].astype(float)
    volume = df["成交量"].astype(float)
    amount = pd.to_numeric(df["成交额"], errors="coerce")

    current_close = safe_float(close.iloc[-1])
    previous_close = safe_float(close.iloc[-2]) if len(close) >= 2 else None
    current_open = safe_float(open_.iloc[-1])
    current_volume = safe_float(volume.iloc[-1])
    current_amount = safe_float(amount.iloc[-1])
    previous_high = safe_float(
        high.rolling(args.breakout_window).max().shift(1).iloc[-1]
    )
    volume_ma = safe_float(
        volume.rolling(args.breakout_window).mean().shift(1).iloc[-1]
    )

    if (
        current_close in (None, 0)
        or current_volume is None
        or previous_high in (None, 0)
        or volume_ma in (None, 0)
    ):
        return None

    volume_ratio = current_volume / volume_ma
    amount_yi = None if current_amount is None else current_amount / 100_000_000
    if current_close <= previous_high:
        return None
    if volume_ratio < args.min_volume_ratio:
        return None
    if args.min_amount_yi > 0 and (amount_yi is None or amount_yi < args.min_amount_yi):
        return None
    if args.require_up_day and previous_close is not None and current_close <= previous_close:
        return None
    if (
        args.require_positive_candle
        and current_open is not None
        and current_close <= current_open
    ):
        return None

    day_change = None
    if previous_close not in (None, 0):
        day_change = (current_close / previous_close - 1) * 100

    return {
        "K线日期": pd.to_datetime(latest_date).strftime("%Y-%m-%d"),
        "收盘价": round_or_none(current_close),
        f"前{args.breakout_window}日最高": round_or_none(previous_high),
        "突破幅度%": round_or_none((current_close / previous_high - 1) * 100),
        "成交量(手)": round_or_none(current_volume, 0),
        f"{args.breakout_window}日均量(手)": round_or_none(volume_ma, 0),
        f"量比{args.breakout_window}日均量": round_or_none(volume_ratio),
        "成交额(亿)": round_or_none(amount_yi),
        "涨跌幅%": round_or_none(day_change),
    }


def build_record(row: Any, signal: dict[str, Any]) -> dict[str, Any]:
    record = {
        "代码": row.get("代码"),
        "名称": row.get("名称"),
        "最新价": round_or_none(row.get("最新价")),
        "主力净流入(万)": round_or_none(row.get("主力净流入(万)")),
        "主力净占比%": round_or_none(row.get("主力净占比%")),
        "超大单净流入(万)": round_or_none(row.get("超大单净流入(万)")),
        "大单净流入(万)": round_or_none(row.get("大单净流入(万)")),
        "资金流时间": row.get("资金流时间"),
    }
    record.update(signal)
    return record


def sort_hits(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        records,
        key=lambda item: (
            safe_float(item.get("主力净流入(万)")) or 0,
            safe_float(item.get("量比20日均量")) or 0,
            safe_float(item.get("突破幅度%")) or 0,
        ),
        reverse=True,
    )


def limit_records(records: list[dict[str, Any]], top: int) -> list[dict[str, Any]]:
    if top <= 0:
        return records
    return records[:top]


def sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): sanitize(val) for key, val in value.items()}
    if isinstance(value, list):
        return [sanitize(item) for item in value]
    try:
        if pd is not None and pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:  # noqa: BLE001
            pass
    return value


def records_to_dataframe(records: list[dict[str, Any]]) -> Any:
    df = pd.DataFrame(records)
    if df.empty:
        return df
    columns = [column for column in OUTPUT_COLUMNS if column in df.columns]
    columns.extend(column for column in df.columns if column not in columns)
    return df[columns]


def render_result(payload: dict[str, Any], output_format: str) -> str:
    result_df = records_to_dataframe(payload["结果"])
    if output_format == "json":
        return json.dumps(sanitize(payload), ensure_ascii=False, indent=2, default=str)
    if output_format == "csv":
        return result_df.to_csv(index=False)
    if result_df.empty:
        return "没有找到符合条件的股票。"
    return result_df.to_string(index=False)


def write_output(content: str, output_path: Path | None) -> None:
    if output_path is None:
        print(content)
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    print(f"已写入：{display_path(output_path)}")


def main() -> None:
    args = parse_args()
    if args.breakout_window <= 1:
        raise SystemExit("--breakout-window 必须大于 1")
    if args.min_volume_ratio <= 0:
        raise SystemExit("--min-volume-ratio 必须大于 0")
    if args.history_days < args.breakout_window + 1:
        raise SystemExit("--history-days 必须大于 --breakout-window")
    if args.candidate_top < 0:
        raise SystemExit("--candidate-top 不能小于 0")
    if args.top < 0:
        raise SystemExit("--top 不能小于 0")
    if args.delay < 0:
        raise SystemExit("--delay 不能小于 0")

    ak = require_dependencies()
    input_path, date_tag = resolve_input_path(args)
    end_date = build_end_date(date_tag, args.year)
    source_df = normalize_today_df(read_today_csv(input_path))
    candidate_df = build_candidate_df(source_df, args)

    hits: list[dict[str, Any]] = []
    failures: list[str] = []
    total = len(candidate_df)
    for index, (_, row) in enumerate(candidate_df.iterrows(), start=1):
        code = str(row.get("代码"))
        name = str(row.get("名称") or "")
        print(f"[{index}/{total}] 检查 {code} {name}...", file=sys.stderr)
        try:
            hist_df = fetch_history(
                ak=ak,
                code=code,
                end_date=end_date,
                days=args.history_days,
                adjust=args.adjust,
            )
            signal = analyze_breakout(hist_df, end_date, args)
            if signal:
                hits.append(build_record(row, signal))
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{code} {name}: {type(exc).__name__}: {exc}")
        if args.delay > 0 and index < total:
            time.sleep(args.delay)

    sorted_hits = sort_hits(hits)
    limited_hits = limit_records(sorted_hits, args.top)
    payload = {
        "分析参数": {
            "输入文件": display_path(input_path),
            "资金流日期": date_tag,
            "历史行情结束日期": end_date,
            "突破窗口": args.breakout_window,
            "最小量比": args.min_volume_ratio,
            "主力确认方式": args.flow_confirm,
            "主力净流入阈值(万)": args.min_main_inflow_wan,
            "主力净占比阈值%": args.min_main_ratio,
            "成交额下限(亿)": args.min_amount_yi,
            "预筛候选上限": args.candidate_top,
            "输出上限": args.top,
        },
        "筛选说明": (
            f"收盘价突破前{args.breakout_window}个交易日最高价，"
            f"成交量大于前{args.breakout_window}日均量的{args.min_volume_ratio:g}倍，"
            "并且主力资金满足确认条件。"
        ),
        "样本信息": {
            "资金流样本数": int(len(source_df)),
            "资金确认候选数": int(len(candidate_df)),
            "命中个数": int(len(sorted_hits)),
            "失败个数": int(len(failures)),
        },
        "结果": limited_hits,
    }
    if failures and args.format == "json":
        payload["失败样例"] = failures[:10]

    content = render_result(payload, args.format)
    output_path = resolve_project_path(args.output) if args.output else None
    write_output(content, output_path)


if __name__ == "__main__":
    main()
