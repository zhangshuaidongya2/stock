#!/usr/bin/env python3
"""Fetch A-share main fund flow history.

Examples:
  python stock_fund_flow.py
  python stock_fund_flow.py --symbols 000001,600519 --days 10
  python stock_fund_flow.py --symbols 平安银行,贵州茅台 --days 20
  python stock_fund_flow.py --symbols 000001 --days 10 --no-details
  python stock_fund_flow.py --symbols 000001 --days 10 --details
  python stock_fund_flow.py --symbols 300750 --days 30 --output fund_flow.csv --format csv

Default JSON output contains 结论 only. Set DEFAULT_SHOW_DETAILS=True or pass
--details to include daily fund-flow records.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any


pd = None

SYMBOL_CACHE_PATH = Path(__file__).with_name("stock_symbols_cache.json")
DEFAULT_SYMBOLS = "002342"
DEFAULT_DAYS = 125
DEFAULT_FORMAT = "json"
DEFAULT_SHOW_DETAILS = False

NET_AMOUNT_COLUMNS = {
    "主力净流入-净额": "主力净流入(万)",
    "超大单净流入-净额": "超大单净流入(万)",
    "大单净流入-净额": "大单净流入(万)",
    "中单净流入-净额": "中单净流入(万)",
    "小单净流入-净额": "小单净流入(万)",
}

TABLE_COLUMNS = [
    "代码",
    "名称",
    "日期",
    "收盘价",
    "涨跌幅%",
    "主力流向",
    "主力净流入(万)",
    "超大单净流入(万)",
    "大单净流入(万)",
    "中单净流入(万)",
    "小单净流入(万)",
]

SUMMARY_COLUMNS = [
    "代码",
    "名称",
    "查询天数",
    "统计交易日数",
    "起始日期",
    "开始股价",
    "结束日期",
    "结束股价",
    "主力流入总和(万)",
    "主力流出总和(万)",
    "主力净流入合计(万)",
    "主力流入天数",
    "主力流出天数",
    "结论",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="抓取 A 股个股最近 N 个交易日的主力资金流入流出情况。"
    )
    parser.add_argument(
        "--symbols",
        default=DEFAULT_SYMBOLS,
        help="股票代码或名称，多个用逗号分隔；默认 002342。",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"查询最近多少个交易日的资金流数据，默认 {DEFAULT_DAYS}。",
    )
    parser.add_argument(
        "--format",
        choices=["table", "json", "csv"],
        default=DEFAULT_FORMAT,
        help="输出格式：table / json / csv；默认 json。",
    )
    details_group = parser.add_mutually_exclusive_group()
    details_group.add_argument(
        "--details",
        action="store_true",
        dest="show_details",
        default=DEFAULT_SHOW_DETAILS,
        help="输出每日明细。",
    )
    details_group.add_argument(
        "--no-details",
        "--summary-only",
        action="store_false",
        dest="show_details",
        default=DEFAULT_SHOW_DETAILS,
        help="不输出每日明细，只输出结论汇总。",
    )
    parser.add_argument(
        "--ascending",
        action="store_true",
        help="按日期升序输出；默认按日期降序，最新交易日排在前面。",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="查询多只股票时的请求间隔秒数，默认 0.2。",
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


def detect_market(code: str) -> str:
    normalized = normalize_code(code)
    if normalized.startswith("6"):
        return "sh"
    if normalized.startswith(("4", "8", "9")):
        return "bj"
    return "sz"


def read_symbol_cache() -> dict[str, str]:
    if not SYMBOL_CACHE_PATH.exists():
        return {}
    try:
        payload = json.loads(SYMBOL_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        str(name).strip(): normalize_code(code)
        for name, code in payload.items()
        if str(name).strip() and normalize_code(code)
    }


def resolve_symbols(symbols: str) -> list[dict[str, str]]:
    tokens = [item.strip() for item in symbols.split(",") if item.strip()]
    if not tokens:
        raise SystemExit("--symbols 不能为空")

    name_map = read_symbol_cache()
    code_name_map = {code: name for name, code in name_map.items()}
    resolved = []
    unresolved = []

    for token in tokens:
        code = normalize_code(token)
        if code and any(ch.isdigit() for ch in token):
            resolved.append({"代码": code, "名称": code_name_map.get(code, "")})
            continue

        cached_code = name_map.get(token)
        if cached_code:
            resolved.append({"代码": cached_code, "名称": token})
        else:
            unresolved.append(token)

    if unresolved:
        raise SystemExit(
            "无法解析股票名称："
            + "、".join(unresolved)
            + "。请改用股票代码，或更新 stock_symbols_cache.json。"
        )
    return resolved


def safe_float(value: Any) -> float | None:
    if value is None:
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


def build_direction(value: Any) -> str:
    converted = safe_float(value)
    if converted is None:
        return ""
    if converted > 0:
        return "流入"
    if converted < 0:
        return "流出"
    return "持平"


def format_wan(value: Any) -> str:
    converted = safe_float(value)
    if converted is None:
        return "数据不足"
    return f"{converted:.2f}万元"


def sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): sanitize(val) for key, val in value.items()}
    if isinstance(value, list):
        return [sanitize(item) for item in value]
    if pd is not None and pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:  # noqa: BLE001
            pass
    return value


def normalize_fund_flow_df(
    df: Any,
    symbol: dict[str, str],
    days: int,
    ascending: bool,
) -> Any:
    if df is None or df.empty:
        raise RuntimeError("资金流接口返回空数据")

    result = df.copy()
    result["日期"] = pd.to_datetime(result["日期"], errors="coerce")
    result = result.dropna(subset=["日期"]).sort_values("日期")
    result = result.tail(days).copy()

    result.insert(0, "名称", symbol.get("名称", ""))
    result.insert(0, "代码", symbol["代码"])
    result["涨跌幅%"] = result["涨跌幅"].map(lambda value: round_or_none(value))
    result["收盘价"] = result["收盘价"].map(lambda value: round_or_none(value))

    for raw_col, output_col in NET_AMOUNT_COLUMNS.items():
        if raw_col not in result.columns:
            result[output_col] = None
            continue
        result[output_col] = result[raw_col].map(
            lambda value: round_or_none(
                safe_float(value) / 10000 if safe_float(value) is not None else None
            )
        )

    result["主力流向"] = result["主力净流入(万)"].map(build_direction)
    result["日期"] = result["日期"].dt.strftime("%Y-%m-%d")

    visible_columns = [col for col in TABLE_COLUMNS if col in result.columns]
    result = result[visible_columns]
    if not ascending:
        result = result.sort_values(["代码", "日期"], ascending=[True, False])
    return result


def build_summary_text(
    day_count: int,
    start_price: float | None,
    end_price: float | None,
    inflow_total: float | None,
    outflow_total: float | None,
    net_total: float | None,
    inflow_days: int,
    outflow_days: int,
) -> str:
    if net_total is None:
        return f"近{day_count}个交易日主力资金数据不足，无法判断净流向。"

    if net_total > 0:
        net_direction = "净流入"
        net_amount = net_total
    elif net_total < 0:
        net_direction = "净流出"
        net_amount = abs(net_total)
    else:
        net_direction = "净流入为零"
        net_amount = 0.0

    if net_direction == "净流入为零":
        net_phrase = "合计净流入为0.00万元"
    else:
        net_phrase = f"合计{net_direction}{format_wan(net_amount)}"

    return (
        f"近{day_count}个交易日股价从{round_or_none(start_price)}到"
        f"{round_or_none(end_price)}；主力资金累计流入{format_wan(inflow_total)}，"
        f"累计流出{format_wan(outflow_total)}，{net_phrase}；"
        f"流入{inflow_days}天，流出{outflow_days}天。"
    )


def build_summary_df(detail_df: Any, requested_days: int) -> Any:
    summaries = []
    grouped = detail_df.groupby(["代码", "名称"], dropna=False, sort=False)
    for (code, name), group in grouped:
        main_net = pd.to_numeric(group["主力净流入(万)"], errors="coerce")
        valid_net = main_net.dropna()
        day_count = int(len(group))
        dated_group = group.copy()
        dated_group["_日期排序"] = pd.to_datetime(dated_group["日期"], errors="coerce")
        dated_group = dated_group.dropna(subset=["_日期排序"]).sort_values("_日期排序")

        inflow_total = None
        outflow_total = None
        net_total = None
        inflow_days = 0
        outflow_days = 0
        start_date = ""
        end_date = ""
        start_price = None
        end_price = None

        if not dated_group.empty:
            first_row = dated_group.iloc[0]
            last_row = dated_group.iloc[-1]
            start_date = str(first_row["日期"])
            end_date = str(last_row["日期"])
            start_price = round_or_none(first_row.get("收盘价"))
            end_price = round_or_none(last_row.get("收盘价"))

        if not valid_net.empty:
            inflow_values = valid_net[valid_net > 0]
            outflow_values = valid_net[valid_net < 0]
            inflow_total = round_or_none(inflow_values.sum())
            outflow_total = round_or_none(abs(outflow_values.sum()))
            net_total = round_or_none(valid_net.sum())
            inflow_days = int((valid_net > 0).sum())
            outflow_days = int((valid_net < 0).sum())

        summaries.append(
            {
                "代码": code,
                "名称": name,
                "查询天数": requested_days,
                "统计交易日数": day_count,
                "起始日期": start_date,
                "开始股价": start_price,
                "结束日期": end_date,
                "结束股价": end_price,
                "主力流入总和(万)": inflow_total,
                "主力流出总和(万)": outflow_total,
                "主力净流入合计(万)": net_total,
                "主力流入天数": inflow_days,
                "主力流出天数": outflow_days,
                "结论": build_summary_text(
                    day_count=day_count,
                    start_price=start_price,
                    end_price=end_price,
                    inflow_total=inflow_total,
                    outflow_total=outflow_total,
                    net_total=net_total,
                    inflow_days=inflow_days,
                    outflow_days=outflow_days,
                ),
            }
        )

    return pd.DataFrame(summaries, columns=SUMMARY_COLUMNS)


def fetch_fund_flow(ak: Any, symbol: dict[str, str], days: int, ascending: bool) -> Any:
    code = symbol["代码"]
    market = detect_market(code)
    df = ak.stock_individual_fund_flow(stock=code, market=market)
    return normalize_fund_flow_df(
        df=df,
        symbol=symbol,
        days=days,
        ascending=ascending,
    )


def emit_result(
    detail_df: Any,
    summary_df: Any,
    output_format: str,
    show_details: bool,
    output_path: str | None,
) -> None:
    if output_format == "json":
        result = {
            "结论": sanitize(summary_df.to_dict(orient="records")),
        }
        if show_details:
            result["明细"] = sanitize(detail_df.to_dict(orient="records"))
        payload = json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    elif output_format == "csv":
        if show_details:
            payload = detail_df.merge(
                summary_df,
                on=["代码", "名称"],
                how="left",
            ).to_csv(index=False)
        else:
            payload = summary_df.to_csv(index=False)
    else:
        payload = "结论:\n" + summary_df.to_string(index=False)
        if show_details:
            payload += "\n\n明细:\n" + detail_df.to_string(index=False)

    if output_path:
        target = Path(output_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")
        print(f"已写入：{target}")
        return
    print(payload)


def main() -> None:
    args = parse_args()
    if args.days <= 0:
        raise SystemExit("--days 必须大于 0")
    if args.delay < 0:
        raise SystemExit("--delay 不能小于 0")

    ak = require_dependencies()
    symbols = resolve_symbols(args.symbols)

    frames = []
    errors = []
    for index, symbol in enumerate(symbols, start=1):
        code = symbol["代码"]
        name = symbol.get("名称", "")
        print(f"[{index}/{len(symbols)}] 获取 {code} {name} 资金流...", file=sys.stderr)
        try:
            frames.append(fetch_fund_flow(ak, symbol, args.days, args.ascending))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{code} {name}: {type(exc).__name__}: {exc}")
        if args.delay > 0 and index < len(symbols):
            time.sleep(args.delay)

    if not frames:
        print(f"错误：获取资金流失败。{'；'.join(errors)}", file=sys.stderr)
        raise SystemExit(1)

    if errors:
        print(f"警告：部分股票获取失败：{'；'.join(errors)}", file=sys.stderr)

    detail_df = pd.concat(frames, ignore_index=True)
    summary_df = build_summary_df(detail_df, requested_days=args.days)
    emit_result(detail_df, summary_df, args.format, args.show_details, args.output)


if __name__ == "__main__":
    main()
