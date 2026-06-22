#!/usr/bin/env python3
"""Inspect one stock's recent or historical volume expansion metrics as JSON.

Examples:
  python flow/get_volume_signal.py --code 000001
  python flow/get_volume_signal.py --code 平安银行
  python flow/get_volume_signal.py --code 军工电子 --history-days 120
  python flow/get_volume_signal.py --code 000001 --date 2026-06-11
  python flow/get_volume_signal.py --code 000001 --date 0611
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

import pandas as pd
import stock_info as stock_info_module
from symbol_search import (
    build_suggestion_message,
    load_symbol_name_map,
    normalize_code,
    normalize_text,
    search_symbol_records,
    symbol_records_from_name_map,
)


SYMBOL_CACHE_PATH = PROJECT_DIR / "stock_symbols_full_cache.json"
DEFAULT_HISTORY_DAYS = 120
DEFAULT_VOLUME_WINDOW = 20
DEFAULT_MIN_VOLUME_RATIO = 1.2
DEFAULT_HISTORY_SOURCE = "sina"
DEFAULT_ADJUST = "qfq"
DEFAULT_QUOTE_SOURCE = "tencent"
MULTI_PERIOD_WINDOWS = (5, 10, 20)
CHINA_TZ = timezone(timedelta(hours=8))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "输入单只股票代码或名称，输出 5/10/20 日多周期量能与量价关系 JSON。"
            "包括成交量量比、成交额量比、收盘位置、上影线、主力资金和量价结论。"
        )
    )
    parser.add_argument(
        "--code",
        required=True,
        help="股票代码或名称，例如 000001、平安银行。",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=DEFAULT_HISTORY_DAYS,
        help=f"历史回看天数，默认 {DEFAULT_HISTORY_DAYS}。",
    )
    parser.add_argument(
        "--date",
        help="可选：目标日期，支持 MMDD、YYYYMMDD、YYYY-MM-DD；不传则分析最新交易日。",
    )
    parser.add_argument(
        "--volume-window",
        type=int,
        default=DEFAULT_VOLUME_WINDOW,
        help=f"均量窗口，默认 {DEFAULT_VOLUME_WINDOW}。",
    )
    parser.add_argument(
        "--min-volume-ratio",
        type=float,
        default=DEFAULT_MIN_VOLUME_RATIO,
        help=f"认定放量的最小量比阈值，默认 {DEFAULT_MIN_VOLUME_RATIO:g}。",
    )
    parser.add_argument(
        "--history-source",
        choices=["auto", "eastmoney", "sina"],
        default=DEFAULT_HISTORY_SOURCE,
        help=f"历史行情来源，默认 {DEFAULT_HISTORY_SOURCE}。",
    )
    parser.add_argument(
        "--adjust",
        choices=["", "qfq", "hfq"],
        default=DEFAULT_ADJUST,
        help=f"复权方式，默认 {DEFAULT_ADJUST}。",
    )
    parser.add_argument("--output", help="输出 JSON 文件；不传则打印到终端。")
    parser.add_argument(
        "--main-net-inflow-yi",
        type=float,
        default=None,
        help="可选：手动传入当日主力净流入金额，单位：亿元；净流出填负数，例如 -20。",
    )
    parser.add_argument(
        "--main-net-inflow-yuan",
        type=float,
        default=None,
        help="可选：手动传入当日主力净流入金额，单位：元；净流出填负数。",
    )
    return parser.parse_args()


def parse_target_date(value: str) -> datetime:
    raw = str(value).strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 4:
        digits = f"{datetime.now().year}{digits}"
    if len(digits) != 8:
        raise SystemExit("--date 必须是 MMDD、YYYYMMDD 或 YYYY-MM-DD，例如 0611 或 2026-06-11")
    try:
        return datetime.strptime(digits, "%Y%m%d")
    except ValueError as exc:
        raise SystemExit(f"无法解析日期：{value}") from exc


def china_now() -> datetime:
    return datetime.now(CHINA_TZ)


def resolve_unique_symbol_match(
    token: str,
    symbol_records: list[dict[str, str]],
) -> tuple[str | None, str, list[dict[str, Any]]]:
    matches = search_symbol_records(symbol_records, token, top=8)
    if len(matches) != 1:
        return None, "", matches

    code = normalize_code(matches[0].get("代码", ""))
    name = str(matches[0].get("名称", "")).strip()
    if not code:
        return None, "", matches
    return code, name, matches


def resolve_stock_identity(token: str) -> tuple[str, str]:
    name_map = load_symbol_name_map(SYMBOL_CACHE_PATH)
    if not name_map:
        raise SystemExit(f"未找到股票缓存：{SYMBOL_CACHE_PATH.name}")

    code_name_map = {normalize_code(code): name for name, code in name_map.items()}
    normalized_input_code = normalize_code(token)
    symbol_records = symbol_records_from_name_map(name_map)

    if normalized_input_code:
        if normalized_input_code in code_name_map:
            return normalized_input_code, str(code_name_map[normalized_input_code]).strip()
        resolved_code, resolved_name, matches = resolve_unique_symbol_match(token, symbol_records)
        if resolved_code:
            print(
                f'提示：未精确命中 "{token}"，已自动使用唯一候选 {resolved_code} {resolved_name}。',
                file=sys.stderr,
            )
            return resolved_code, resolved_name
        raise SystemExit(
            build_suggestion_message(
                token,
                matches,
                not_found_prefix="未找到股票：",
                include_reason=True,
            )
        )

    normalized_input_name = normalize_text(token)
    normalized_name_map = {
        normalize_text(name): (normalize_code(code), str(name).strip())
        for name, code in name_map.items()
    }
    if normalized_input_name in normalized_name_map:
        return normalized_name_map[normalized_input_name]

    resolved_code, resolved_name, matches = resolve_unique_symbol_match(token, symbol_records)
    if resolved_code:
        print(
            f'提示：未精确命中 "{token}"，已自动使用唯一候选 {resolved_code} {resolved_name}。',
            file=sys.stderr,
        )
        return resolved_code, resolved_name
    raise SystemExit(
        build_suggestion_message(
            token,
            matches,
            not_found_prefix="未找到股票：",
            include_reason=True,
        )
    )


def fetch_history_df(
    ak: Any,
    code: str,
    days: int,
    adjust: str,
    source: str,
    target_date: datetime | None = None,
) -> tuple[pd.DataFrame, str]:
    end_dt = target_date or china_now()
    end_date = end_dt.strftime("%Y%m%d")
    start_date = (end_dt - timedelta(days=max(40, days * 3))).strftime("%Y%m%d")
    errors: list[str] = []

    hist_df = None
    resolved_source = source
    if source in ("eastmoney", "auto"):
        try:
            hist_df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
                timeout=stock_info_module.REQUEST_TIMEOUT,
            )
            resolved_source = "东方财富"
        except Exception as exc:  # noqa: BLE001
            hist_df = None
            errors.append(f"东方财富: {stock_info_module.compact_error(exc)}")

    if hist_df is None or hist_df.empty:
        if source == "eastmoney":
            raise SystemExit("历史行情错误：" + "；".join(errors))
        try:
            hist_df = stock_info_module.fetch_sina_history(ak, code, start_date, end_date, adjust)
            resolved_source = "新浪"
        except Exception as exc:  # noqa: BLE001
            errors.append(f"新浪: {stock_info_module.compact_error(exc)}")
            raise SystemExit("历史行情错误：" + "；".join(errors)) from exc

    if hist_df is None or hist_df.empty:
        raise SystemExit("历史行情为空")

    if "日期" not in hist_df.columns:
        raise SystemExit("历史行情缺少字段：日期")

    hist_df = hist_df.copy()
    hist_df["日期"] = pd.to_datetime(hist_df["日期"], errors="coerce")
    hist_df = hist_df.dropna(subset=["日期"]).sort_values("日期").copy()
    if hist_df.empty:
        raise SystemExit("历史行情中没有可用日期数据。")

    if target_date is not None:
        target_ts = pd.Timestamp(target_date.date())
        target_rows = hist_df["日期"] == target_ts
        if not target_rows.any():
            first_date = hist_df["日期"].iloc[0].strftime("%Y-%m-%d")
            last_date = hist_df["日期"].iloc[-1].strftime("%Y-%m-%d")
            raise SystemExit(
                f"--date={target_date.strftime('%Y-%m-%d')} 不在历史行情中。"
                f"当前可用范围：{first_date} ~ {last_date}"
            )
        hist_df = hist_df.loc[hist_df["日期"] <= target_ts].copy()

    hist_df["日期"] = hist_df["日期"].dt.strftime("%Y-%m-%d")
    return hist_df.tail(days).copy(), resolved_source


def safe_float(value: Any) -> float | None:
    return stock_info_module.safe_float(value)


def round_or_none(value: Any, digits: int = 2) -> float | None:
    return stock_info_module.round_or_none(value, digits)


def parse_tencent_quote_time(value: Any) -> datetime | None:
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(digits) != 14:
        return None
    try:
        return datetime.strptime(digits, "%Y%m%d%H%M%S").replace(tzinfo=CHINA_TZ)
    except ValueError:
        return None


def fetch_tencent_realtime_row(code: str, fallback_name: str) -> tuple[dict[str, Any], datetime | None]:
    import requests

    response = requests.get(
        "https://qt.gtimg.cn/q=" + stock_info_module.to_tencent_symbol(code),
        timeout=stock_info_module.REQUEST_TIMEOUT,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    response.raise_for_status()
    text = response.content.decode("gbk", errors="ignore")
    if '="' not in text:
        raise RuntimeError("腾讯返回格式异常")
    payload = text.split('="', 1)[1].rsplit('"', 1)[0]
    parts = payload.split("~")
    if len(parts) < 58 or not parts[2]:
        raise RuntimeError("腾讯未返回可解析行情")

    amount_raw = safe_float(parts[57])
    return (
        {
            "代码": normalize_code(parts[2]),
            "名称": str(parts[1]).strip() or fallback_name,
            "最新价": safe_float(parts[3]),
            "涨跌幅": safe_float(parts[32]),
            "成交量": safe_float(parts[36]),
            "成交额": amount_raw * 10_000 if amount_raw is not None else None,
            "最高": safe_float(parts[33]),
            "最低": safe_float(parts[34]),
            "今开": safe_float(parts[5]),
            "昨收": safe_float(parts[4]),
            "换手率": safe_float(parts[38]),
            "行情来源": "腾讯",
        },
        parse_tencent_quote_time(parts[30]),
    )


def is_usable_realtime_row(row: dict[str, Any]) -> bool:
    latest_price = safe_float(row.get("最新价"))
    previous_close = safe_float(row.get("昨收"))
    volume = safe_float(row.get("成交量"))
    amount = safe_float(row.get("成交额"))
    if latest_price in (None, 0):
        return False
    if volume is not None and volume > 0:
        return True
    if amount is not None and amount > 0:
        return True
    if previous_close in (None, 0):
        return False
    for field in ("今开", "最高", "最低", "最新价"):
        field_value = safe_float(row.get(field))
        if field_value is not None and abs(field_value - previous_close) > 1e-9:
            return True
    return False


def build_realtime_history_row(
    code: str,
    realtime_row: dict[str, Any],
    quote_time: datetime,
) -> dict[str, Any]:
    return {
        "日期": quote_time.strftime("%Y-%m-%d"),
        "股票代码": code,
        "开盘": safe_float(realtime_row.get("今开")),
        "收盘": safe_float(realtime_row.get("最新价")),
        "最高": safe_float(realtime_row.get("最高")),
        "最低": safe_float(realtime_row.get("最低")),
        "成交量": safe_float(realtime_row.get("成交量")),
        "成交额": safe_float(realtime_row.get("成交额")),
        "换手率": safe_float(realtime_row.get("换手率")),
    }


def merge_realtime_row_into_history(
    hist_df: pd.DataFrame,
    realtime_row: dict[str, Any],
    quote_time: datetime,
    code: str,
) -> pd.DataFrame:
    merged_df = hist_df.copy()
    merged_df["日期"] = pd.to_datetime(merged_df["日期"], errors="coerce")
    realtime_date = pd.Timestamp(quote_time.date())
    realtime_history_row = build_realtime_history_row(code, realtime_row, quote_time)
    realtime_df = pd.DataFrame([realtime_history_row])
    realtime_df["日期"] = pd.to_datetime(realtime_df["日期"], errors="coerce")

    merged_df = merged_df.loc[merged_df["日期"] != realtime_date].copy()
    merged_df = pd.concat([merged_df, realtime_df], ignore_index=True)
    merged_df = merged_df.sort_values("日期").copy()
    merged_df["日期"] = merged_df["日期"].dt.strftime("%Y-%m-%d")
    return merged_df


def maybe_attach_realtime_row(
    hist_df: pd.DataFrame,
    *,
    code: str,
    name: str,
    target_date: datetime | None,
) -> tuple[pd.DataFrame, str, str | None, str | None]:
    if target_date is not None:
        return hist_df, "历史日线", None, None

    try:
        realtime_row, quote_time = fetch_tencent_realtime_row(code, name)
    except Exception as exc:  # noqa: BLE001
        print(
            "提示：未拿到当天实时行情，已回退到历史日线。"
            f"原因：{stock_info_module.compact_error(exc)}",
            file=sys.stderr,
        )
        return hist_df, "历史日线回退", None, None

    if quote_time is None:
        print("提示：实时行情缺少更新时间，已回退到历史日线。", file=sys.stderr)
        return hist_df, "历史日线回退", None, None

    if quote_time.date() != china_now().date():
        print(
            "提示：实时行情未更新到今天，已回退到历史日线。"
            f"实时时间：{quote_time.strftime('%Y-%m-%d %H:%M:%S')}",
            file=sys.stderr,
        )
        return hist_df, "历史日线回退", None, None

    if not is_usable_realtime_row(realtime_row):
        print("提示：当天实时行情关键字段不足，已回退到历史日线。", file=sys.stderr)
        return hist_df, "历史日线回退", None, None

    merged_df = merge_realtime_row_into_history(hist_df, realtime_row, quote_time, code)
    print(
        "提示：已优先使用当天实时行情参与分析。"
        f" 实时来源：{realtime_row.get('行情来源', DEFAULT_QUOTE_SOURCE)}"
        f" 时间：{quote_time.strftime('%Y-%m-%d %H:%M:%S')}",
        file=sys.stderr,
    )
    return (
        merged_df,
        "实时行情+历史日线",
        str(realtime_row.get("行情来源", DEFAULT_QUOTE_SOURCE)),
        quote_time.isoformat(timespec="seconds"),
    )


def calculate_shifted_average_ratio(
    series: pd.Series | None,
    current_value: float | None,
    window: int,
) -> tuple[float | None, float | None]:
    if series is None or series.empty:
        return None, None
    average_value = safe_float(series.rolling(window, min_periods=window).mean().shift(1).iloc[-1])
    if average_value in (None, 0) or current_value is None:
        return average_value, None
    return average_value, current_value / average_value


def calculate_multi_period_ratios(
    volume: pd.Series,
    amount: pd.Series | None,
) -> dict[int, dict[str, float | None]]:
    metrics: dict[int, dict[str, float | None]] = {}
    current_volume = safe_float(volume.iloc[-1]) if not volume.empty else None
    current_amount = safe_float(amount.iloc[-1]) if amount is not None and not amount.empty else None

    for window in MULTI_PERIOD_WINDOWS:
        volume_ma, volume_ratio = calculate_shifted_average_ratio(volume, current_volume, window)
        amount_ma, amount_ratio = calculate_shifted_average_ratio(amount, current_amount, window)
        metrics[window] = {
            "volume_ma": volume_ma,
            "volume_ratio": volume_ratio,
            "amount_ma": amount_ma,
            "amount_ratio": amount_ratio,
        }
    return metrics


def classify_volume_level(main_ratio: float | None) -> tuple[bool | None, str]:
    if main_ratio is None:
        return None, "历史数据不足，无法判断"
    if main_ratio < 0.8:
        return False, "缩量"
    if main_ratio < 1.2:
        return False, "正常量"
    if main_ratio < 1.5:
        return True, "温和放量"
    if main_ratio < 2.0:
        return True, "明显放量"
    if main_ratio < 2.5:
        return True, "大幅放量"
    return True, "极端放量"


def classify_close_position(close_position: float | None) -> str | None:
    if close_position is None:
        return None
    if close_position >= 0.7:
        return "收盘偏强"
    if close_position <= 0.4:
        return "收盘偏弱"
    return "收盘中性"


def classify_upper_shadow(upper_shadow_pct: float | None) -> str | None:
    if upper_shadow_pct is None:
        return None
    if upper_shadow_pct >= 5:
        return "上影线很长，抛压较重"
    if upper_shadow_pct >= 3:
        return "上影线明显"
    return "上影线不明显"


def classify_main_flow(main_net_inflow_ratio: float | None) -> str | None:
    if main_net_inflow_ratio is None:
        return None
    if main_net_inflow_ratio < -15:
        return "流出压力很重"
    if main_net_inflow_ratio < -8:
        return "主力明显流出"
    if main_net_inflow_ratio < -3:
        return "主力偏流出"
    if main_net_inflow_ratio <= 3:
        return "主力资金中性"
    if main_net_inflow_ratio <= 8:
        return "主力偏流入"
    return "主力明显流入"


def select_primary_ratio(
    period_metrics: dict[int, dict[str, float | None]],
) -> tuple[float | None, str | None]:
    candidates = [
        ("10日成交额量比", period_metrics[10]["amount_ratio"]),
        ("10日成交量量比", period_metrics[10]["volume_ratio"]),
        ("20日成交额量比", period_metrics[20]["amount_ratio"]),
        ("20日成交量量比", period_metrics[20]["volume_ratio"]),
    ]
    for label, value in candidates:
        if value is not None:
            return value, label
    return None, None


def build_multi_period_volume_comment(
    period_metrics: dict[int, dict[str, float | None]],
) -> str | None:
    ratio_5 = period_metrics[5]["amount_ratio"]
    ratio_10 = period_metrics[10]["amount_ratio"]
    ratio_20 = period_metrics[20]["amount_ratio"]

    if ratio_5 is None and ratio_10 is None and ratio_20 is None:
        return None
    if None not in (ratio_5, ratio_10, ratio_20):
        assert ratio_5 is not None and ratio_10 is not None and ratio_20 is not None
        if ratio_5 >= 1.5 and ratio_10 >= 1.5 and ratio_20 >= 1.5:
            return "多周期同步放量，说明今日成交相对短中期都明显放大，筹码交换剧烈。"
        if ratio_5 >= 1.5 and ratio_10 < 1.5 and ratio_20 < 1.5:
            return "短线异动放量，相对最近几天明显活跃，但中期异常程度一般。"
        if ratio_5 < 1.5 and ratio_20 >= 1.5:
            return "近期已持续放量，今日相对最近几天不算极端，但相对过去一个月仍处于高成交状态。"
        if ratio_5 < 0.8 and ratio_10 < 0.8 and ratio_20 < 0.8:
            return "多周期缩量，资金参与度下降。"
    return "多周期量能分化，短中期活跃度并不同步，需要结合后续走势确认。"


def classify_volume_price_signal(
    *,
    amount_ratio_10: float | None,
    volume_ratio_10: float | None,
    amount_ratio_20: float | None,
    volume_ratio_20: float | None,
    day_change_pct: float | None,
    close_position: float | None,
    upper_shadow_pct: float | None,
    main_net_inflow_ratio: float | None,
) -> tuple[str, str, list[str]]:
    risk_flags: list[str] = []
    period_metrics = {
        10: {"amount_ratio": amount_ratio_10, "volume_ratio": volume_ratio_10},
        20: {"amount_ratio": amount_ratio_20, "volume_ratio": volume_ratio_20},
    }
    main_ratio, main_ratio_source = select_primary_ratio(period_metrics)

    if main_ratio is None or day_change_pct is None:
        return "数据不足", "历史数据不足，无法判断量价关系", risk_flags

    if main_ratio_source is not None:
        risk_flags.append(f"主判断量比来自{main_ratio_source}")
    if main_ratio >= 2.5:
        risk_flags.append("主量比达到极端放量水平")
    elif main_ratio >= 1.5:
        risk_flags.append("主量比处于明显放量区间")
    if close_position is not None and close_position < 0.4:
        risk_flags.append("收盘位置偏低，尾盘承接偏弱")
    if upper_shadow_pct is not None and upper_shadow_pct >= 5:
        risk_flags.append("上影线很长，抛压较重")
    elif upper_shadow_pct is not None and upper_shadow_pct >= 3:
        risk_flags.append("出现明显上影线，存在冲高回落")
    if main_net_inflow_ratio is not None and main_net_inflow_ratio < -15:
        risk_flags.append("主力资金流出压力很重")
    elif main_net_inflow_ratio is not None and main_net_inflow_ratio <= -8:
        risk_flags.append("主力资金明显流出")

    if (
        main_ratio >= 1.5
        and upper_shadow_pct is not None
        and upper_shadow_pct >= 3
        and main_net_inflow_ratio is not None
        and main_net_inflow_ratio <= -8
    ):
        return "疑似高位派发", "疑似高位派发：放量、长上影、主力明显流出同时出现，需警惕资金借高人气兑现。", risk_flags
    if main_ratio >= 1.5 and day_change_pct >= 5 and close_position is not None and close_position >= 0.7:
        return "强势放量上涨", "强势放量上涨：成交明显放大，股价大涨且收盘靠近高点，承接较强。", risk_flags
    if main_ratio >= 1.2 and day_change_pct >= 3 and close_position is not None and close_position >= 0.7:
        return "温和放量上涨", "温和放量上涨：量能放大，价格同步上涨，走势偏健康。", risk_flags
    if main_ratio >= 1.5 and 0 <= day_change_pct <= 2 and close_position is not None and close_position < 0.6:
        return "放量滞涨", "放量滞涨：成交明显放大，但股价涨幅有限，说明上方抛压较重。", risk_flags
    if main_ratio >= 1.5 and day_change_pct < 0:
        return "放量下跌", "放量下跌：成交明显放大但股价下跌，卖方占优，资金撤退迹象较强。", risk_flags
    if main_ratio <= 0.8 and day_change_pct < 0:
        return "缩量回调", "缩量回调：量能萎缩，股价回调，暂未显示恐慌性抛压。", risk_flags
    if main_ratio <= 0.8 and day_change_pct > 0:
        return "缩量上涨", "缩量上涨：股价上涨但量能不足，可能是锁筹，也可能是跟风不足。", risk_flags
    return "量价中性", "量价关系暂不极端，需要结合后续走势确认", risk_flags


def analyze_volume_signal(
    hist_df: pd.DataFrame,
    *,
    volume_window: int,
    min_volume_ratio: float,
    main_net_inflow_yuan: float | None = None,
) -> dict[str, Any]:
    required = {"日期", "收盘", "成交量"}
    missing = required - set(hist_df.columns)
    if missing:
        raise SystemExit("历史行情缺少字段：" + "、".join(sorted(missing)))

    df = hist_df.copy()
    for column in ["开盘", "最高", "最低", "收盘", "成交量", "成交额", "换手率"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=["收盘", "成交量"]).copy()
    if df.empty:
        raise SystemExit("历史行情中没有可用的成交量数据。")

    open_series = pd.to_numeric(df["开盘"], errors="coerce") if "开盘" in df.columns else None
    high_series = pd.to_numeric(df["最高"], errors="coerce") if "最高" in df.columns else None
    low_series = pd.to_numeric(df["最低"], errors="coerce") if "最低" in df.columns else None
    close = df["收盘"].astype(float)
    volume = df["成交量"].astype(float)
    amount = pd.to_numeric(df["成交额"], errors="coerce") if "成交额" in df.columns else None
    turnover = pd.to_numeric(df["换手率"], errors="coerce") if "换手率" in df.columns else None

    latest_row = df.iloc[-1]
    latest_date = pd.to_datetime(latest_row["日期"]).strftime("%Y-%m-%d")
    current_open = safe_float(open_series.iloc[-1]) if open_series is not None and not open_series.empty else None
    current_high = safe_float(high_series.iloc[-1]) if high_series is not None and not high_series.empty else None
    current_low = safe_float(low_series.iloc[-1]) if low_series is not None and not low_series.empty else None
    current_close = safe_float(close.iloc[-1])
    previous_close = safe_float(close.iloc[-2]) if len(close) >= 2 else None
    current_volume = safe_float(volume.iloc[-1])
    current_amount = safe_float(amount.iloc[-1]) if amount is not None and not amount.empty else None
    current_turnover = safe_float(turnover.iloc[-1]) if turnover is not None and not turnover.empty else None

    period_metrics = calculate_multi_period_ratios(volume, amount)

    volume_ma, volume_ratio = calculate_shifted_average_ratio(volume, current_volume, volume_window)
    amount_ma, amount_ratio = calculate_shifted_average_ratio(amount, current_amount, volume_window)

    turnover_ma = None
    turnover_ratio = None
    if turnover is not None:
        turnover_ma, turnover_ratio = calculate_shifted_average_ratio(turnover, current_turnover, volume_window)

    volume_pct = None
    valid_volume = volume.dropna()
    if current_volume is not None and not valid_volume.empty:
        volume_pct = float((valid_volume <= current_volume).mean() * 100)

    turnover_pct = None
    if turnover is not None:
        valid_turnover = turnover.dropna()
        if current_turnover is not None and not valid_turnover.empty:
            turnover_pct = float((valid_turnover <= current_turnover).mean() * 100)

    day_change_pct = None
    if previous_close not in (None, 0) and current_close is not None:
        day_change_pct = current_close / previous_close * 100 - 100

    close_position = None
    if (
        current_high is not None
        and current_low is not None
        and current_close is not None
        and current_high != current_low
    ):
        close_position = (current_close - current_low) / (current_high - current_low)

    upper_shadow_pct = None
    if (
        current_high is not None
        and current_open is not None
        and current_close is not None
        and previous_close not in (None, 0)
    ):
        upper_shadow_pct = (current_high - max(current_open, current_close)) / previous_close * 100

    main_net_inflow_ratio = None
    if main_net_inflow_yuan is not None and current_amount not in (None, 0):
        main_net_inflow_ratio = main_net_inflow_yuan / current_amount * 100

    main_ratio, main_ratio_source = select_primary_ratio(period_metrics)
    is_expanding, signal_label = classify_volume_level(main_ratio)
    close_position_label = classify_close_position(close_position)
    upper_shadow_label = classify_upper_shadow(upper_shadow_pct)
    main_flow_label = classify_main_flow(main_net_inflow_ratio)
    multi_period_comment = build_multi_period_volume_comment(period_metrics)
    volume_price_label, volume_price_desc, risk_flags = classify_volume_price_signal(
        amount_ratio_10=period_metrics[10]["amount_ratio"],
        volume_ratio_10=period_metrics[10]["volume_ratio"],
        amount_ratio_20=period_metrics[20]["amount_ratio"],
        volume_ratio_20=period_metrics[20]["volume_ratio"],
        day_change_pct=day_change_pct,
        close_position=close_position,
        upper_shadow_pct=upper_shadow_pct,
        main_net_inflow_ratio=main_net_inflow_ratio,
    )
    explanation = []
    for window in MULTI_PERIOD_WINDOWS:
        volume_ratio_value = period_metrics[window]["volume_ratio"]
        amount_ratio_value = period_metrics[window]["amount_ratio"]
        if volume_ratio_value is not None:
            explanation.append(f"{window}日成交量量比={round(volume_ratio_value, 2)}")
        if amount_ratio_value is not None:
            explanation.append(f"{window}日成交额量比={round(amount_ratio_value, 2)}")
    if main_ratio is not None:
        main_ratio_text = f"主判断量比={round(main_ratio, 2)}"
        if main_ratio_source:
            main_ratio_text += f"({main_ratio_source})"
        explanation.append(main_ratio_text)
    if volume_ratio is not None:
        explanation.append(f"量比{volume_window}日均量={round(volume_ratio, 2)}")
    if amount_ratio is not None:
        explanation.append(f"成交额量比{volume_window}日均额={round(amount_ratio, 2)}")
    if close_position is not None:
        explanation.append(f"收盘位置={round(close_position, 2)}")
    if upper_shadow_pct is not None:
        explanation.append(f"上影线比例={round(upper_shadow_pct, 2)}%")
    if volume_pct is not None:
        explanation.append(f"成交量分位={round(volume_pct, 2)}%")
    if turnover_pct is not None:
        explanation.append(f"换手率分位={round(turnover_pct, 2)}%")

    return {
        "分析日期": latest_date,
        "最新开盘价": round_or_none(current_open),
        "最新最高价": round_or_none(current_high),
        "最新最低价": round_or_none(current_low),
        "最新收盘价": round_or_none(current_close),
        "最新涨跌幅%": round_or_none(day_change_pct),
        "今日涨跌幅%": round_or_none(day_change_pct),
        "成交量(手)": round_or_none(current_volume, 0),
        "5日均成交量": round_or_none(period_metrics[5]["volume_ma"], 0),
        "10日均成交量": round_or_none(period_metrics[10]["volume_ma"], 0),
        "20日均成交量": round_or_none(period_metrics[20]["volume_ma"], 0),
        "5日成交量量比": round_or_none(period_metrics[5]["volume_ratio"]),
        "10日成交量量比": round_or_none(period_metrics[10]["volume_ratio"]),
        "20日成交量量比": round_or_none(period_metrics[20]["volume_ratio"]),
        f"{volume_window}日均量(手)": round_or_none(volume_ma, 0),
        f"量比{volume_window}日均量": round_or_none(volume_ratio),
        "成交额(亿)": round_or_none(None if current_amount is None else current_amount / 100_000_000),
        "5日均成交额(亿)": round_or_none(None if period_metrics[5]["amount_ma"] is None else period_metrics[5]["amount_ma"] / 100_000_000),
        "10日均成交额(亿)": round_or_none(None if period_metrics[10]["amount_ma"] is None else period_metrics[10]["amount_ma"] / 100_000_000),
        "20日均成交额(亿)": round_or_none(None if period_metrics[20]["amount_ma"] is None else period_metrics[20]["amount_ma"] / 100_000_000),
        "5日成交额量比": round_or_none(period_metrics[5]["amount_ratio"]),
        "10日成交额量比": round_or_none(period_metrics[10]["amount_ratio"]),
        "20日成交额量比": round_or_none(period_metrics[20]["amount_ratio"]),
        f"{volume_window}日均成交额(亿)": round_or_none(None if amount_ma is None else amount_ma / 100_000_000),
        f"成交额量比{volume_window}日均额": round_or_none(amount_ratio),
        "换手率%": round_or_none(current_turnover),
        f"{volume_window}日均换手率%": round_or_none(turnover_ma),
        f"换手率比{volume_window}日均换手率": round_or_none(turnover_ratio),
        "主判断量比": round_or_none(main_ratio),
        "主判断量比来源": main_ratio_source,
        "收盘位置": round_or_none(close_position),
        "收盘位置解读": close_position_label,
        "上影线比例%": round_or_none(upper_shadow_pct),
        "上影线解读": upper_shadow_label,
        "主力净流入(亿)": round_or_none(None if main_net_inflow_yuan is None else main_net_inflow_yuan / 100_000_000),
        "主力净流入占成交额%": round_or_none(main_net_inflow_ratio),
        "主力资金解读": main_flow_label,
        "成交量分位%": round_or_none(volume_pct),
        "换手率分位%": round_or_none(turnover_pct),
        "是否放量": is_expanding,
        "量能结论": signal_label,
        "多周期量能解读": multi_period_comment,
        "量价结论": volume_price_label,
        "量价解读": volume_price_desc,
        "风险信号": risk_flags,
        "量能说明": "；".join(explanation) if explanation else None,
    }


def build_result(
    *,
    code: str,
    name: str,
    history_source: str,
    analysis_mode: str,
    realtime_source: str | None,
    realtime_time: str | None,
    target_date: str | None,
    history_days: int,
    volume_window: int,
    min_volume_ratio: float,
    signal: dict[str, Any],
) -> dict[str, Any]:
    analysis_params: dict[str, Any] = {
        "历史回看天数": history_days,
        "均量窗口": volume_window,
        "放量阈值": round_or_none(min_volume_ratio),
        "分析数据模式": analysis_mode,
    }
    if target_date is not None:
        analysis_params["目标日期"] = target_date
    if realtime_source is not None:
        analysis_params["实时行情来源"] = realtime_source
    if realtime_time is not None:
        analysis_params["实时行情时间"] = realtime_time
    return {
        "代码": code,
        "名称": name,
        "历史行情来源": history_source,
        "分析参数": analysis_params,
        "放量相关数据": signal,
    }


def write_output(result: dict[str, Any], output_path: Path | None) -> None:
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    if output_path is None:
        print(payload)
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(payload + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    if args.history_days <= 0:
        raise SystemExit("--history-days 必须大于 0")
    if args.volume_window <= 0:
        raise SystemExit("--volume-window 必须大于 0")
    if args.min_volume_ratio <= 0:
        raise SystemExit("--min-volume-ratio 必须大于 0")

    target_date = parse_target_date(args.date) if args.date else None
    code, name = resolve_stock_identity(args.code)
    ak = stock_info_module.require_dependencies()
    hist_df, history_source = fetch_history_df(
        ak,
        code,
        args.history_days,
        args.adjust,
        args.history_source,
        target_date=target_date,
    )
    hist_df, analysis_mode, realtime_source, realtime_time = maybe_attach_realtime_row(
        hist_df,
        code=code,
        name=name,
        target_date=target_date,
    )
    main_net_inflow_yuan = args.main_net_inflow_yuan
    if args.main_net_inflow_yi is not None:
        main_net_inflow_yuan = args.main_net_inflow_yi * 100_000_000

    signal = analyze_volume_signal(
        hist_df,
        volume_window=args.volume_window,
        min_volume_ratio=args.min_volume_ratio,
        main_net_inflow_yuan=main_net_inflow_yuan,
    )
    output_path = Path(args.output) if args.output else None
    result = build_result(
        code=code,
        name=name,
        history_source=history_source,
        analysis_mode=analysis_mode,
        realtime_source=realtime_source,
        realtime_time=realtime_time,
        target_date=target_date.strftime("%Y-%m-%d") if target_date is not None else None,
        history_days=args.history_days,
        volume_window=args.volume_window,
        min_volume_ratio=args.min_volume_ratio,
        signal=signal,
    )
    write_output(result, output_path)


if __name__ == "__main__":
    main()
