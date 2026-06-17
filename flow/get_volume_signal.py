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
from datetime import datetime, timedelta
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "输入单只股票代码或名称，输出放量/量价关系 JSON。"
            "包括量比、成交额量比、收盘位置、上影线、换手率分位和量价结论。"
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
    end_dt = target_date or datetime.now()
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


def classify_volume_signal(volume_ratio: float | None, min_volume_ratio: float) -> tuple[bool | None, str]:
    """只判断量能本身：有没有放量。"""
    if volume_ratio is None:
        return None, "历史数据不足，无法判断"
    if volume_ratio >= max(min_volume_ratio, 1.8):
        return True, "明显放量"
    if volume_ratio >= min_volume_ratio:
        return True, "温和放量"
    if volume_ratio <= 0.8:
        return False, "缩量"
    return False, "量能平稳"


def classify_volume_price_signal(
    *,
    volume_ratio: float | None,
    amount_ratio: float | None,
    day_change_pct: float | None,
    close_position: float | None,
    upper_shadow_pct: float | None,
    turnover_ratio: float | None,
    main_net_inflow_ratio: float | None,
    min_volume_ratio: float,
) -> tuple[str, str, list[str]]:
    """综合量、价、K线位置、换手和主力资金，判断放量性质。

    重点不是“有没有放量”，而是“放量之后价格有没有被推上去”。
    """
    risk_flags: list[str] = []

    if volume_ratio is None or day_change_pct is None:
        return "数据不足", "历史数据不足，无法判断量价关系", risk_flags

    is_heavy_volume = volume_ratio >= max(min_volume_ratio, 1.8)
    is_mild_volume = volume_ratio >= min_volume_ratio
    is_low_volume = volume_ratio <= 0.8
    heavy_amount = amount_ratio is not None and amount_ratio >= max(min_volume_ratio, 1.8)
    mild_amount = amount_ratio is not None and amount_ratio >= min_volume_ratio

    close_strong = close_position is not None and close_position >= 0.70
    close_weak = close_position is not None and close_position <= 0.40
    long_upper_shadow = upper_shadow_pct is not None and upper_shadow_pct >= 3.0
    high_turnover = turnover_ratio is not None and turnover_ratio >= 1.8
    main_outflow = main_net_inflow_ratio is not None and main_net_inflow_ratio <= -8.0

    if is_heavy_volume:
        risk_flags.append("成交量明显高于均值")
    if heavy_amount:
        risk_flags.append("成交额明显高于均值")
    elif mild_amount:
        risk_flags.append("成交额高于均值")
    if long_upper_shadow:
        risk_flags.append("出现明显上影线，存在冲高回落")
    if close_weak:
        risk_flags.append("收盘位置偏低，承接偏弱")
    if high_turnover:
        risk_flags.append("换手率明显高于均值，筹码交换剧烈")
    if main_outflow:
        risk_flags.append("主力净流出占成交额比例较高")

    if is_heavy_volume and day_change_pct >= 5 and close_strong and not main_outflow:
        return "强势放量上涨", "放量后价格被有效推升，且收盘靠近高点，承接较强", risk_flags

    if is_heavy_volume and day_change_pct < 0:
        return "放量下跌", "成交明显放大但股价下跌，资金撤退迹象较强", risk_flags

    if is_heavy_volume and 0 <= day_change_pct <= 2 and (close_weak or main_outflow):
        return "放量滞涨", "成交明显放大，但涨幅有限且承接不强，存在高位派发嫌疑", risk_flags

    if is_heavy_volume and long_upper_shadow and (main_outflow or close_weak):
        return "疑似高位派发", "放量、上影线、收盘偏弱或主力流出同时出现，需要重点警惕", risk_flags

    if is_mild_volume and day_change_pct >= 3 and close_strong:
        return "温和放量上涨", "量能放大，价格同步上涨，走势相对健康", risk_flags

    if is_low_volume and day_change_pct < 0:
        return "缩量回调", "量能萎缩且股价回调，暂未显示恐慌性抛压", risk_flags

    if is_low_volume and day_change_pct > 0:
        return "缩量上涨", "股价上涨但量能不足，可能是锁筹，也可能是跟风不足", risk_flags

    if is_heavy_volume and day_change_pct > 0 and main_outflow:
        return "放量分歧上涨", "股价上涨但主力资金流出，说明老资金兑现与新资金承接同时存在", risk_flags

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

    volume_ma = None
    volume_ratio = None
    if len(volume) >= volume_window + 1:
        volume_ma = safe_float(volume.rolling(volume_window).mean().shift(1).iloc[-1])
        if volume_ma not in (None, 0) and current_volume is not None:
            volume_ratio = current_volume / volume_ma

    amount_ma = None
    amount_ratio = None
    if amount is not None and len(amount.dropna()) >= volume_window + 1:
        amount_ma = safe_float(amount.rolling(volume_window).mean().shift(1).iloc[-1])
        if amount_ma not in (None, 0) and current_amount is not None:
            amount_ratio = current_amount / amount_ma

    turnover_ma = None
    turnover_ratio = None
    if turnover is not None and len(turnover.dropna()) >= volume_window + 1:
        turnover_ma = safe_float(turnover.rolling(volume_window).mean().shift(1).iloc[-1])
        if turnover_ma not in (None, 0) and current_turnover is not None:
            turnover_ratio = current_turnover / turnover_ma

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

    is_expanding, signal_label = classify_volume_signal(volume_ratio, min_volume_ratio)
    volume_price_label, volume_price_desc, risk_flags = classify_volume_price_signal(
        volume_ratio=volume_ratio,
        amount_ratio=amount_ratio,
        day_change_pct=day_change_pct,
        close_position=close_position,
        upper_shadow_pct=upper_shadow_pct,
        turnover_ratio=turnover_ratio,
        main_net_inflow_ratio=main_net_inflow_ratio,
        min_volume_ratio=min_volume_ratio,
    )
    explanation = []
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
        "成交量(手)": round_or_none(current_volume, 0),
        f"{volume_window}日均量(手)": round_or_none(volume_ma, 0),
        f"量比{volume_window}日均量": round_or_none(volume_ratio),
        "成交额(亿)": round_or_none(None if current_amount is None else current_amount / 100_000_000),
        f"{volume_window}日均成交额(亿)": round_or_none(None if amount_ma is None else amount_ma / 100_000_000),
        f"成交额量比{volume_window}日均额": round_or_none(amount_ratio),
        "换手率%": round_or_none(current_turnover),
        f"{volume_window}日均换手率%": round_or_none(turnover_ma),
        f"换手率比{volume_window}日均换手率": round_or_none(turnover_ratio),
        "收盘位置": round_or_none(close_position),
        "上影线比例%": round_or_none(upper_shadow_pct),
        "主力净流入(亿)": round_or_none(None if main_net_inflow_yuan is None else main_net_inflow_yuan / 100_000_000),
        "主力净流入占成交额%": round_or_none(main_net_inflow_ratio),
        "成交量分位%": round_or_none(volume_pct),
        "换手率分位%": round_or_none(turnover_pct),
        "是否放量": is_expanding,
        "量能结论": signal_label,
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
    }
    if target_date is not None:
        analysis_params["目标日期"] = target_date
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
        target_date=target_date.strftime("%Y-%m-%d") if target_date is not None else None,
        history_days=args.history_days,
        volume_window=args.volume_window,
        min_volume_ratio=args.min_volume_ratio,
        signal=signal,
    )
    write_output(result, output_path)


if __name__ == "__main__":
    main()
