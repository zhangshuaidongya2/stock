#!/usr/bin/env python3
"""Fetch A-share stock information triggered by market changes.

Default run:
  python stock_change_info.py

Override examples:
  python stock_change_info.py --symbols 000001,002342,600519 --history-days 30
  python stock_change_info.py --quote-source auto --history-source auto
  python stock_change_info.py --min-change 5 --top 20 --format table
  python stock_change_info.py --min-change 4 --output result.csv --format csv
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


pd = None


REQUEST_TIMEOUT = 10
FUND_FLOW_RETRY_TIMES = 2
SINA_PAGE_SIZE = 80
SINA_SPOT_COUNT_URL = (
    "http://vip.stock.finance.sina.com.cn/quotes_service/api/"
    "json_v2.php/Market_Center.getHQNodeStockCount?node=hs_a"
)
SINA_SPOT_URL = (
    "http://vip.stock.finance.sina.com.cn/quotes_service/api/"
    "json_v2.php/Market_Center.getHQNodeData"
)
EASTMONEY_FUND_FLOW_URL = "https://push2.eastmoney.com/api/qt/ulist.np/get"
SYMBOL_CACHE_PATH = Path(__file__).with_name("stock_symbols_cache.json")

# 命令行未传参时使用的默认配置。
DEFAULT_RUN_CONFIG = {
    # 要查询的股票（代码或名称，多个用逗号分隔）。
    "symbols": "002342",
    # 实时行情数据源：auto / eastmoney / tencent / sina。
    "quote_source": "tencent",
    # 历史行情数据源：auto / eastmoney / sina。
    "history_source": "sina",
    # 历史摘要天数；0 表示不查询历史行情。
    "history_days": 5,
    # 输出条数上限；0 表示不过滤数量。
    "top": 0,
    # 输出格式：table / json / csv。
    "output_format": "json",
    # 个股详情请求间隔秒数。
    "delay": 0.0,
    # 是否补充主力资金流数据。
    "fund_flow": True,
    # 主力资金流模式：realtime / daily / auto。
    "fund_flow_mode": "realtime",
    # 是否计算支撑位/压力位。
    "support_resistance": True,
    # 支撑位/压力位分析回看天数。
    "sr_days": 120,
    # 支撑位/压力位返回层数。
    "sr_levels": 3,
    # 局部高低点识别窗口（交易日）。
    "sr_pivot_window": 3,
    # 是否计算扩展技术因子。
    "advanced_factors": True,
    # 扩展技术因子分析回看天数。
    "factor_days": 120,
}


NUMERIC_COLUMNS = [
    "最新价",
    "涨跌幅",
    "涨跌额",
    "成交量",
    "成交额",
    "振幅",
    "最高",
    "最低",
    "今开",
    "昨收",
    "量比",
    "换手率",
    "市盈率-动态",
    "市净率",
    "总市值",
    "流通市值",
    "涨速",
    "5分钟涨跌",
    "60日涨跌幅",
    "年初至今涨跌幅",
    "主力净流入",
    "主力净占比",
    "超大单净流入",
    "大单净流入",
    "中单净流入",
    "小单净流入",
]


DEFAULT_TABLE_COLUMNS = [
    "代码",
    "名称",
    "变化信号",
    "最新价",
    "涨跌幅%",
    "涨速%",
    "5分钟涨跌%",
    "主力净流入(万)",
    "主力净占比%",
    "成交额(亿)",
    "换手率%",
    "量比",
    "行情来源",
    "历史来源",
    "历史区间",
    "历史天数",
    "均线趋势",
    "20日突破信号",
    "RSI14",
    "ATR14%",
    "成交量分位%",
    "支撑位1",
    "压力位1",
    "总市值(亿)",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "用 AKShare 获取沪深京 A 股实时行情，并根据股票变化条件补充个股相关信息。"
        )
    )
    parser.add_argument(
        "--symbols",
        default=DEFAULT_RUN_CONFIG["symbols"],
        help="只查询指定股票，支持代码或名称，多个值用逗号分隔；不传则扫描全市场。",
    )
    parser.add_argument(
        "--quote-source",
        choices=["auto", "eastmoney", "tencent", "sina"],
        default=DEFAULT_RUN_CONFIG["quote_source"],
        help="实时行情数据源：auto=自动兜底，eastmoney=东方财富，tencent=腾讯，sina=新浪。",
    )
    parser.add_argument(
        "--direction",
        choices=["up", "down", "both"],
        default="both",
        help="涨跌方向过滤：up=上涨，down=下跌，both=双向。",
    )
    parser.add_argument(
        "--min-change",
        type=float,
        default=5.0,
        help="最小涨跌幅阈值，单位百分比；both 模式取绝对值。",
    )
    parser.add_argument(
        "--min-speed",
        type=float,
        help="最小涨速阈值，单位百分比；both 模式取绝对值。",
    )
    parser.add_argument(
        "--min-5m-change",
        type=float,
        help="最小 5 分钟涨跌阈值，单位百分比；both 模式取绝对值。",
    )
    parser.add_argument("--min-turnover", type=float, help="最小换手率，单位百分比。")
    parser.add_argument("--min-volume-ratio", type=float, help="最小量比。")
    parser.add_argument(
        "--min-amount-yi",
        type=float,
        help="最小成交额，单位亿元。",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=DEFAULT_RUN_CONFIG["top"],
        help="最多输出多少只股票。传 0 表示不过滤数量。",
    )
    parser.add_argument(
        "--sort-by",
        default="涨跌幅",
        help="排序字段，默认按涨跌幅排序，可用字段包括涨速、5分钟涨跌、成交额、换手率等。",
    )
    parser.add_argument(
        "--ascending",
        action="store_true",
        help="升序排序，默认降序。",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=DEFAULT_RUN_CONFIG["history_days"],
        help="补充最近 N 个交易日历史摘要，传 0 表示不查询历史行情。",
    )
    parser.add_argument(
        "--history-source",
        choices=["auto", "eastmoney", "sina"],
        default=DEFAULT_RUN_CONFIG["history_source"],
        help="历史行情数据源：auto=自动兜底，eastmoney=东方财富，sina=新浪。",
    )
    parser.add_argument(
        "--adjust",
        choices=["", "qfq", "hfq"],
        default="qfq",
        help="历史行情复权方式：空字符串=不复权，qfq=前复权，hfq=后复权。",
    )
    parser.add_argument(
        "--financial",
        action="store_true",
        help="尝试补充最近一期财务指标。该接口较慢，默认关闭。",
    )
    parser.add_argument(
        "--fund-flow",
        action="store_true",
        default=DEFAULT_RUN_CONFIG["fund_flow"],
        help="补充主力资金流入流出数据。该接口较慢，默认开启。",
    )
    parser.add_argument(
        "--no-fund-flow",
        action="store_false",
        dest="fund_flow",
        help="不获取主力资金流数据。",
    )
    parser.add_argument(
        "--fund-flow-mode",
        choices=["realtime", "daily", "auto"],
        default=DEFAULT_RUN_CONFIG["fund_flow_mode"],
        help=(
            "资金流模式：realtime=仅实时(默认)；"
            "daily=仅最近交易日日线；auto=实时失败时回退日线。"
        ),
    )
    parser.add_argument(
        "--support-resistance",
        action="store_true",
        default=DEFAULT_RUN_CONFIG["support_resistance"],
        help="计算支撑位和压力位，默认开启。",
    )
    parser.add_argument(
        "--no-support-resistance",
        action="store_false",
        dest="support_resistance",
        help="不计算支撑位和压力位。",
    )
    parser.add_argument(
        "--sr-days",
        type=int,
        default=DEFAULT_RUN_CONFIG["sr_days"],
        help="支撑位/压力位分析回看天数，默认 120。",
    )
    parser.add_argument(
        "--sr-levels",
        type=int,
        default=DEFAULT_RUN_CONFIG["sr_levels"],
        help="返回多少层支撑位/压力位，默认 3。",
    )
    parser.add_argument(
        "--sr-pivot-window",
        type=int,
        default=DEFAULT_RUN_CONFIG["sr_pivot_window"],
        help="局部高低点识别窗口（交易日），默认 3。",
    )
    parser.add_argument(
        "--advanced-factors",
        action="store_true",
        default=DEFAULT_RUN_CONFIG["advanced_factors"],
        help="计算扩展技术因子（均线、RSI、ATR、量能分位等），默认开启。",
    )
    parser.add_argument(
        "--no-advanced-factors",
        action="store_false",
        dest="advanced_factors",
        help="不计算扩展技术因子。",
    )
    parser.add_argument(
        "--factor-days",
        type=int,
        default=DEFAULT_RUN_CONFIG["factor_days"],
        help="扩展技术因子分析回看天数，默认 120。",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_RUN_CONFIG["delay"],
        help="个股详情接口之间的等待秒数，降低被数据源限流概率。",
    )
    parser.add_argument(
        "--format",
        choices=["table", "json", "csv"],
        default=DEFAULT_RUN_CONFIG["output_format"],
        help="输出格式。",
    )
    parser.add_argument("--output", help="输出文件路径；不传则打印到终端。")
    return parser.parse_args()


def require_dependencies():
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


def normalize_code(value: str) -> str:
    code = str(value).strip().lower()
    for prefix in ("sh", "sz", "bj"):
        if code.startswith(prefix):
            code = code[len(prefix) :]
    return "".join(ch for ch in code if ch.isdigit()).zfill(6)[-6:]


def to_sina_symbol(code: str) -> str:
    normalized = normalize_code(code)
    if normalized.startswith("6"):
        return f"sh{normalized}"
    if normalized.startswith(("4", "8", "9")):
        return f"bj{normalized}"
    return f"sz{normalized}"


def signed_filter(series: Any, direction: str, threshold: float) -> Any:
    if direction == "up":
        return series >= threshold
    if direction == "down":
        return series <= -threshold
    return series.abs() >= threshold


def fetch_spot_quotes(
    ak: Any,
    symbols: str | None = None,
    source: str = "auto",
) -> Any:
    if source == "eastmoney":
        return fetch_eastmoney_spot_quotes(ak)
    if source == "tencent":
        return coerce_numeric_columns(fetch_tencent_spot_quotes(symbols or ""))
    if source == "sina":
        try:
            return normalize_sina_spot_quotes(fetch_sina_spot_quotes())
        except Exception:
            return normalize_sina_spot_quotes(fetch_akshare_sina_spot_quotes(ak))

    errors = []
    tried_tencent = False

    try:
        return fetch_eastmoney_spot_quotes(ak)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"东方财富: {compact_error(exc)}")

    if symbols and symbols_are_codes(symbols):
        tried_tencent = True
        try:
            spot_df = fetch_tencent_spot_quotes(symbols)
            if spot_df is not None and not spot_df.empty:
                print(
                    "提示：东方财富实时行情不可用，已切换到腾讯行情。",
                    file=sys.stderr,
                )
                return coerce_numeric_columns(spot_df)
            errors.append("腾讯: 返回空数据")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"腾讯: {compact_error(exc)}")

    try:
        spot_df = fetch_sina_spot_quotes()
    except Exception as exc:  # noqa: BLE001
        errors.append(f"新浪直接请求: {compact_error(exc)}")
        try:
            spot_df = fetch_akshare_sina_spot_quotes(ak)
        except Exception as ak_sina_exc:  # noqa: BLE001
            errors.append(f"AKShare 新浪: {compact_error(ak_sina_exc)}")
            spot_df = None

    try:
        if spot_df is not None and not spot_df.empty:
            print(
                "提示：东方财富实时行情不可用，已切换到新浪行情；"
                "涨速、5分钟涨跌、量比、市值等字段可能为空。",
                file=sys.stderr,
            )
            return normalize_sina_spot_quotes(spot_df)
        errors.append("新浪: 返回空数据")
    except Exception as exc:  # noqa: BLE001
        errors.append(f"新浪: {compact_error(exc)}")

    if symbols and not tried_tencent:
        try:
            spot_df = fetch_tencent_spot_quotes(symbols)
            if spot_df is not None and not spot_df.empty:
                print(
                    "提示：东方财富和新浪实时行情不可用，已切换到腾讯行情。",
                    file=sys.stderr,
                )
                return coerce_numeric_columns(spot_df)
            errors.append("腾讯: 返回空数据")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"腾讯: {compact_error(exc)}")

    raise RuntimeError(f"获取实时行情失败。错误：{'；'.join(errors)}")


def fetch_eastmoney_spot_quotes(ak: Any) -> Any:
    spot_df = ak.stock_zh_a_spot_em()
    if spot_df is None or spot_df.empty:
        raise RuntimeError("东方财富返回空实时行情")
    spot_df = spot_df.copy()
    spot_df["行情来源"] = "东方财富"
    return coerce_numeric_columns(spot_df)


def symbols_are_codes(symbols: str) -> bool:
    tokens = [item.strip() for item in symbols.split(",") if item.strip()]
    return bool(tokens) and all(any(ch.isdigit() for ch in token) for token in tokens)


def resolve_symbol_tokens(symbols: str) -> tuple[list[str], list[str]]:
    tokens = [item.strip() for item in symbols.split(",") if item.strip()]
    resolved_codes = []
    unresolved_names = []

    name_map = None
    for token in tokens:
        if any(ch.isdigit() for ch in token):
            resolved_codes.append(normalize_code(token))
            continue

        if name_map is None:
            name_map = load_symbol_name_map()
        code = name_map.get(token)
        if code:
            resolved_codes.append(code)
        else:
            unresolved_names.append(token)

    return resolved_codes, unresolved_names


def load_symbol_name_map() -> dict[str, str]:
    cached_map = read_symbol_cache()
    if cached_map:
        return cached_map

    spot_df = fetch_sina_spot_quotes()
    if spot_df is None or spot_df.empty:
        raise RuntimeError("无法获取股票名称映射")

    name_map = {}
    for _, row in spot_df.iterrows():
        name = str(row.get("名称", "")).strip()
        code = normalize_code(row.get("代码", ""))
        if name and code:
            name_map[name] = code

    if name_map:
        write_symbol_cache(name_map)
    return name_map


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
        str(name): normalize_code(code)
        for name, code in payload.items()
        if str(name).strip() and normalize_code(code)
    }


def write_symbol_cache(name_map: dict[str, str]) -> None:
    try:
        SYMBOL_CACHE_PATH.write_text(
            json.dumps(name_map, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError:
        pass


def normalize_sina_spot_quotes(df: Any) -> Any:
    df = df.copy()
    df["行情来源"] = "新浪"

    for col in NUMERIC_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    if "代码" in df.columns:
        df["代码"] = df["代码"].map(normalize_code)

    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 新浪 A 股实时行情返回的成交量单位是股；脚本内部统一使用手。
    if "成交量" in df.columns:
        df["成交量"] = df["成交量"] / 100
    # 新浪 A 股实时行情返回的市值单位是万元；脚本内部统一使用元。
    for col in ("总市值", "流通市值"):
        if col in df.columns:
            df[col] = df[col] * 10_000
    return df


def fetch_sina_spot_quotes() -> Any:
    import requests

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            )
        }
    )

    count_response = session.get(SINA_SPOT_COUNT_URL, timeout=REQUEST_TIMEOUT)
    count_response.raise_for_status()
    total_digits = "".join(ch for ch in count_response.text if ch.isdigit())
    if not total_digits:
        raise RuntimeError(f"新浪未返回股票总数：{count_response.text[:120]}")

    total = int(total_digits)
    page_count = (total + SINA_PAGE_SIZE - 1) // SINA_PAGE_SIZE
    records = []

    for page in range(1, page_count + 1):
        params = {
            "page": page,
            "num": SINA_PAGE_SIZE,
            "sort": "symbol",
            "asc": "1",
            "node": "hs_a",
            "symbol": "",
            "_s_r_a": "page",
        }
        response = session.get(SINA_SPOT_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        page_records = json.loads(response.text)
        if not isinstance(page_records, list):
            raise RuntimeError(f"新浪第 {page} 页返回格式异常：{response.text[:120]}")
        records.extend(page_records)
        if page < page_count:
            time.sleep(0.35)

    if not records:
        raise RuntimeError("新浪返回空行情数据")

    df = pd.DataFrame(records)
    df.rename(
        columns={
            "code": "代码",
            "name": "名称",
            "trade": "最新价",
            "pricechange": "涨跌额",
            "changepercent": "涨跌幅",
            "settlement": "昨收",
            "open": "今开",
            "high": "最高",
            "low": "最低",
            "volume": "成交量",
            "amount": "成交额",
            "per": "市盈率-动态",
            "pb": "市净率",
            "mktcap": "总市值",
            "nmc": "流通市值",
            "turnoverratio": "换手率",
        },
        inplace=True,
    )
    return df


def fetch_akshare_sina_spot_quotes(ak: Any) -> Any:
    try:
        import akshare.stock.stock_zh_a_sina as sina_module  # type: ignore
    except Exception:  # noqa: BLE001
        return ak.stock_zh_a_spot()

    original_get_tqdm = sina_module.get_tqdm
    sina_module.get_tqdm = lambda enable=True: (lambda iterable, *args, **kwargs: iterable)
    try:
        return ak.stock_zh_a_spot()
    finally:
        sina_module.get_tqdm = original_get_tqdm


def fetch_tencent_spot_quotes(symbols: str) -> Any:
    import requests

    resolved_codes, unresolved_names = resolve_symbol_tokens(symbols)
    query_symbols = [to_tencent_symbol(code) for code in resolved_codes]
    if not query_symbols:
        raise RuntimeError("未能解析出可查询的股票代码")

    response = requests.get(
        "https://qt.gtimg.cn/q=" + ",".join(query_symbols),
        timeout=REQUEST_TIMEOUT,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    response.raise_for_status()
    text = response.content.decode("gbk", errors="ignore")
    records = []
    for line in text.splitlines():
        if '="' not in line:
            continue
        payload = line.split('="', 1)[1].rsplit('"', 1)[0]
        parts = payload.split("~")
        if len(parts) < 58 or not parts[2]:
            continue
        records.append(
            {
                "代码": parts[2],
                "名称": parts[1],
                "最新价": parts[3],
                "涨跌幅": parts[32],
                "涨跌额": parts[31],
                "成交量": parts[36],
                "成交额": safe_float(parts[57]) * 10_000
                if safe_float(parts[57]) is not None
                else None,
                "振幅": parts[43],
                "最高": parts[33],
                "最低": parts[34],
                "今开": parts[5],
                "昨收": parts[4],
                "换手率": parts[38],
                "市盈率-动态": parts[39],
                "市净率": parts[46],
                "总市值": safe_float(parts[45]) * 100_000_000
                if safe_float(parts[45]) is not None
                else None,
                "流通市值": safe_float(parts[44]) * 100_000_000
                if safe_float(parts[44]) is not None
                else None,
                "行情来源": "腾讯",
            }
        )

    if unresolved_names:
        print(f"提示：未找到这些股票名称：{','.join(unresolved_names)}", file=sys.stderr)
    if not records:
        raise RuntimeError("腾讯未返回可解析行情")
    return pd.DataFrame(records)


def to_tencent_symbol(code: str) -> str:
    normalized = normalize_code(code)
    if normalized.startswith("6"):
        return f"sh{normalized}"
    if normalized.startswith(("4", "8", "9")):
        return f"bj{normalized}"
    return f"sz{normalized}"


def compact_error(exc: Exception) -> str:
    message = f"{type(exc).__name__}: {exc}"
    return message if len(message) <= 220 else f"{message[:217]}..."


def coerce_numeric_columns(df: Any) -> Any:
    df = df.copy()
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "代码" in df.columns:
        df["代码"] = df["代码"].map(normalize_code)
    return df


def filter_by_symbols(df: Any, symbols: str) -> Any:
    tokens = [item.strip() for item in symbols.split(",") if item.strip()]
    if not tokens:
        return df

    mask = pd.Series(False, index=df.index)
    name_map = None
    for token in tokens:
        if any(ch.isdigit() for ch in token):
            mask |= df["代码"].astype(str).str.zfill(6) == normalize_code(token)
        else:
            name_mask = df["名称"].astype(str).str.contains(token, case=False, na=False)
            if name_mask.any():
                mask |= name_mask
                continue
            if name_map is None:
                name_map = read_symbol_cache()
            code = name_map.get(token, "")
            if code:
                mask |= df["代码"].astype(str).str.zfill(6) == code
    return df[mask].copy()


def filter_by_change(df: Any, args: argparse.Namespace) -> Any:
    if args.symbols:
        return filter_by_symbols(df, args.symbols)

    masks = []
    if args.min_change is not None and "涨跌幅" in df.columns:
        masks.append(signed_filter(df["涨跌幅"], args.direction, args.min_change))
    if args.min_speed is not None and "涨速" in df.columns:
        masks.append(signed_filter(df["涨速"], args.direction, args.min_speed))
    if args.min_5m_change is not None and "5分钟涨跌" in df.columns:
        masks.append(signed_filter(df["5分钟涨跌"], args.direction, args.min_5m_change))
    if args.min_amount_yi is not None and "成交额" in df.columns:
        masks.append(df["成交额"] >= args.min_amount_yi * 100_000_000)
    if args.min_turnover is not None and "换手率" in df.columns:
        masks.append(df["换手率"] >= args.min_turnover)
    if args.min_volume_ratio is not None and "量比" in df.columns:
        masks.append(df["量比"] >= args.min_volume_ratio)

    if not masks:
        return df.copy()

    combined = masks[0]
    for mask in masks[1:]:
        combined &= mask
    return df[combined].copy()


def sort_and_limit(df: Any, args: argparse.Namespace) -> Any:
    if df.empty:
        return df

    sort_col = args.sort_by
    if sort_col not in df.columns:
        candidates = ", ".join(str(col) for col in df.columns)
        raise SystemExit(f"排序字段不存在：{sort_col}\n可用字段：{candidates}")

    sorted_df = df.sort_values(sort_col, ascending=args.ascending, na_position="last")
    if args.top and args.top > 0:
        sorted_df = sorted_df.head(args.top)
    return sorted_df


def build_signal(row: Any, args: argparse.Namespace) -> str:
    signals = []
    add_signed_signal(signals, row, "涨跌幅", args.min_change, args.direction)
    add_signed_signal(signals, row, "涨速", args.min_speed, args.direction)
    add_signed_signal(signals, row, "5分钟涨跌", args.min_5m_change, args.direction)

    amount = safe_float(row.get("成交额"))
    if args.min_amount_yi is not None and amount is not None:
        if amount >= args.min_amount_yi * 100_000_000:
            signals.append(f"成交额>={args.min_amount_yi:g}亿")

    turnover = safe_float(row.get("换手率"))
    if args.min_turnover is not None and turnover is not None:
        if turnover >= args.min_turnover:
            signals.append(f"换手率>={args.min_turnover:g}%")

    volume_ratio = safe_float(row.get("量比"))
    if args.min_volume_ratio is not None and volume_ratio is not None:
        if volume_ratio >= args.min_volume_ratio:
            signals.append(f"量比>={args.min_volume_ratio:g}")

    return "；".join(signals) if signals else "指定股票"


def add_signed_signal(
    signals: list[str],
    row: Any,
    field: str,
    threshold: float | None,
    direction: str,
) -> None:
    if threshold is None:
        return
    value = safe_float(row.get(field))
    if value is None:
        return
    if direction == "up" and value >= threshold:
        signals.append(f"{field}>={threshold:g}%")
    elif direction == "down" and value <= -threshold:
        signals.append(f"{field}<={-threshold:g}%")
    elif direction == "both" and abs(value) >= threshold:
        signals.append(f"|{field}|>={threshold:g}%")


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(converted):
        return None
    return converted


def round_or_none(value: Any, digits: int = 2) -> float | None:
    converted = safe_float(value)
    if converted is None:
        return None
    return round(converted, digits)


def scale_or_none(value: Any, divisor: float, digits: int = 2) -> float | None:
    converted = safe_float(value)
    if converted is None:
        return None
    return round(converted / divisor, digits)


def fetch_history_summary(
    ak: Any,
    code: str,
    days: int,
    adjust: str,
    source: str = "auto",
    include_sr: bool = False,
    sr_days: int = 120,
    sr_levels: int = 3,
    sr_pivot_window: int = 3,
    include_factors: bool = False,
    factor_days: int = 120,
) -> dict[str, Any]:
    need_summary = days > 0
    need_sr = include_sr and sr_days > 0 and sr_levels > 0
    need_factors = include_factors and factor_days > 0
    if not need_summary and not need_sr and not need_factors:
        return {}

    lookback_days = max(
        days if days > 0 else 0,
        sr_days if need_sr else 0,
        factor_days if need_factors else 0,
    )
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=max(40, lookback_days * 3))).strftime(
        "%Y%m%d"
    )
    errors = []

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
                timeout=REQUEST_TIMEOUT,
            )
            resolved_source = "东方财富"
        except Exception as exc:  # noqa: BLE001
            hist_df = None
            errors.append(f"东方财富: {compact_error(exc)}")

    if hist_df is None or hist_df.empty:
        if source == "eastmoney":
            return {"历史行情错误": "；".join(errors)} if errors else {}
        try:
            hist_df = fetch_sina_history(ak, code, start_date, end_date, adjust)
            if source == "auto":
                print(
                    f"提示：{code} 东方财富历史行情不可用，已切换到新浪日线。",
                    file=sys.stderr,
                )
            resolved_source = "新浪"
        except Exception as exc:  # noqa: BLE001
            errors.append(f"新浪: {compact_error(exc)}")
            return {"历史行情错误": "；".join(errors)} if errors else {}

    hist_df = hist_df.copy()
    for col in ["开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅", "换手率"]:
        if col in hist_df.columns:
            hist_df[col] = pd.to_numeric(hist_df[col], errors="coerce")

    summary = {}
    if need_summary:
        tail = hist_df.tail(days)
        if not tail.empty and "收盘" in tail.columns:
            first_close = safe_float(tail["收盘"].iloc[0])
            last_close = safe_float(tail["收盘"].iloc[-1])
            period_change = None
            if first_close not in (None, 0) and last_close is not None:
                period_change = (last_close / first_close - 1) * 100

            highest = safe_float(tail["最高"].max()) if "最高" in tail.columns else None
            lowest = safe_float(tail["最低"].min()) if "最低" in tail.columns else None
            amplitude = None
            if lowest not in (None, 0) and highest is not None:
                amplitude = (highest / lowest - 1) * 100

            label = f"近{len(tail)}日"
            summary.update(
                {
                    "历史来源": resolved_source,
                    "历史区间": f"{tail['日期'].iloc[0]} ~ {tail['日期'].iloc[-1]}"
                    if "日期" in tail.columns
                    else None,
                    "历史天数": len(tail),
                    f"{label}涨跌幅%": round_or_none(period_change),
                    f"{label}振幅%": round_or_none(amplitude),
                    f"{label}最高": round_or_none(highest),
                    f"{label}最低": round_or_none(lowest),
                }
            )
            if "成交额" in tail.columns:
                summary[f"{label}日均成交额(亿)"] = round_or_none(
                    tail["成交额"].mean() / 100_000_000
                )
            if "换手率" in tail.columns:
                summary[f"{label}日均换手率%"] = round_or_none(tail["换手率"].mean())

    if need_sr:
        sr_payload = analyze_support_resistance(
            hist_df=hist_df,
            lookback_days=sr_days,
            level_count=sr_levels,
            pivot_window=sr_pivot_window,
        )
        if sr_payload:
            if "历史来源" not in summary:
                summary["历史来源"] = resolved_source
            summary.update(sr_payload)

    if need_factors:
        factor_payload = analyze_advanced_factors(
            hist_df=hist_df,
            lookback_days=factor_days,
        )
        if factor_payload:
            if "历史来源" not in summary:
                summary["历史来源"] = resolved_source
            summary.update(factor_payload)
    return summary


def analyze_support_resistance(
    hist_df: Any,
    lookback_days: int,
    level_count: int,
    pivot_window: int,
) -> dict[str, Any]:
    if lookback_days <= 0 or level_count <= 0:
        return {}
    if pivot_window < 1:
        pivot_window = 1

    tail = hist_df.tail(lookback_days).copy()
    required = {"收盘", "最高", "最低"}
    if tail.empty or not required.issubset(set(tail.columns)):
        return {}

    tail = tail.dropna(subset=["收盘", "最高", "最低"])
    if len(tail) < pivot_window * 2 + 1:
        return {}

    current_price = safe_float(tail["收盘"].iloc[-1])
    if current_price in (None, 0):
        return {}

    lows = [safe_float(v) for v in tail["最低"].tolist()]
    highs = [safe_float(v) for v in tail["最高"].tolist()]
    support_candidates: list[float] = []
    resistance_candidates: list[float] = []

    for idx in range(pivot_window, len(tail) - pivot_window):
        center_low = lows[idx]
        center_high = highs[idx]
        low_window = lows[idx - pivot_window : idx + pivot_window + 1]
        high_window = highs[idx - pivot_window : idx + pivot_window + 1]
        if (
            center_low is None
            or center_high is None
            or any(value is None for value in low_window)
            or any(value is None for value in high_window)
        ):
            continue

        if center_low == min(low_window):
            support_candidates.append(center_low)
        if center_high == max(high_window):
            resistance_candidates.append(center_high)

    range_low = safe_float(tail["最低"].min())
    range_high = safe_float(tail["最高"].max())
    if range_low is not None:
        support_candidates.append(range_low)
    if range_high is not None:
        resistance_candidates.append(range_high)

    supports = pick_price_levels(
        candidates=support_candidates,
        current_price=current_price,
        level_count=level_count,
        level_type="support",
    )
    resistances = pick_price_levels(
        candidates=resistance_candidates,
        current_price=current_price,
        level_count=level_count,
        level_type="resistance",
    )

    payload: dict[str, Any] = {
        "支撑压力基准价": round_or_none(current_price),
        "支撑压力回看天数": len(tail),
    }
    for index, value in enumerate(supports, start=1):
        payload[f"支撑位{index}"] = round_or_none(value)
    for index, value in enumerate(resistances, start=1):
        payload[f"压力位{index}"] = round_or_none(value)
    return payload


def pick_price_levels(
    candidates: list[float],
    current_price: float,
    level_count: int,
    level_type: str,
) -> list[float]:
    clean_values = sorted(
        {
            round(value, 2)
            for value in candidates
            if value is not None and value > 0
        }
    )
    if not clean_values:
        return []

    if level_type == "support":
        preferred = [value for value in clean_values if value <= current_price]
        fallback = [value for value in clean_values if value > current_price]
        ordered = sorted(preferred, reverse=True) + sorted(fallback)
    else:
        preferred = [value for value in clean_values if value >= current_price]
        fallback = [value for value in clean_values if value < current_price]
        ordered = sorted(preferred) + sorted(fallback, reverse=True)

    min_gap = max(current_price * 0.003, 0.02)
    selected: list[float] = []
    for value in ordered:
        if all(abs(value - existing) >= min_gap for existing in selected):
            selected.append(value)
        if len(selected) >= level_count:
            break
    return selected


def analyze_advanced_factors(
    hist_df: Any,
    lookback_days: int,
) -> dict[str, Any]:
    tail = hist_df.tail(lookback_days).copy()
    required = {"收盘", "最高", "最低"}
    if tail.empty or not required.issubset(set(tail.columns)):
        return {}

    tail = tail.dropna(subset=["收盘", "最高", "最低"])
    if tail.empty:
        return {}

    close = tail["收盘"].astype(float)
    high = tail["最高"].astype(float)
    low = tail["最低"].astype(float)

    current_price = safe_float(close.iloc[-1])
    if current_price in (None, 0):
        return {}

    ma20 = close.rolling(20).mean().iloc[-1] if len(close) >= 20 else None
    ma60 = close.rolling(60).mean().iloc[-1] if len(close) >= 60 else None

    dist_ma20 = None
    ma20_value = safe_float(ma20)
    if ma20_value not in (None, 0):
        dist_ma20 = (current_price / ma20_value - 1) * 100

    dist_ma60 = None
    ma60_value = safe_float(ma60)
    if ma60_value not in (None, 0):
        dist_ma60 = (current_price / ma60_value - 1) * 100

    trend_label = None
    if ma20_value is not None and ma60_value is not None:
        if current_price > ma20_value > ma60_value:
            trend_label = "多头"
        elif current_price < ma20_value < ma60_value:
            trend_label = "空头"
        else:
            trend_label = "震荡"

    breakout_signal = None
    if len(tail) >= 21:
        prev_high_20 = safe_float(high.rolling(20).max().shift(1).iloc[-1])
        prev_low_20 = safe_float(low.rolling(20).min().shift(1).iloc[-1])
        if prev_high_20 is not None and current_price > prev_high_20:
            breakout_signal = "上破20日高点"
        elif prev_low_20 is not None and current_price < prev_low_20:
            breakout_signal = "下破20日低点"
        else:
            breakout_signal = "区间内"

    previous_close = close.shift(1)
    tr_df = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    )
    tr = tr_df.max(axis=1)
    atr14 = tr.rolling(14).mean().iloc[-1] if len(tr) >= 14 else None
    atr14_value = safe_float(atr14)
    atr14_pct = None
    if atr14_value is not None and current_price:
        atr14_pct = atr14_value / current_price * 100

    rsi14 = None
    if len(close) >= 15:
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / loss.replace(0, pd.NA)
        rsi_series = 100 - 100 / (1 + rs)
        rsi14 = safe_float(rsi_series.iloc[-1])
        if rsi14 is None and safe_float(loss.iloc[-1]) == 0 and safe_float(gain.iloc[-1]) is not None:
            rsi14 = 100.0

    volume_pct = None
    volume_ratio_20 = None
    if "成交量" in tail.columns:
        volume = pd.to_numeric(tail["成交量"], errors="coerce")
        current_volume = safe_float(volume.iloc[-1])
        valid_volume = volume.dropna()
        if current_volume is not None and not valid_volume.empty:
            volume_pct = float((valid_volume <= current_volume).mean() * 100)
        if len(volume) >= 21:
            vol_ma20 = safe_float(volume.rolling(20).mean().shift(1).iloc[-1])
            if vol_ma20 not in (None, 0) and current_volume is not None:
                volume_ratio_20 = current_volume / vol_ma20

    turnover_pct = None
    if "换手率" in tail.columns:
        turnover = pd.to_numeric(tail["换手率"], errors="coerce").dropna()
        if not turnover.empty:
            current_turnover = safe_float(turnover.iloc[-1])
            if current_turnover is not None:
                turnover_pct = float((turnover <= current_turnover).mean() * 100)

    return {
        "均线20": round_or_none(ma20),
        "均线60": round_or_none(ma60),
        "距20日均线%": round_or_none(dist_ma20),
        "距60日均线%": round_or_none(dist_ma60),
        "均线趋势": trend_label,
        "20日突破信号": breakout_signal,
        "ATR14": round_or_none(atr14),
        "ATR14%": round_or_none(atr14_pct),
        "RSI14": round_or_none(rsi14),
        "成交量分位%": round_or_none(volume_pct),
        "量比20日均量": round_or_none(volume_ratio_20),
        "换手率分位%": round_or_none(turnover_pct),
    }


def fetch_sina_history(
    ak: Any,
    code: str,
    start_date: str,
    end_date: str,
    adjust: str,
) -> Any:
    sina_df = ak.stock_zh_a_daily(
        symbol=to_sina_symbol(code),
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
    )
    if sina_df is None or sina_df.empty:
        raise RuntimeError("新浪返回空历史行情")

    df = sina_df.copy()
    df.rename(
        columns={
            "date": "日期",
            "open": "开盘",
            "close": "收盘",
            "high": "最高",
            "low": "最低",
            "volume": "成交量",
            "amount": "成交额",
            "turnover": "换手率",
        },
        inplace=True,
    )
    df["股票代码"] = normalize_code(code)
    df["成交量"] = pd.to_numeric(df["成交量"], errors="coerce") / 100
    df["换手率"] = pd.to_numeric(df["换手率"], errors="coerce") * 100

    for col in ["开盘", "收盘", "最高", "最低", "成交额"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    previous_close = df["收盘"].shift(1)
    df["涨跌额"] = df["收盘"] - previous_close
    df["涨跌幅"] = df["涨跌额"] / previous_close * 100
    df["振幅"] = (df["最高"] - df["最低"]) / previous_close * 100

    return df[
        [
            "日期",
            "股票代码",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "振幅",
            "涨跌幅",
            "涨跌额",
            "换手率",
        ]
    ]


def fetch_financial_indicator(ak: Any, code: str) -> dict[str, Any]:
    financial_df = None
    errors = []

    for func_name, kwargs in (
        ("stock_financial_analysis_indicator", {"symbol": code}),
        ("stock_financial_abstract", {"stock": code}),
    ):
        func = getattr(ak, func_name, None)
        if func is None:
            continue
        try:
            financial_df = func(**kwargs)
            if financial_df is not None and not financial_df.empty:
                break
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{func_name}: {exc}")

    if financial_df is None or financial_df.empty:
        return {"财务指标错误": "；".join(errors)} if errors else {}

    latest = financial_df.tail(1).to_dict(orient="records")[0]
    keep_keywords = [
        "日期",
        "报告期",
        "每股收益",
        "净利润",
        "营业",
        "净资产收益率",
        "资产负债率",
        "毛利率",
    ]
    selected = {
        key: value
        for key, value in latest.items()
        if any(keyword in str(key) for keyword in keep_keywords)
    }
    return {"最近财务指标": sanitize(selected or latest)}


def fetch_fund_flow(
    ak: Any,
    code: str,
    mode: str = "realtime",
) -> dict[str, Any]:
    """获取个股主力资金流数据。"""
    if mode == "daily":
        attempts = ["daily"]
    elif mode == "auto":
        attempts = ["realtime", "daily"]
    else:
        attempts = ["realtime"]

    for attempt in attempts:
        for retry in range(FUND_FLOW_RETRY_TIMES + 1):
            try:
                if attempt == "realtime":
                    result = fetch_fund_flow_realtime(code)
                else:
                    result = fetch_fund_flow_daily(ak, code)
                if result:
                    return result
            except Exception:  # noqa: BLE001
                pass

            if retry < FUND_FLOW_RETRY_TIMES:
                time.sleep(0.3 * (retry + 1))

    # 静默失败，不影响其他数据获取。
    return {}


def to_eastmoney_secid(code: str) -> str:
    normalized = normalize_code(code)
    # 东财 secid: 沪市=1，深市/北交所=0
    market = "1" if normalized.startswith("6") else "0"
    return f"{market}.{normalized}"


def fetch_fund_flow_realtime(code: str) -> dict[str, Any]:
    """实时资金流：东方财富分时接口（当日累计）。"""
    import requests

    response = requests.get(
        EASTMONEY_FUND_FLOW_URL,
        params={
            "secids": to_eastmoney_secid(code),
            "fields": "f12,f14,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f124",
            "ut": "b2884a393a59ad64002292a3e90d46a5",
            "fltt": "2",
            "invt": "2",
            "np": "1",
        },
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Referer": "https://quote.eastmoney.com/",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    diff = payload.get("data", {}).get("diff", [])
    if not isinstance(diff, list) or not diff:
        raise RuntimeError("东方财富实时资金流返回空数据")

    latest = diff[0]
    result = {
        "主力净流入(万)": scale_or_none(latest.get("f62"), 10000),
        "主力净占比%": round_or_none(latest.get("f184")),
        "超大单净流入(万)": scale_or_none(latest.get("f66"), 10000),
        "超大单净占比%": round_or_none(latest.get("f69")),
        "大单净流入(万)": scale_or_none(latest.get("f72"), 10000),
        "大单净占比%": round_or_none(latest.get("f75")),
        "中单净流入(万)": scale_or_none(latest.get("f78"), 10000),
        "中单净占比%": round_or_none(latest.get("f81")),
        "小单净流入(万)": scale_or_none(latest.get("f84"), 10000),
        "小单净占比%": round_or_none(latest.get("f87")),
        "资金流来源": "东方财富-实时",
    }

    flow_time = safe_float(latest.get("f124"))
    if flow_time is not None and flow_time > 0:
        result["资金流时间"] = datetime.fromtimestamp(
            int(flow_time),
            tz=timezone.utc,
        ).astimezone().isoformat()

    core_values = [
        result.get("主力净流入(万)"),
        result.get("超大单净流入(万)"),
        result.get("大单净流入(万)"),
        result.get("中单净流入(万)"),
        result.get("小单净流入(万)"),
    ]
    if all(value is None for value in core_values):
        raise RuntimeError("东方财富实时资金流字段为空")
    return result


def fetch_fund_flow_daily(ak: Any, code: str) -> dict[str, Any]:
    """回退资金流：东方财富日线接口（最近交易日）。"""
    normalized = normalize_code(code)
    if normalized.startswith("6"):
        market = "sh"
    elif normalized.startswith(("4", "8", "9")):
        market = "bj"
    else:
        market = "sz"

    fund_df = ak.stock_individual_fund_flow(stock=normalized, market=market)
    if fund_df is None or fund_df.empty:
        return {}

    latest = fund_df.tail(1).iloc[0]

    # 列名在不同接口版本下可能变化，按候选顺序匹配。
    result = {
        "资金流来源": "东方财富-日线(最近交易日)",
    }
    if "日期" in latest.index and latest["日期"] is not None:
        result["资金流时间"] = str(latest["日期"])

    # 净额字段单位为元，这里统一换算为万元。
    for col in ["主力净流入-净额", "主力净额", "主力净流入"]:
        if col in latest.index:
            result["主力净流入(万)"] = round_or_none(
                safe_float(latest[col]) / 10000
                if safe_float(latest[col]) is not None
                else None
            )
            break

    for col in ["主力净流入-净占比", "主力净占比", "主力占比"]:
        if col in latest.index:
            result["主力净占比%"] = round_or_none(latest[col])
            break

    for col in ["超大单净流入-净额", "超大单净额", "超大单净流入"]:
        if col in latest.index:
            result["超大单净流入(万)"] = round_or_none(
                safe_float(latest[col]) / 10000
                if safe_float(latest[col]) is not None
                else None
            )
            break

    for col in ["超大单净流入-净占比", "超大单净占比", "超大单占比"]:
        if col in latest.index:
            result["超大单净占比%"] = round_or_none(latest[col])
            break

    for col in ["大单净流入-净额", "大单净额", "大单净流入"]:
        if col in latest.index:
            result["大单净流入(万)"] = round_or_none(
                safe_float(latest[col]) / 10000
                if safe_float(latest[col]) is not None
                else None
            )
            break

    for col in ["大单净流入-净占比", "大单净占比", "大单占比"]:
        if col in latest.index:
            result["大单净占比%"] = round_or_none(latest[col])
            break

    for col in ["中单净流入-净额", "中单净额", "中单净流入"]:
        if col in latest.index:
            result["中单净流入(万)"] = round_or_none(
                safe_float(latest[col]) / 10000
                if safe_float(latest[col]) is not None
                else None
            )
            break

    for col in ["小单净流入-净额", "小单净额", "小单净流入"]:
        if col in latest.index:
            result["小单净流入(万)"] = round_or_none(
                safe_float(latest[col]) / 10000
                if safe_float(latest[col]) is not None
                else None
            )
            break

    return result


def to_records(df: Any) -> list[dict[str, Any]]:
    return sanitize(df.to_dict(orient="records"))


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


def build_record(ak: Any, row: Any, args: argparse.Namespace) -> dict[str, Any]:
    code = normalize_code(row["代码"])
    history = fetch_history_summary(
        ak,
        code,
        args.history_days,
        args.adjust,
        args.history_source,
        include_sr=args.support_resistance,
        sr_days=args.sr_days,
        sr_levels=args.sr_levels,
        sr_pivot_window=args.sr_pivot_window,
        include_factors=args.advanced_factors,
        factor_days=args.factor_days,
    )
    financial = fetch_financial_indicator(ak, code) if args.financial else {}
    fund_flow = (
        fetch_fund_flow(ak, code, mode=args.fund_flow_mode)
        if args.fund_flow
        else {}
    )

    record = {
        "代码": code,
        "名称": row.get("名称"),
        "变化信号": build_signal(row, args),
        "行情来源": row.get("行情来源"),
        "最新价": round_or_none(row.get("最新价")),
        "涨跌幅%": round_or_none(row.get("涨跌幅")),
        "涨跌额": round_or_none(row.get("涨跌额")),
        "涨速%": round_or_none(row.get("涨速")),
        "5分钟涨跌%": round_or_none(row.get("5分钟涨跌")),
        "60日涨跌幅%": round_or_none(row.get("60日涨跌幅")),
        "年初至今涨跌幅%": round_or_none(row.get("年初至今涨跌幅")),
        "成交量(手)": round_or_none(row.get("成交量"), 0),
        "成交额(亿)": scale_or_none(row.get("成交额"), 100_000_000),
    }
    record.update(fund_flow)
    record.update({
        "振幅%": round_or_none(row.get("振幅")),
        "换手率%": round_or_none(row.get("换手率")),
        "量比": round_or_none(row.get("量比")),
        "市盈率-动态": round_or_none(row.get("市盈率-动态")),
        "市净率": round_or_none(row.get("市净率")),
        "总市值(亿)": scale_or_none(row.get("总市值"), 100_000_000),
        "流通市值(亿)": scale_or_none(row.get("流通市值"), 100_000_000),
    })
    record.update(history)
    record.update(financial)
    return sanitize(record)


def records_to_dataframe(records: list[dict[str, Any]]) -> Any:
    return pd.DataFrame(records)


def emit_result(df: Any, args: argparse.Namespace) -> None:
    if args.format == "json":
        payload = json.dumps(to_records(df), ensure_ascii=False, indent=2, default=str)
    elif args.format == "csv":
        payload = df.to_csv(index=False)
    else:
        columns = [col for col in DEFAULT_TABLE_COLUMNS if col in df.columns]
        columns.extend(col for col in df.columns if col.startswith("近") and col not in columns)
        columns.extend(col for col in df.columns if col.startswith("支撑位") and col not in columns)
        columns.extend(col for col in df.columns if col.startswith("压力位") and col not in columns)
        visible_df = df[columns] if columns else df
        payload = visible_df.to_string(index=False)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload, encoding="utf-8")
        print(f"已写入：{output_path}")
    else:
        print(payload)


def main() -> None:
    args = parse_args()
    if args.sr_days <= 0:
        raise SystemExit("--sr-days 必须大于 0")
    if args.sr_levels <= 0:
        raise SystemExit("--sr-levels 必须大于 0")
    if args.sr_pivot_window <= 0:
        raise SystemExit("--sr-pivot-window 必须大于 0")
    if args.factor_days <= 0:
        raise SystemExit("--factor-days 必须大于 0")

    ak = require_dependencies()

    try:
        spot_df = fetch_spot_quotes(ak, args.symbols, args.quote_source)
        selected_df = filter_by_change(spot_df, args)
        selected_df = sort_and_limit(selected_df, args)
    except RuntimeError as exc:
        print(f"错误：{exc}", file=sys.stderr)
        raise SystemExit(1) from None

    if selected_df.empty:
        print("没有找到符合条件的股票。")
        return

    records = []
    total = len(selected_df)
    for index, (_, row) in enumerate(selected_df.iterrows(), start=1):
        print(f"[{index}/{total}] 获取 {row.get('代码')} {row.get('名称')} 相关信息...", file=sys.stderr)
        records.append(build_record(ak, row, args))
        if args.delay > 0 and index < total:
            time.sleep(args.delay)

    result_df = records_to_dataframe(records)
    emit_result(result_df, args)


if __name__ == "__main__":
    main()
