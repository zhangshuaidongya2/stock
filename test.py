#!/Users/zxd/Projects/stock/.venv/bin/python

import akshare as ak


def to_sina_symbol(code):
    if code.startswith("6"):
        return "sh" + code
    if code.startswith(("4", "8", "9")):
        return "bj" + code
    return "sz" + code


symbol = "000001"
start_date = "20170301"
end_date = "20231022"
adjust = ""

print(ak.__version__)

try:
    stock_zh_a_hist_df = ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
        timeout=10,
    )
    print("数据源：东方财富")
except Exception as exc:
    print(f"东方财富历史行情失败，切换新浪。错误：{type(exc).__name__}: {exc}")
    stock_zh_a_hist_df = ak.stock_zh_a_daily(
        symbol=to_sina_symbol(symbol),
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
    )
    print("数据源：新浪")

print(stock_zh_a_hist_df)
