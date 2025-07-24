import polars as pl
import numpy as np
from datetime import datetime,timedelta
codes = np.random.randint(100000,size = 100)
start_date = datetime(2005,1,1)
end_date = datetime(2025,5,27)
dates = pl.date_range(start_date, end_date, interval="1d", eager=True)
stock_codes = list(codes)
df = pl.DataFrame()
factor = pl.DataFrame()
for code in stock_codes:
    # 生成该股票的价格序列（随机游走）
    n_days = len(dates)
    base_price = np.random.uniform(100, 200)  # 随机初始价格

    # 生成收益率序列（正态分布）
    returns = np.random.normal(loc=0.0005, scale=0.02, size=n_days)
    prices = base_price * np.exp(np.cumsum(returns))

    # 生成K线数据
    stock_df = pl.DataFrame({
        "date": dates,
        "open": prices * np.random.uniform(0.99, 1.01, n_days),
        "high": prices * np.random.uniform(1.01, 1.03, n_days),
        "low": prices * np.random.uniform(0.97, 0.99, n_days),
        "close": prices,
        "volume": np.random.randint(1_000_000, 10_000_000, n_days)
    })
    factor_data = pl.DataFrame({
        "date": dates,
        "factor_name": np.random.random(n_days)
    })
    # 添加股票代码列
    stock_df = stock_df.with_columns(
        pl.lit(code).alias("code")
    )
    factor_data = factor_data.with_columns((
        pl.lit(code).alias("code")
    ))

    # 合并到主DataFrame
    df = pl.concat([df, stock_df])
    factor = pl.concat([factor, factor_data])

# 调整列顺序并排序
df = df.select(["code", "date", "open", "high", "low", "close", "volume"])
df = df.sort(["code", "date"])
# 添加交易周过滤（可选：移除非交易日）
df = df.filter(pl.col("date").dt.weekday() < 5)  # 保留周一到周五
df.write_parquet("test.parquet")
factor = factor.select(["code", "date", "factor_name"])
factor = factor.sort(["code", "date"])
factor = factor.filter(pl.col('date').dt.weekday() < 5)
factor.write_parquet("factor.parquet")