"""
数据更新模块
下载并清洗股票数据，用于长线组合优化
"""

import akshare as ak
import baostock as bs  # 备用数据源
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import time

# 尝试导入可选库
try:
    import quantdata as qd
    HAS_QUANTDATA = True
except ImportError:
    HAS_QUANTDATA = False


class DataUpdater:
    """股票数据更新器"""

    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.data_dir = os.path.join(os.path.dirname(config_path), "data")
        os.makedirs(self.data_dir, exist_ok=True)
        # 初始化 baostock
        bs.login()

    def _load_config(self, path: str) -> dict:
        """加载配置文件"""
        import yaml
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def get_stock_data(self, symbol: str, period: str = "2300d", retry: int = 2) -> pd.DataFrame:
        """
        获取单只股票历史数据

        Args:
            symbol: 股票代码，如 "600519.SH"
            period: 数据周期，默认2300天(约9年)
            retry: 重试次数

        Returns:
            包含 OHLCV 数据的 DataFrame
        """
        # 先尝试 akshare
        for i in range(retry):
            try:
                time.sleep(0.5)
                df = ak.stock_zh_a_hist(
                    symbol=symbol.replace(".SH", "").replace(".SZ", ""),
                    period="daily",
                    start_date="20100101",
                    end_date=datetime.now().strftime("%Y%m%d"),
                    adjust="qfq"
                )
                df.set_index('日期', inplace=True)
                df.sort_index(inplace=True)
                return df
            except Exception as e:
                if i < retry - 1:
                    time.sleep(1)
                else:
                    pass  # 静默，使用备用

        # 使用 baostock 备用
        try:
            code = symbol.replace(".SH", "").replace(".SZ", "")
            if symbol.endswith(".SH"):
                bs_code = f"sh.{code}"
            else:
                bs_code = f"sz.{code}"

            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,open,high,low,close,volume,amount",
                start_date='2010-01-01',
                end_date=datetime.now().strftime("%Y-%m-%d"),
                frequency="d",
                adjustflag="2"  # 前复权
            )

            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())

            if data_list:
                df = pd.DataFrame(data_list, columns=rs.fields)
                df.set_index('date', inplace=True)
                df = df[['open', 'high', 'low', 'close', 'volume']]
                df = df.astype(float)
                return df
        except Exception as e:
            print(f"baostock 获取 {symbol} 也失败: {e}")

        return pd.DataFrame()

    def get_etf_data(self, symbol: str, retry: int = 3) -> pd.DataFrame:
        """获取ETF数据"""
        for i in range(retry):
            try:
                time.sleep(1)
                df = ak.fund_etf_hist_em(
                    symbol=symbol.replace(".SH", "").replace(".SZ", ""),
                    period="daily",
                    start_date="20100101",
                    end_date=datetime.now().strftime("%Y%m%d"),
                    adjust="qfq"
                )
                df.set_index('日期', inplace=True)
                df.sort_index(inplace=True)
                return df
            except Exception as e:
                if i < retry - 1:
                    print(f"  重试 {i+1}/{retry-1}...")
                    time.sleep(2)
                else:
                    print(f"获取 {symbol} 数据失败: {e}")
                    return pd.DataFrame()
        return pd.DataFrame()

    def download_all_data(self) -> pd.DataFrame:
        """
        下载所有配置中的股票数据
        返回合并的收盘价序列
        """
        price_data = {}
        symbols = self.config['data_source']['stock_list']

        for symbol in symbols:
            print(f"正在下载 {symbol} ...")

            if symbol.endswith(".SH") or symbol.endswith(".SZ"):
                df = self.get_stock_data(symbol)
            else:
                df = self.get_etf_data(symbol)

            if df.empty:
                print(f"  数据为空，跳过")
                continue

            # 兼容中文和英文列名
            close_col = '收盘' if '收盘' in df.columns else 'close'
            if close_col in df.columns:
                price_data[symbol] = df[close_col]
                print(f"  成功获取 {len(df)} 条数据")
            else:
                print(f"  无收盘价列，可用列: {list(df.columns)}")

        # 合并为 DataFrame
        prices = pd.DataFrame(price_data)

        if prices.empty:
            print("警告: 未获取到任何数据")
            return prices

        prices.index = pd.to_datetime(prices.index)

        # 保存
        prices.to_csv(os.path.join(self.data_dir, "prices.csv"))
        print(f"数据已保存至 {self.data_dir}/prices.csv")

        return prices

    def calculate_returns(self, prices: pd.DataFrame = None) -> pd.DataFrame:
        """计算收益率"""
        if prices is None:
            prices = pd.read_csv(
                os.path.join(self.data_dir, "prices.csv"),
                index_col=0, parse_dates=True
            )

        returns = prices.pct_change().dropna()
        returns.to_csv(os.path.join(self.data_dir, "returns.csv"))
        return returns

    def get_risk_free_rate(self) -> float:
        """获取无风险利率 (10年期国债收益率)"""
        try:
            df = ak.bond_china_yield_curve()
            # 取最新一期数据
            latest = df.iloc[-1]
            # 返回年化收益率 (转为小数)
            return float(latest['中证10年']) / 100
        except:
            # 默认值
            return 0.025


if __name__ == "__main__":
    updater = DataUpdater()
    prices = updater.download_all_data()
    returns = updater.calculate_returns()
    rf_rate = updater.get_risk_free_rate()
    print(f"无风险利率: {rf_rate:.2%}")
    print(f"数据范围: {prices.index[0]} ~ {prices.index[-1]}")
    print(f"资产数量: {len(prices.columns)}")
