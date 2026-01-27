"""
组合优化模块
使用 scipy 进行均值-方差优化
"""

import pandas as pd
import numpy as np
import yaml
import os
from datetime import datetime
from scipy.optimize import minimize

# 内部模块
from data_updater import DataUpdater


class PortfolioOptimizer:
    """组合优化器"""

    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.data_dir = os.path.join(os.path.dirname(config_path), "data")
        self.updater = DataUpdater(config_path)

    def _load_config(self, path: str) -> dict:
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def load_data(self) -> tuple:
        """加载收益率数据"""
        returns = pd.read_csv(
            os.path.join(self.data_dir, "returns.csv"),
            index_col=0, parse_dates=True
        )
        prices = pd.read_csv(
            os.path.join(self.data_dir, "prices.csv"),
            index_col=0, parse_dates=True
        )
        return returns, prices

    def optimize_portfolio(self, returns: pd.DataFrame = None) -> pd.DataFrame:
        """
        运行组合优化 - 最小方差组合

        Returns:
            目标权重 DataFrame
        """
        if returns is None:
            _, returns = self.load_data()

        constraints = self.config.get('constraints', {})
        w_max = constraints.get('max_weight', 0.20)
        w_min = constraints.get('min_weight', 0.02)

        n = len(returns.columns)
        cov_matrix = returns.cov().values

        # 目标函数: 方差
        def portfolio_variance(weights):
            return weights @ cov_matrix @ weights

        # 约束
        constraints = [
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1}  # 权重和为1
        ]

        # 边界
        bounds = tuple((w_min, w_max) for _ in range(n))

        # 初始猜测: 等权
        initial_weights = np.ones(n) / n

        try:
            result = minimize(
                portfolio_variance,
                initial_weights,
                method='SLSQP',
                bounds=bounds,
                constraints=constraints
            )

            if result.success:
                weights = result.x
            else:
                print(f"优化未完全收敛: {result.message}")
                weights = initial_weights
        except Exception as e:
            print(f"优化失败: {e}")
            weights = initial_weights

        weights_df = pd.DataFrame({
            'symbol': returns.columns,
            'weight': weights
        })
        weights_df['weight'] = weights_df['weight'].round(4)

        return weights_df

    def compute_metrics(self, returns: pd.DataFrame, weights: np.ndarray) -> dict:
        """计算组合绩效指标"""
        port_returns = (returns * weights).sum(axis=1)

        annualized_return = port_returns.mean() * 252
        annualized_vol = port_returns.std() * np.sqrt(252)
        sharpe = (annualized_return - self.updater.get_risk_free_rate()) / annualized_vol

        max_drawdown = (port_returns.cumsum() - port_returns.cumsum().cummax()).min()

        return {
            'annualized_return': annualized_return,
            'annualized_vol': annualized_vol,
            'sharpe_ratio': sharpe,
            'max_drawdown': max_drawdown
        }

    def run(self):
        """运行完整优化流程"""
        print("=" * 50)
        print("组合优化开始")
        print("=" * 50)

        # 更新数据
        print("\n[1/3] 更新数据...")
        self.updater.download_all_data()
        self.updater.calculate_returns()

        # 加载数据
        returns, prices = self.load_data()
        print(f"    数据范围: {returns.index[0].date()} ~ {returns.index[-1].date()}")

        # 优化
        print("\n[2/3] 运行优化...")
        weights = self.optimize_portfolio(returns)

        # 保存结果
        output_path = os.path.join(self.data_dir, "..", "output_weights.csv")
        weights.to_csv(output_path, index=False)
        print(f"    权重已保存至 {output_path}")

        # 绩效指标
        print("\n[3/3] 绩效指标...")
        w_array = weights.set_index('symbol')['weight'].values
        metrics = self.compute_metrics(returns, w_array)

        print(f"    年化收益: {metrics['annualized_return']:.2%}")
        print(f"    年化波动: {metrics['annualized_vol']:.2%}")
        print(f"    夏普比率: {metrics['sharpe_ratio']:.2f}")
        print(f"    最大回撤: {metrics['max_drawdown']:.2%}")

        print("\n" + "=" * 50)
        print("优化完成")
        print("=" * 50)

        return weights, metrics


if __name__ == "__main__":
    optimizer = PortfolioOptimizer()
    weights, metrics = optimizer.run()
