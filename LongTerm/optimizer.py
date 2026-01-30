"""
组合优化模块
使用 scipy 进行均值-方差优化 + 趋势过滤
"""

import pandas as pd
import numpy as np
import yaml
import os
import json
from datetime import datetime
from scipy.optimize import minimize

# 内部模块
from data_updater import DataUpdater


class PortfolioOptimizer:
    """组合优化器"""

    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.config = self._load_config(config_path)
        self.base_dir = os.path.dirname(config_path)

        # 数据目录: 从配置读取，默认到 storage/processed
        data_config = self.config.get('data_dir', '../storage/processed')
        if os.path.isabs(data_config):
            self.data_dir = data_config
        else:
            self.data_dir = os.path.join(self.base_dir, data_config)

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

    def run(self, apply_trend_filter: bool = None):
        """
        运行完整优化流程

        Args:
            apply_trend_filter: 是否应用趋势过滤，None 表示根据配置自动判断
        """
        print("=" * 50)
        print("组合优化开始")
        print("=" * 50)

        # 更新数据
        print("\n[1/4] 更新数据...")
        self.updater.download_all_data()
        self.updater.calculate_returns()

        # 加载数据
        returns, prices = self.load_data()
        print(f"    数据范围: {returns.index[0].date()} ~ {returns.index[-1].date()}")
        print(f"    原始资产数量: {len(prices.columns)}")

        # 趋势过滤
        trend_config = self.updater.get_trend_filter_config()
        if apply_trend_filter is None:
            apply_trend_filter = trend_config.get('enabled', False)

        filtered_symbols = list(prices.columns)
        analysis_results = {}

        if apply_trend_filter:
            print("\n[2/4] 趋势分析 (基本面 + 技术面)...")
            analysis_results = self.updater.analyze_all_stocks(prices)

            # 过滤：通过趋势得分筛选
            min_score = trend_config.get('min_trend_score', 0.33)
            min_pe_pct = trend_config.get('pe_percentile_threshold', 0.4)

            filtered_symbols = []
            for symbol, result in analysis_results.items():
                # 价值 + 趋势得分 >= 阈值
                if result['value_score'] == 1 and result['trend_score'] >= min_score:
                    filtered_symbols.append(symbol)
                # PE 分位数特别低 (价值陷阱风险低) 也通过
                elif result.get('pe_percentile', 1.0) < 0.2:
                    filtered_symbols.append(symbol)

            # 只保留有足够数据的股票
            valid_symbols = []
            for s in filtered_symbols:
                if len(returns[s].dropna()) >= 250:
                    valid_symbols.append(s)
            filtered_symbols = valid_symbols

            print(f"    趋势过滤后资产数量: {len(filtered_symbols)}")

            if len(filtered_symbols) < 2:
                print("    警告: 过滤后资产不足，使用全部资产")
                filtered_symbols = list(prices.columns)

            # 保存分析结果
            analysis_path = os.path.join(self.data_dir, "trend_analysis.json")
            with open(analysis_path, 'w', encoding='utf-8') as f:
                json.dump(analysis_results, f, ensure_ascii=False, indent=2)
            print(f"    分析结果已保存至 {analysis_path}")

            # 打印过滤详情
            print("\n    入选股票:")
            for s in filtered_symbols:
                r = analysis_results.get(s, {})
                print(f"      {s}: {r.get('status', 'N/A')} (趋势分: {r.get('trend_score', 0):.2f})")

        # 优化
        print(f"\n[3/4] 运行优化 ({len(filtered_symbols)} 只股票)...")
        returns_filtered = returns[filtered_symbols]
        weights = self.optimize_portfolio(returns_filtered)

        # 保存结果到 storage/outputs
        output_config = self.config.get('output', {})
        output_path = output_config.get('weights_file', '../storage/outputs/longterm/weights/output_weights.csv')
        if not os.path.isabs(output_path):
            output_path = os.path.join(os.path.dirname(self.config_path), output_path)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        weights.to_csv(output_path, index=False)
        print(f"    权重已保存至 {output_path}")

        # 绩效指标
        print("\n[4/4] 绩效指标...")
        w_array = weights.set_index('symbol')['weight'].values
        metrics = self.compute_metrics(returns_filtered, w_array)

        print(f"    年化收益: {metrics['annualized_return']:.2%}")
        print(f"    年化波动: {metrics['annualized_vol']:.2%}")
        print(f"    夏普比率: {metrics['sharpe_ratio']:.2f}")
        print(f"    最大回撤: {metrics['max_drawdown']:.2%}")

        print("\n" + "=" * 50)
        print("优化完成")
        print("=" * 50)

        return weights, metrics, analysis_results


if __name__ == "__main__":
    optimizer = PortfolioOptimizer()
    weights, metrics, analysis = optimizer.run(apply_trend_filter=True)
