"""
报告生成模块
生成优化的投资组合报告
"""

import pandas as pd
import numpy as np
import os
import yaml
from datetime import datetime
from jinja2 import Template
import base64

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except:
    HAS_MATPLOTLIB = False


class PortfolioReport:
    """组合报告生成器"""

    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.data_dir = os.path.join(os.path.dirname(config_path), "data")
        self.output_dir = os.path.join(self.data_dir, "..")
        self.charts_dir = os.path.join(self.output_dir, "charts")
        os.makedirs(self.charts_dir, exist_ok=True)

    def _load_config(self, path: str) -> dict:
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def load_weights(self) -> pd.DataFrame:
        """加载优化后的权重"""
        weights = pd.read_csv(os.path.join(self.output_dir, "output_weights.csv"))
        # 过滤零权重
        weights = weights[weights['weight'] > 0.001]
        return weights.sort_values('weight', ascending=False)

    def load_returns(self) -> pd.DataFrame:
        """加载收益率数据"""
        returns = pd.read_csv(
            os.path.join(self.data_dir, "returns.csv"),
            index_col=0, parse_dates=True
        )
        return returns

    def generate_pie_chart(self, weights: pd.DataFrame) -> str:
        """生成权重饼图"""
        if not HAS_MATPLOTLIB:
            return None

        plt.figure(figsize=(10, 8))

        # 只显示权重 > 1% 的
        labels = weights['symbol'].tolist()
        sizes = weights['weight'].tolist()

        # 其他归为 "Others"
        threshold = 0.01
        filtered = weights[weights['weight'] >= threshold]
        others_weight = weights[weights['weight'] < threshold]['weight'].sum()

        if others_weight > 0:
            filtered = pd.concat([
                filtered,
                pd.DataFrame({'symbol': ['其他'], 'weight': [others_weight]})
            ])

        plt.pie(
            filtered['weight'],
            labels=filtered['symbol'],
            autopct='%1.1f%%',
            startangle=90
        )
        plt.title('资产配置权重', fontsize=14)
        plt.axis('equal')

        chart_path = os.path.join(self.charts_dir, "allocation_pie.png")
        plt.savefig(chart_path, dpi=150, bbox_inches='tight')
        plt.close()

        return chart_path

    def generate_historical_curve(self, returns: pd.DataFrame, weights: pd.DataFrame) -> str:
        """生成历史净值曲线"""
        if not HAS_MATPLOTLIB:
            return None

        # 获取有效的股票代码（存在于 returns 中的）
        valid_symbols = [s for s in weights['symbol'] if s in returns.columns]
        if not valid_symbols:
            return None

        w_dict = dict(zip(weights['symbol'], weights['weight']))
        port_returns = returns[valid_symbols].dot(
            np.array([w_dict[s] for s in valid_symbols])
        )
        cumulative = (1 + port_returns).cumprod()

        plt.figure(figsize=(12, 6))
        plt.plot(cumulative.index, cumulative.values, linewidth=1.5)
        plt.title('组合历史净值', fontsize=14)
        plt.xlabel('日期')
        plt.ylabel('净值')
        plt.grid(True, alpha=0.3)

        chart_path = os.path.join(self.charts_dir, "cumulative_return.png")
        plt.savefig(chart_path, dpi=150, bbox_inches='tight')
        plt.close()

        return chart_path

    def generate_text_report(self, weights: pd.DataFrame, metrics: dict) -> str:
        """生成文本报告"""
        report = f"""
# 投资组合优化报告
生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 绩效指标

| 指标 | 数值 |
|------|------|
| 年化收益 | {metrics['annualized_return']:.2%} |
| 年化波动 | {metrics['annualized_vol']:.2%} |
| 夏普比率 | {metrics['sharpe_ratio']:.2f} |
| 最大回撤 | {metrics['max_drawdown']:.2%} |

## 推荐配置

| 资产代码 | 权重 |
|----------|------|
"""

        for _, row in weights.iterrows():
            report += f"| {row['symbol']} | {row['weight']:.2%} |\n"

        report += """
## 操作建议

1. 根据上述权重调整持仓
2. 再平衡频率: 季度
3. 盘中不进行主动调仓
4. 长期持有，静待花开

---
*本报告由量化系统自动生成，仅供参考，不构成投资建议*
"""

        return report

    def save_as_html(self, text_report: str, charts: dict):
        """保存为 HTML 格式"""
        # 读取图表为 base64
        def image_to_base64(path):
            if not path or not os.path.exists(path):
                return None
            with open(path, "rb") as f:
                return base64.b64encode(f.read()).decode()

        pie_b64 = image_to_base64(charts.get('pie'))
        cum_b64 = image_to_base64(charts.get('cumulative'))

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>投资组合报告</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        table {{ border-collapse: collapse; width: 50%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        .chart {{ margin: 20px 0; }}
        pre {{ background-color: #f5f5f5; padding: 15px; }}
    </style>
</head>
<body>
    <h1>投资组合优化报告</h1>
    <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
"""

        if pie_b64:
            html += f"""
    <div class="chart">
        <h3>资产配置</h3>
        <img src="data:image/png;base64,{pie_b64}" width="500">
    </div>
"""

        if cum_b64:
            html += f"""
    <div class="chart">
        <h3>历史净值</h3>
        <img src="data:image/png;base64,{cum_b64}" width="700">
    </div>
"""

        # 添加表格
        html += """
    <h3>推荐配置</h3>
    <table>
        <tr><th>资产代码</th><th>权重</th></tr>
"""

        weights = self.load_weights()
        for _, row in weights.iterrows():
            html += f"<tr><td>{row['symbol']}</td><td>{row['weight']:.2%}</td></tr>"

        html += """
    </table>
</body>
</html>
"""

        html_path = os.path.join(self.output_dir, "portfolio_report.html")
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html)

        return html_path

    def run(self, metrics: dict = None):
        """生成完整报告"""
        print("生成报告...")

        weights = self.load_weights()
        print(f"  - 资产数量: {len(weights)}")

        if metrics is None:
            # 简单计算
            returns = self.load_returns()
            w_dict = dict(zip(weights['symbol'], weights['weight']))
            port_returns = returns[weights['symbol']].dot(w_dict)
            metrics = {
                'annualized_return': port_returns.mean() * 252,
                'annualized_vol': port_returns.std() * np.sqrt(252),
                'sharpe_ratio': (port_returns.mean() * 252 - 0.025) / (port_returns.std() * np.sqrt(252)),
                'max_drawdown': (port_returns.cumsum() - port_returns.cumsum().cummax()).min()
            }

        # 生成图表
        charts = {}
        if HAS_MATPLOTLIB:
            charts['pie'] = self.generate_pie_chart(weights)
            charts['cumulative'] = self.generate_historical_curve(self.load_returns(), weights)

        # 生成报告
        text_report = self.generate_text_report(weights, metrics)
        text_path = os.path.join(self.output_dir, "portfolio_report.md")
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write(text_report)
        print(f"  - 文本报告: {text_path}")

        html_path = self.save_as_html(text_report, charts)
        print(f"  - HTML报告: {html_path}")

        print("报告生成完成")
        return text_report, html_path


if __name__ == "__main__":
    report = PortfolioReport()
    report.run()
