"""
风险雷达核心模块 - 真正的风险评估，只做排雷，不做加分

供 Scanner 和 Dashboard 共用，确保前后端风险分一致。

设计原则：
- 风险分从 0 开始，没有风险就是 0
- 只识别真正的风险项（乖离率、超买、阻力位、流动性枯竭、量价背离）
- 绝不把买入信号（多头排列、金叉）当成"低风险"
"""

from typing import List, Tuple, Optional
import pandas as pd


def detect_risk_signals(signals: List[dict]) -> List[str]:
    """从信号列表中检测风险信号（顶背离等）"""
    risks = []
    for sig in signals:
        sig_name = sig.get("signal_name", "")
        desc = sig.get("description", "")
        # 顶背离
        if "顶背离" in sig_name:
            risks.append(f"🚨 {sig_name} - 价格新高但指标未新高，见顶信号")
        # 高位量价背离
        if "量价背离" in sig_name and ("高位" in desc or "顶背离" in desc):
            risks.append("🚨 量价顶背离 - 价格高位+量能萎缩，上涨乏力")
    return risks


def _calculate_rsi(close_series: pd.Series, window: int = 14) -> float:
    """计算最新 RSI"""
    if len(close_series) < window + 1:
        return 50.0
    delta = close_series.diff()
    gain = delta.where(delta > 0, 0).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1]


def calculate_risk_radar(df_daily: Optional[pd.DataFrame] = None,
                         technicals: Optional[dict] = None,
                         is_index: bool = False) -> Tuple[int, List[str]]:
    """
    真正的风险雷达（0-100，越高越危险）

    优先使用 df_daily（更完整），回退到 technicals（前端兼容）
    """
    explanations = []
    risk = 0

    # 尝试从 df_daily 提取最新数据
    if df_daily is not None and not df_daily.empty:
        latest = df_daily.iloc[-1]
        close = latest['close']
        ma5 = latest.get('ma5')
        ma20 = latest.get('ma20')
        ma60 = latest.get('ma60')
        ma250 = latest.get('ma250')
        volume = latest.get('volume', 0)
        amount = latest.get('amount', 0)
        high = latest.get('high', close)
        low = latest.get('low', close)
        change_pct = latest.get('change_pct')
        volume_ratio = latest.get('volume_ratio', 1.0)

        # 如果均线没计算，自己算
        if pd.isna(ma5):
            ma5 = df_daily['close'].tail(5).mean()
        if pd.isna(ma20):
            ma20 = df_daily['close'].tail(20).mean()
        if pd.isna(ma60):
            ma60 = df_daily['close'].tail(60).mean()
        if pd.isna(ma250) and len(df_daily) >= 250:
            ma250 = df_daily['close'].tail(250).mean()

        # === 防线1：单日暴跌硬性风控（一票否决级）===
        if not pd.isna(change_pct):
            # 单日暴跌超过5%
            if change_pct < -5.0:
                risk += 35
                explanations.append(f"🚨 单日暴跌风险: 今日重挫 {change_pct:.2f}%，承接力溃散，禁止买入！")

            # 高位放量砸盘：巨量且大跌
            if change_pct < -4.0 and volume_ratio > 1.2:
                risk += 30
                explanations.append(f"🚨 高位放量砸盘风险: 巨量下跌(量{volume_ratio:.1f}倍)，主力资金大概率在出逃！")

        # 短期破位：跌破MA5（且前一日在MA5之上）
        if len(df_daily) >= 2 and not pd.isna(ma5):
            prev_close = df_daily.iloc[-2]['close']
            prev_ma5 = df_daily['close'].tail(6).iloc[0:5].mean() if len(df_daily) >= 6 else ma5
            if close < ma5 and prev_close >= prev_ma5:
                risk += 25
                explanations.append(f"🚨 短期破位风险: 跌破5日均线(¥{ma5:.2f})，短线趋势走坏！")

        # 1. 乖离率风险：偏离MA20 > 12%
        if ma20 and ma20 > 0:
            bias = (close - ma20) / ma20
            if bias > 0.12:
                risk += 25
                explanations.append(f"🚨 乖离率风险: 偏离MA20达{bias*100:.1f}%，追高极易站岗")

        # 2. 阻力位风险：距离MA250 < 3%
        if ma250 and ma250 > 0:
            dist = (ma250 - close) / close
            if 0 < dist < 0.03:
                risk += 20
                explanations.append(f"🚨 阻力位风险: 距离年线仅{dist*100:.1f}%，强阻力压制")

        # 3. 流动性风险：成交额 < 5000万（指数不存在流动性问题，直接跳过）
        if not is_index and amount and amount < 50_000_000:
            risk += 15
            explanations.append("🚨 流动性风险: 成交额不足5000万，进出困难")

        # 4. RSI 超买
        rsi = _calculate_rsi(df_daily['close'])
        if rsi > 85:
            risk += 20
            explanations.append(f"🚨 RSI超买风险: RSI={rsi:.1f}，严重超买")
        elif rsi > 75:
            risk += 10
            explanations.append(f"⚠️ RSI偏高: RSI={rsi:.1f}，注意过热")

        # 5. 量价背离：价格接近新高但成交量萎缩
        if len(df_daily) >= 5:
            recent_high = df_daily['high'].tail(5).max()
            if close >= recent_high * 0.995:
                avg_vol_5 = df_daily['volume'].tail(5).mean()
                if volume < avg_vol_5 * 0.8:
                    risk += 20
                    explanations.append("🚨 量价背离: 价格新高但成交量萎缩，上涨乏力")

    # 回退到 technicals（供前端旧数据兼容）
    elif technicals:
        ma20 = technicals.get('ma20', 0)
        close = technicals.get('close', 0) or technicals.get('ma5', 0)
        if ma20 and close:
            bias = (close - ma20) / ma20
            if bias > 0.12:
                risk += 25
                explanations.append(f"🚨 乖离率风险: 偏离MA20达{bias*100:.1f}%，追高极易站岗")

        rsi = technicals.get('rsi', 0)
        if rsi > 85:
            risk += 20
            explanations.append(f"🚨 RSI超买风险: RSI={rsi:.1f}，严重超买")
        elif rsi > 75:
            risk += 10
            explanations.append(f"⚠️ RSI偏高: RSI={rsi:.1f}，注意过热")

        kdj_j = technicals.get('kdj_j', 0)
        if kdj_j > 90:
            risk += 15
            explanations.append(f"🚨 KDJ超买风险: J={kdj_j:.1f}，短期过热")

    return min(100, risk), explanations


def calculate_risk_score(technicals: dict, signals: List[dict]) -> Tuple[int, List[str]]:
    """
    兼容旧接口的风险评分。
    新版后端已改用 calculate_risk_radar(df_daily)，
    此函数保留给前端直接调用旧 technicals 数据时使用。
    """
    # 先检测信号层面的风险
    signal_risks = detect_risk_signals(signals)
    explanations = signal_risks.copy()
    risk = len(signal_risks) * 20

    # 再用 technicals 过雷达
    radar_risk, radar_explanations = calculate_risk_radar(technicals=technicals)
    risk += radar_risk
    explanations.extend(radar_explanations)

    # 去重
    seen = set()
    unique = []
    for e in explanations:
        if e not in seen:
            seen.add(e)
            unique.append(e)

    return min(100, risk), unique
