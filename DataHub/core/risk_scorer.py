"""
风险评分核心模块 - 纯计算，无 UI 依赖

供 Scanner 和 Dashboard 共用，确保前后端风险分一致。
"""

from typing import List, Tuple


def detect_risk_signals(signals: List[dict]) -> List[str]:
    """
    从信号列表中检测风险信号（顶背离、趋势走坏等）

    Args:
        signals: 信号列表

    Returns:
        风险描述列表
    """
    risks = []

    for sig in signals:
        sig_name = sig.get("signal_name", "")
        sig_type = sig.get("signal_type", "left")

        # 顶背离风险
        if "顶背离" in sig_name or ("背离" in sig_name and sig_type == "right"):
            risks.append(f"🔴 {sig_name} - 价格新高但指标未新高，见顶信号")

        # 量价背离（高位缩量上涨）
        if sig_name == "量价背离":
            desc = sig.get("description", "")
            if "高位" in desc or "顶背离" in desc:
                risks.append(f"🔴 量价顶背离 - 价格高位+量能萎缩，上涨乏力")

        # 趋势走坏确认
        if sig_type == "right" and sig_name in ["MA5死叉MA10", "MA5死叉MA20", "MACD死叉", "KDJ死叉"]:
            risks.append(f"⚠️ {sig_name} - 趋势转弱信号")

    return risks


def calculate_risk_score(technicals: dict, signals: List[dict]) -> Tuple[int, List[str]]:
    """
    基于技术指标和信号计算风险分 (0-100，越高越危险) 和详细解释

    Args:
        technicals: 技术指标数据
        signals: 信号列表

    Returns:
        (风险分数, 风险解释列表)
    """
    explanations = []
    risk = 50  # 基准

    # 1. 从信号中检测风险信号
    signal_risks = detect_risk_signals(signals)
    for sr in signal_risks:
        risk += 15
        explanations.append(sr)

    # 2. 技术面分析
    if not technicals:
        return max(0, min(100, int(risk + 10))), explanations + ["⚠️ 无技术指标数据"]

    ma5 = technicals.get("ma5", 0)
    ma10 = technicals.get("ma10", 0)
    ma20 = technicals.get("ma20", 0)
    ma60 = technicals.get("ma60", 0)
    close = technicals.get("close", 0) or ma5

    # 均线排列
    if ma5 and ma10 and ma20:
        if ma5 > ma10 > ma20:  # 多头排列
            risk -= 10
            explanations.append("✅ 多头排列 - 趋势健康")
        elif ma5 < ma10 < ma20:  # 空头排列
            risk += 15
            explanations.append("🔴 空头排列 - 趋势走坏")
        elif ma5 < ma10:  # 短期走弱
            risk += 8
            explanations.append("⚠️ 短期均线下穿 - 短期偏弱")

    # MA60 位置
    if ma60 and close:
        pct_from_ma60 = (close - ma60) / ma60 * 100
        if close > ma60:
            risk -= 8
            explanations.append(f"✅ 站上MA60 (+{pct_from_ma60:.1f}%)")
        else:
            risk += 12
            explanations.append(f"🔴 跌破MA60 ({pct_from_ma60:.1f}%)")

    # MACD 状态
    macd_dif = technicals.get("macd_dif", 0)
    macd_dea = technicals.get("macd_dea", 0)
    if macd_dif and macd_dea:
        if macd_dif > macd_dea:
            risk -= 8
            explanations.append("✅ MACD金叉")
        else:
            risk += 10
            explanations.append("⚠️ MACD死叉/空头")

    # KDJ 超买/超卖
    kdj_j = technicals.get("kdj_j", 0)
    if kdj_j:
        if kdj_j > 90:
            risk += 12
            explanations.append(f"🔴 KDJ超买 (J={kdj_j:.1f})")
        elif kdj_j < 10:
            risk -= 8
            explanations.append(f"✅ KDJ超卖 (J={kdj_j:.1f})")

    # 限制在 0-100
    final_risk = max(0, min(100, int(risk)))

    # 如果没有具体风险项，添加总体评价
    if not explanations:
        if final_risk <= 30:
            explanations.append("✅ 趋势健康")
        elif final_risk <= 60:
            explanations.append("⚠️ 趋势中性，注意风险")
        else:
            explanations.append("🔴 趋势走坏，建议回避")

    return final_risk, explanations
