"""
指标格式化工具模块

用于在信号列表和图表页面共享指标显示逻辑
"""


def format_technicals(tech: dict) -> str:
    """格式化技术指标显示"""
    parts = []

    # 均线
    ma_parts = []
    for ma in ['ma5', 'ma10', 'ma20', 'ma60']:
        if tech.get(ma) is not None:
            ma_parts.append(f"{ma.upper()[-2:]}:{tech[ma]}")
    if ma_parts:
        parts.append(" | ".join(ma_parts))

    # MACD
    if tech.get('macd_dif') is not None and tech.get('macd_dea') is not None:
        parts.append(f"MACD:{tech['macd_dif']}/{tech['macd_dea']}")

    # KDJ
    if tech.get('kdj_k') is not None and tech.get('kdj_d') is not None:
        kdj_str = f"KDJ:{tech['kdj_k']}/{tech['kdj_d']}"
        if tech.get('kdj_j') is not None:
            kdj_str += f"/J:{tech['kdj_j']}"
        parts.append(kdj_str)

    return " | ".join(parts) if parts else "暂无数据"


def format_flat_mas(tech: dict) -> tuple:
    """
    格式化均线走平显示

    返回: (html_string, has_flat_ma)
    """
    flat_mas = tech.get('flat_mas', [])
    if not flat_mas:
        return "", False

    parts = []
    for ma_info in flat_mas:
        try:
            score = float(ma_info.split('(')[1].rstrip(')'))
            ma_part = ma_info.split('(')[0]
            ma_name = ma_part.split('@')[0].replace('走平', '')
            price = ma_part.split('@')[1] if '@' in ma_part else ""

            # 选择颜色
            if score >= 0.90:
                color = "#ff6b6b"  # 红色 - 强烈走平
            elif score >= 0.80:
                color = "#feca57"  # 黄色 - 较平
            else:
                color = "#48dbfb"  # 蓝色 - 平缓

            parts.append(f"<b style='color:{color}'>{ma_name}@{price}</b>")
        except:
            parts.append(ma_info)

    return " | ".join(parts), True


def render_flat_ma_badge(flat_ma_html: str) -> str:
    """
    渲染均线走平徽章 HTML

    Args:
        flat_ma_html: format_flat_mas 返回的 HTML 字符串

    Returns:
        完整的徽章 HTML
    """
    if not flat_ma_html:
        return ""

    return f"""<div style="margin: 5px 0;"><span style="background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); color: white; padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 600;">📏 均线走平 {flat_ma_html}</span></div>"""


def detect_flat_mas_for_symbol(scanner, symbol: str) -> dict:
    """
    检测指定股票的多周期均线走平情况

    Args:
        scanner: StockSignalScanner 实例
        symbol: 股票代码

    Returns:
        {
            'daily': ['MA10@6.66', 'MA20@6.65'],
            'weekly': [],
            'monthly': ['MA20@6.70']
        }
    """
    df_daily = scanner.load_stock_data(symbol, "daily")
    df_weekly = scanner.load_stock_data(symbol, "weekly")
    df_monthly = scanner.load_stock_data(symbol, "monthly")

    return scanner._detect_flat_mas(df_daily, df_weekly, df_monthly)


def format_ma_bonding(tech: dict) -> str:
    """
    格式化均线粘合显示

    当多根均线非常接近时（价格差异 < 2%），表示均线粘合

    Returns:
        HTML字符串或空字符串
    """
    mas = {}
    for ma_name in ['ma5', 'ma10', 'ma20', 'ma60']:
        if tech.get(ma_name) is not None:
            mas[ma_name.upper()] = float(tech[ma_name])

    if len(mas) < 3:
        return ""

    # 找出最大最小值
    values = list(mas.values())
    max_val = max(values)
    min_val = min(values)
    avg_val = sum(values) / len(values)

    # 计算粘合度（最大差异百分比）
    bonding_pct = (max_val - min_val) / avg_val * 100

    # 粘合阈值：差异 < 2%
    if bonding_pct > 2.0:
        return ""

    # 根据粘合度选择颜色
    if bonding_pct < 0.5:
        color = "#ff6b6b"  # 红色 - 强烈粘合
        level = "强烈"
    elif bonding_pct < 1.0:
        color = "#feca57"  # 黄色 - 中度粘合
        level = "中度"
    else:
        color = "#48dbfb"  # 蓝色 - 轻度粘合
        level = "轻度"

    # 显示哪些均线粘合
    ma_names = list(mas.keys())
    ma_names = [n.replace('MA', 'M') for n in ma_names]  # MA5 -> M5

    ma_str = '/'.join(ma_names)
    return f"<b style='color:{color}'>{ma_str} ({bonding_pct:.2f}%)</b>"


def render_signal_card(signal: dict, idx: int = 0) -> str:
    """
    渲染单个信号卡片（模块化函数，供信号列表和个股图表共用）

    Args:
        signal: 信号字典
        idx: 信号索引，用于生成唯一key

    Returns:
        HTML字符串
    """
    sig_period = signal.get('period', 'daily')
    period_emoji = {'daily': '📅', 'weekly': '📆', 'monthly': '🗓️'}.get(sig_period, '📅')
    period_name = {'daily': '日线', 'weekly': '周线', 'monthly': '月线'}.get(sig_period, '日线')

    sig_type = signal.get('signal_type', 'left')
    type_color = '#2ed573' if sig_type == 'left' else '#ff4757'
    type_name = '左' if sig_type == 'left' else '右'

    strength = signal.get('strength', 'medium')
    strength_color = {'strong': '#ff6b6b', 'medium': '#feca57', 'weak': '#dfe6e9'}.get(strength, '#dfe6e9')

    sig_score = signal.get('score', 0)
    sig_name = signal.get('signal_name', '')
    sig_desc = signal.get('description', '')

    tech = signal.get('technicals', {})

    # 信号类型边框颜色
    border_color = '#2ed573' if sig_type == 'left' else '#ff4757'

    html = f"""
    <div style="background: white; border-radius: 12px; padding: 15px; margin-bottom: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); border-left: 4px solid {border_color};">
        <div style="display: flex; align-items: center; flex-wrap: wrap; gap: 8px; margin-bottom: 8px;">
            <span style="font-size: 14px;">{period_emoji}</span>
            <span style="background: {type_color}; color: white; padding: 2px 6px; border-radius: 4px; font-size: 11px;">{type_name}</span>
            <span style="background: {strength_color}; color: {'white' if strength == 'strong' else '#333'}; padding: 2px 8px; border-radius: 12px; font-size: 11px;">{strength.upper()}</span>
            <span style="font-weight: 600; font-size: 14px; color: #333;">{sig_name}</span>
            <span style="background: #f0f0f0; color: #666; padding: 2px 8px; border-radius: 12px; font-size: 11px;">{sig_score}分</span>
            <span style="background: #e8f4f8; color: #2980b9; padding: 2px 8px; border-radius: 4px; font-size: 11px;">{period_name}</span>
        </div>
        <div style="font-size: 12px; color: #666; margin-bottom: 10px;">{sig_desc}</div>
        <div style="font-size: 12px; color: #888; background: #f8f9fa; padding: 8px 12px; border-radius: 6px; font-family: monospace; margin-bottom: 8px;">{format_technicals(tech)}</div>
    """

    # 均线走平
    flat_ma_html, has_flat_ma = format_flat_mas(tech)
    if has_flat_ma:
        html += f'<div style="margin: 5px 0;"><span style="background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); color: white; padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 600;">📏 均线走平 {flat_ma_html}</span></div>'

    # 均线粘合
    ma_bonding_html = format_ma_bonding(tech)
    if ma_bonding_html:
        html += f'<div style="margin: 5px 0;"><span style="background: linear-gradient(135deg, #8e44ad 0%, #9b59b6 100%); color: white; padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 600;">🧲 均线粘合 {ma_bonding_html}</span></div>'

    html += '</div>'

    return html
