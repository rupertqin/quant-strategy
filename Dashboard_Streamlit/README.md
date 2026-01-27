#!/usr/bin/env python3
"""
README: Dashboard_Streamlit
"""

# Streamlit Dashboard

## 快速启动

```bash
# 安装依赖
pip install streamlit plotly

# 启动看板
streamlit run app.py
```

## 依赖项目

Dashboard 会自动读取以下目录的输出文件：
- `../Strategy_Value_LongTerm/output_weights.csv` - 长线权重
- `../Strategy_Event_ShortTerm/daily_signals.json` - 短线信号

## 功能

1. **市场状态** - 显示当前是 AGGRESSIVE / NEUTRAL / DEFENSIVE
2. **长线配置** - 饼图 + 权重表格
3. **短线雷达** - 热点板块 + 操作信号
4. **综合建议** - 仓位 + 板块偏好
