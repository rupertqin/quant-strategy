# Quant System - 量化交易系统

## 项目结构

```
/MyQuantSystem
    /Strategy_Value_LongTerm      # 长线/战略部 (Riskfolio)
    /Strategy_Event_ShortTerm     # 短线/战术部 (Pandas/AkShare)
    /Dashboard_Streamlit          # 前端展示 (Streamlit)
```

## 核心理念

- **上帝的归上帝，凯撒的归凯撒**
- 长线：Riskfolio 安静算数学题
- 短线：Pandas 前线处理涨停数据
- 看板：Streamlit 汇聚结果

## 快速开始

### 1. 安装依赖

```bash
# 长线库
pip install riskfolio-lib quantlib numpy pandas

# 短线库
pip install akshare pandas seaborn matplotlib

# 看板
pip install streamlit plotly
```

### 2. 运行

**长线优化 (每月/季度)**
```bash
cd Strategy_Value_LongTerm
python run_optimization.py
```

**短线扫描 (每日收盘)**
```bash
cd Strategy_Event_ShortTerm
python scanner.py
```

**启动看板**
```bash
cd Dashboard_Streamlit
streamlit run app.py
```
