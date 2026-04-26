# Quant Dashboard Astro

量化交易看板的 Astro 纯静态前端版本，完全复制原 Streamlit Dashboard 的功能。

## 项目结构

```
quant-dashboard-astro/
├── public/data/           # 预生成的静态数据（由 sync-data.js 同步）
│   ├── signals/           # 个股信号数据
│   ├── technical/         # 技术面数据（指数、热点板块等）
│   ├── pool-watch/        # 股票池监控数据
│   ├── longterm/          # 长线权重配置
│   └── stock_names.json   # 股票代码→名称映射
├── src/
│   ├── layouts/           # 页面布局
│   ├── pages/             # 路由页面
│   ├── components/        # React 交互组件
│   ├── utils/             # 工具函数
│   └── styles/            # 全局样式
├── scripts/sync-data.js   # 数据同步脚本
└── astro.config.mjs       # Astro 配置
```

## 页面功能

| 页面 | 路径 | 功能 |
|------|------|------|
| 首页 | `/` | 市场状态概览、热点板块、快捷导航 |
| 今日技术面 | `/today-technical/` | 宏观指标、指数图表（K线/分时）、热点板块、信号列表 |
| 股票池监控 | `/pool-watch/` | 买入/卖出/观察信号、均线系统、评分排名 |
| 信号监控 | `/signal-watch/` | 左侧/右侧信号列表、风险雷达、健康度评分、多维度筛选分页 |
| 股票图表 | `/stock-chart/?symbol=xxx` | TradingView Lightweight Charts K线图（日线/周线/月线） |

## 快速开始

### 1. 安装依赖

```bash
cd quant-dashboard-astro
npm install
```

### 2. 同步数据

确保 Python 后端已运行并生成了数据文件，然后：

```bash
npm run sync:data
```

这会从 `../storage/outputs/` 复制 JSON 数据到 `public/data/`。

### 3. 开发模式

```bash
npm run dev
```

### 4. 构建静态站点

```bash
npm run sync:data   # 先同步最新数据
npm run build       # 构建到 dist/ 目录
```

### 5. 预览

```bash
npm run preview
```

## 与原 Streamlit 版本的区别

| 特性 | Streamlit | Astro |
|------|-----------|-------|
| 架构 | Python 服务端渲染 | 纯静态前端 |
| 运行方式 | `streamlit run app.py` | 构建为静态 HTML + JS |
| 部署方式 | 需要 Python 服务器 | 任何静态托管（Vercel/Netlify/GitHub Pages） |
| 数据来源 | 直接读取本地文件 | 从 `public/data/` 的 JSON 文件读取 |
| 图表库 | Streamlit 原生 + HTML 嵌入 | TradingView Lightweight Charts |
| 交互组件 | Streamlit widgets | React islands |

## 数据流

```
Python 后端脚本
    ↓
storage/outputs/*.json  (数据生产层)
    ↓
npm run sync:data       (同步到前端)
    ↓
public/data/*.json      (静态资源)
    ↓
Astro 构建              (生成静态 HTML)
    ↓
dist/                   (部署到 CDN)
```

## 开发指南

### 添加新页面

在 `src/pages/` 下创建 `.astro` 文件，Astro 会自动生成路由。

### 添加交互组件

使用 React 组件 + `client:only="react"` 指令实现客户端交互：

```astro
---
import MyComponent from '@components/MyComponent.jsx';
---
<MyComponent client:only="react" data={data} />
```

### 数据加载

在 Astro frontmatter 中使用 `await` 加载静态数据：

```astro
---
import { loadSignalsData } from '@utils/dataLoader';
const data = await loadSignalsData();
---
```

## 注意事项

- 所有数据必须在构建前同步到 `public/data/`
- 股票历史数据量较大，建议按需加载（客户端 fetch）
- 部署后如需更新数据，重新运行 `sync-data.js` 并重新构建
