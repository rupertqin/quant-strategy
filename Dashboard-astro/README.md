# 秦项投资量化 Astro

量化交易看板的 Astro 纯静态前端版本，完全复制原 Streamlit Dashboard 的功能。

数据在**构建时**从 `DataStorage/` 直接读取，无需数据同步步骤。

## 项目结构

```
Dashboard-astro/
├── src/
│   ├── generated/           # 构建时生成的共享数据模块（由 prebuild 生成）
│   │   ├── signals.json     # 个股信号全量数据
│   │   └── stock_names.json # 股票代码→名称映射
│   ├── layouts/             # 页面布局
│   ├── pages/               # 路由页面
│   ├── components/          # React 交互组件
│   ├── utils/               # 工具函数
│   └── styles/              # 全局样式
├── scripts/
│   └── generate-signals-module.js  # 数据模块生成脚本
├── public/
│   └── favicon.svg
└── astro.config.mjs         # Astro 配置
```

## 页面功能

| 页面       | 路径                       | 功能                                                    |
| ---------- | -------------------------- | ------------------------------------------------------- |
| 首页       | `/`                        | 市场状态概览、热点板块、快捷导航                        |
| 今日技术面 | `/today-technical/`        | 宏观指标、指数图表（K线/分时）、热点板块、信号列表      |
| 股票池监控 | `/pool-watch/`             | 买入/卖出/观察信号、均线系统、评分排名                  |
| 信号监控   | `/signal-watch/`           | 左侧/右侧信号列表、风险雷达、健康度评分、多维度筛选分页 |
| 股票图表   | `/stock/[symbol]/`         | TradingView Lightweight Charts K线图（日线/周线/月线）  |

## 快速开始

### 1. 安装依赖

```bash
cd Dashboard-astro
npm install
```

### 2. 开发模式

```bash
npm run dev
```

> `predev` 会自动运行 `generate-signals-module.js`，从 `DataStorage/` 生成 `src/generated/` 数据模块。

### 3. 构建静态站点

```bash
npm run build
```

> `prebuild` 同样会自动生成最新数据模块，然后 Astro 执行构建。

### 4. 预览

```bash
npm run preview
```

## 与原 Streamlit 版本的区别

| 特性     | Streamlit                  | Astro                                       |
| -------- | -------------------------- | ------------------------------------------- |
| 架构     | Python 服务端渲染          | 纯静态前端                                  |
| 运行方式 | `streamlit run app.py`     | 构建为静态 HTML + JS                        |
| 部署方式 | 需要 Python 服务器         | 任何静态托管（Vercel/Netlify/GitHub Pages） |
| 数据来源 | 直接读取本地文件           | 构建时从 `DataStorage/` 直接读取            |
| 图表库   | Streamlit 原生 + HTML 嵌入 | TradingView Lightweight Charts              |
| 交互组件 | Streamlit widgets          | React islands                               |

## 数据流

```
Python 后端脚本
    ↓
DataStorage/outputs/*.json  (数据生产层)
    ↓
npm run build               (prebuild 生成 src/generated/)
    ↓
Astro 构建时读取 DataStorage/ 和 src/generated/
    ↓
dist/                       (部署到 CDN)
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
<MyComponent client:only="react" />
```

### 数据加载

Astro frontmatter 中直接从文件系统读取数据：

```astro
---
import { loadTechnicalData } from '@utils/dataLoader';
const data = loadTechnicalData();
---
```

客户端组件如需共享全量数据，从生成的模块导入：

```jsx
import SIGNAL_DATA from '../generated/signals.json';
```

Vite 会自动将其抽成公共 chunk，多页面共享缓存。

## 注意事项

- 所有原始数据必须持久化到 `DataStorage/`，Dashboard 构建时直接读取
- 信号等共享大数据通过 `src/generated/` 模块供多页面引用，避免重复打包
- 股票历史数据量较大，K 线页面在构建时直接读取 `DataStorage/raw/stocks/price/` 下的 Parquet 文件
- 部署后如需更新数据，确保 `DataStorage/` 已更新，然后重新构建即可
