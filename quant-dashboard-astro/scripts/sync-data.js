#!/usr/bin/env node
/**
 * 数据同步脚本
 * 将 Python 后端生成的 JSON/CSV 数据复制到 public/data/ 目录
 * 供 Astro 纯静态站点使用
 *
 * 用法:
 *   node scripts/sync-data.js
 *   npm run sync:data
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_ROOT = path.resolve(__dirname, '..', '..');

function loadEnv(key, defaultValue) {
  const envPath = path.join(PROJECT_ROOT, '.env');
  if (!fs.existsSync(envPath)) return defaultValue;
  const content = fs.readFileSync(envPath, 'utf-8');
  for (const line of content.split('\n')) {
    const [k, ...v] = line.split('=');
    if (k.trim() === key) return v.join('=').trim();
  }
  return defaultValue;
}

const STORAGE_DIR = loadEnv('QUANT_STORAGE_DIR', 'DataStorage');
const DATA_SOURCE = path.join(PROJECT_ROOT, STORAGE_DIR, 'outputs');
const DATA_TARGET = path.join(__dirname, '..', 'public', 'data');

function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function copyFile(src, dst) {
  try {
    fs.copyFileSync(src, dst);
    return true;
  } catch (e) {
    console.warn(`  跳过: ${path.basename(src)} (${e.message})`);
    return false;
  }
}

function safeJsonParse(text) {
  const sanitized = text
    .replace(/:\s*NaN/g, ': null')
    .replace(/:\s*-NaN/g, ': null')
    .replace(/:\s*Infinity/g, ': null')
    .replace(/:\s*-Infinity/g, ': null');
  return JSON.parse(sanitized);
}

function syncDirectory(srcDir, dstDir, pattern = null) {
  ensureDir(dstDir);
  if (!fs.existsSync(srcDir)) {
    console.warn(`  源目录不存在: ${srcDir}`);
    return;
  }

  const files = fs.readdirSync(srcDir);
  for (const file of files) {
    const srcPath = path.join(srcDir, file);
    const dstPath = path.join(dstDir, file);

    const stat = fs.statSync(srcPath);
    if (stat.isDirectory()) {
      syncDirectory(srcPath, dstPath, pattern);
    } else if (!pattern || file.match(pattern)) {
      copyFile(srcPath, dstPath);
    }
  }
}

console.log('🔄 同步数据到 Astro 静态站点...');
console.log(`   源: ${DATA_SOURCE}`);
console.log(`   目标: ${DATA_TARGET}`);

// 确保目标目录存在
ensureDir(DATA_TARGET);

// 1. 信号数据
console.log('\n📡 同步信号数据...');
const signalsSrc = path.join(DATA_SOURCE, 'shortterm', 'signals');
const signalsDst = path.join(DATA_TARGET, 'signals');
ensureDir(signalsDst);
if (fs.existsSync(signalsSrc)) {
  const files = fs.readdirSync(signalsSrc).filter(f => f.endsWith('.json'));
  for (const file of files) {
    copyFile(path.join(signalsSrc, file), path.join(signalsDst, file));
  }
  console.log(`   已同步 ${files.length} 个信号文件`);
} else {
  console.log('   目录不存在，跳过');
}

// 2. 技术面数据
console.log('\n🔥 同步技术面数据...');
const techSrc = path.join(DATA_SOURCE, 'shortterm', 'technical_overview');
const techDst = path.join(DATA_TARGET, 'technical');
ensureDir(techDst);
if (fs.existsSync(techSrc)) {
  const files = fs.readdirSync(techSrc).filter(f => f.endsWith('.json'));
  for (const file of files) {
    copyFile(path.join(techSrc, file), path.join(techDst, file));
  }
  // 同时复制 latest.json（如果有的话）
  const latestSrc = path.join(techSrc, 'latest.json');
  if (fs.existsSync(latestSrc)) {
    copyFile(latestSrc, path.join(techDst, 'latest.json'));
  }
  console.log(`   已同步 ${files.length} 个技术面文件`);
} else {
  console.log('   目录不存在，跳过');
}

// 3. 股票池数据（从 LongTerm config + 信号数据生成）
console.log('\n📊 生成股票池数据...');
const poolDst = path.join(DATA_TARGET, 'pool-watch');
ensureDir(poolDst);

function loadYamlStockList(yamlPath) {
  try {
    const content = fs.readFileSync(yamlPath, 'utf-8');
    const lines = content.split('\n');
    const stocks = [];
    let inStockList = false;
    for (const line of lines) {
      const trimmed = line.trim();
      if (trimmed === 'stock_list:') {
        inStockList = true;
        continue;
      }
      if (inStockList) {
        if (trimmed.startsWith('- ')) {
          const symbol = trimmed.replace('- ', '').replace(/"/g, '').split('#')[0].trim();
          if (symbol) stocks.push(symbol);
        } else if (trimmed && !trimmed.startsWith('#')) {
          inStockList = false;
        }
      }
    }
    return stocks;
  } catch {
    return [];
  }
}

const stockList = loadYamlStockList(path.join(PROJECT_ROOT, 'LongTerm', 'config.yaml'));
if (stockList.length > 0) {
  // 读取最新信号数据
  const signalPath = path.join(DATA_TARGET, 'signals', 'signal_latest.json');
  let poolStocks = [];
  let poolSummary = { buy_count: 0, sell_count: 0, watch_count: 0 };

  if (fs.existsSync(signalPath)) {
    const signalData = safeJsonParse(fs.readFileSync(signalPath, 'utf-8'));
    const signalStocks = signalData.stocks || [];
    const stockNames = safeJsonParse(fs.readFileSync(path.join(DATA_TARGET, 'stock_names.json'), 'utf-8') || '{}');

    for (const symbol of stockList) {
      const stock = signalStocks.find(s => s.symbol === symbol);
      if (stock) {
        const signals = stock.signals || [];
        const hasBuy = stock.has_buy_signal || signals.some(s => s.signal_type === 'right');
        const hasSell = signals.some(s => s.signal_type === 'left');
        const poolSignal = hasBuy ? 'buy' : hasSell ? 'sell' : 'watch';

        if (poolSignal === 'buy') poolSummary.buy_count++;
        else if (poolSignal === 'sell') poolSummary.sell_count++;
        else poolSummary.watch_count++;

        // 保留完整信号数据结构，直接复用 SignalTable 组件
        poolStocks.push({
          ...stock,
          pool_signal: poolSignal,
        });
      } else {
        poolSummary.watch_count++;
        poolStocks.push({
          symbol,
          name: stockNames[symbol] || '',
          close_price: null,
          change_pct: null,
          signal_count: 0,
          has_buy_signal: false,
          signals: [],
          signal_score: 0,
          risk_score: 0,
          risk_explanations: [],
          risk_details: {},
          risk_warnings: [],
          risk_level: 'medium',
          risk_recommendation: '',
          stage: null,
          dimension_breakdown: null,
          pool_signal: 'watch',
        });
      }
    }
  }

  const poolData = {
    date: new Date().toISOString().split('T')[0],
    summary: poolSummary,
    stocks: poolStocks,
  };
  fs.writeFileSync(path.join(poolDst, 'latest.json'), JSON.stringify(poolData, null, 2));
  console.log(`   已生成股票池: ${poolStocks.length} 只 (买入${poolSummary.buy_count} / 卖出${poolSummary.sell_count} / 观察${poolSummary.watch_count})`);
} else {
  console.log('   LongTerm/config.yaml 中未找到股票列表');
}

// 4. 长线权重
console.log('\n📈 同步长线权重...');
const longtermSrc = path.join(DATA_SOURCE, 'longterm', 'weights');
const longtermDst = path.join(DATA_TARGET, 'longterm');
ensureDir(longtermDst);
if (fs.existsSync(longtermSrc)) {
  const files = fs.readdirSync(longtermSrc);
  for (const file of files) {
    if (file.endsWith('.csv')) {
      copyFile(path.join(longtermSrc, file), path.join(longtermDst, file));
    } else if (file.endsWith('.json')) {
      copyFile(path.join(longtermSrc, file), path.join(longtermDst, file));
    }
  }
  console.log('   已同步长线权重文件');
} else {
  console.log('   目录不存在，跳过');
}

// 5. 股票名称映射（需要额外生成）
console.log('\n🏷️ 同步股票名称映射...');
const stockBasicPath = path.join(PROJECT_ROOT, STORAGE_DIR, 'stock_basic_info.csv');
if (fs.existsSync(stockBasicPath)) {
  const lines = fs.readFileSync(stockBasicPath, 'utf-8').split('\n');
  const names = {};
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',');
    if (cols.length >= 3) {
      const symbol = cols[0]?.trim();
      const name = cols[2]?.trim();
      if (symbol && name) names[symbol] = name;
    }
  }
  fs.writeFileSync(path.join(DATA_TARGET, 'stock_names.json'), JSON.stringify(names, null, 2));
  console.log(`   已生成 ${Object.keys(names).length} 条名称映射`);
} else {
  console.log('   stock_basic_info.csv 不存在，跳过');
}

console.log('\n✅ 数据同步完成！');
console.log('   运行 npm run build 构建静态站点');
