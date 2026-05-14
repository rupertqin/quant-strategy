#!/usr/bin/env node
/**
 * 生成共享数据模块
 * 将 DataStorage 中的信号数据和股票名称映射复制到 src/generated/
 * 供多个页面共享引用（Vite 会自动抽成公共 chunk）
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { execSync } from 'child_process';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
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
const GEN_DIR = path.join(__dirname, '..', 'src', 'generated');

fs.mkdirSync(GEN_DIR, { recursive: true });

// 1. 信号数据
const signalSrc = path.join(PROJECT_ROOT, STORAGE_DIR, 'outputs', 'shortterm', 'signals', 'signal_latest.json');
const signalDst = path.join(GEN_DIR, 'signals.json');
if (fs.existsSync(signalSrc)) {
  fs.copyFileSync(signalSrc, signalDst);
  const size = (fs.statSync(signalSrc).size / 1024 / 1024).toFixed(2);
  console.log(`✓ signals.json (${size} MB)`);
} else {
  console.warn('✗ signal_latest.json 不存在');
  fs.writeFileSync(signalDst, '{"signals":[]}');
}

// 2. 股票名称映射（从 CSV + config 常量生成）
const namesDst = path.join(GEN_DIR, 'stock_names.json');
const names = {};

function loadNamesFromCsv(csvPath) {
  if (!fs.existsSync(csvPath)) return 0;
  const lines = fs.readFileSync(csvPath, 'utf-8').split('\n').filter(Boolean);
  if (lines.length === 0) return 0;

  const headers = lines[0].split(',').map(h => h.trim());
  const symbolIdx = headers.indexOf('symbol');
  const nameIdx = headers.indexOf('name');
  if (symbolIdx < 0 || nameIdx < 0) return 0;

  let count = 0;
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',');
    const symbol = cols[symbolIdx]?.trim();
    const name = cols[nameIdx]?.trim();
    if (symbol && name) {
      names[symbol] = name;
      count++;
    }
  }
  return count;
}

function loadNamesFromConfig(varName) {
  try {
    const cmd = `cd "${PROJECT_ROOT}" && python3 -c "import sys,json; sys.path.insert(0,'${PROJECT_ROOT}'); from DataHub.config import ${varName}; print(json.dumps(${varName}))"`;
    const out = execSync(cmd, { encoding: 'utf-8', timeout: 5000 }).trim();
    const data = JSON.parse(out);
    let count = 0;
    for (const [k, v] of Object.entries(data)) {
      names[k] = v;
      count++;
    }
    return count;
  } catch (e) {
    console.warn(`⚠️  从 config 读取 ${varName} 失败: ${e.message}`);
    return 0;
  }
}

const stockCount = loadNamesFromCsv(path.join(PROJECT_ROOT, STORAGE_DIR, 'stock_basic_info.csv'));
const etfCount = loadNamesFromConfig('ETF_BASIC_INFO');
const indexCount = loadNamesFromConfig('OFFICIAL_INDICES');

fs.writeFileSync(namesDst, JSON.stringify(names));
console.log(`✓ stock_names.json (${Object.keys(names).length} 条) 股票:${stockCount} ETF:${etfCount} 指数:${indexCount}`);
