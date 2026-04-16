"""
市场状态判断模块 V2
综合判断：宏观因子 + 技术面指标
"""

import os
import sys
from pathlib import Path

# 添加父目录到路径以便导入 DataHub
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

from DataHub.core.data_client import UnifiedDataClient

logger = logging.getLogger(__name__)


class MarketRegime:
    """市场状态判断 - 宏观+技术综合版"""

    def __init__(self, config_path: str = None):
        if config_path is None:
            current_dir = os.path.dirname(__file__)
            parent_dir = os.path.dirname(current_dir)
            config_path = os.path.join(parent_dir, "config.yaml")
            if not os.path.exists(config_path):
                config_path = os.path.join(current_dir, "config.yaml")

        self.config = self._load_config(config_path) if os.path.exists(config_path) else {}
        self.cache_dir = self.config.get('cache', {}).get('dir', 'cache')
        os.makedirs(self.cache_dir, exist_ok=True)

        self.data_client = UnifiedDataClient()

        # 板块分类
        self.offensive_sectors = ['半导体', '新能源', '科技', '计算机', '通信', '传媒', '券商']
        self.defensive_sectors = ['黄金', '银行', '公用事业', '医药', '食品饮料', '电力']

    def _load_config(self, path: str) -> dict:
        import yaml
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {path}, using default config")
            return {
                'cache': {'dir': 'cache'},
                'event_params': {'min_zt_count': 3},
                'output': {'signals_file': 'signals.json', 'history_file': 'history.csv'}
            }

    # ========== 宏观因子 ==========

    # 统一的东方财富请求headers
    _EASTMONEY_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer': 'https://quote.eastmoney.com/',
    }

    # 东方财富统一API配置 - 使用trends2接口
    _EASTMONEY_API = {
        'base_url': 'https://push2.eastmoney.com/api/qt/stock/trends2/get',
        'params': {
            'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
            'fltt': '2',
            'invt': '2',
            'v': '2.95',
            'fields1': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f17,f57,f58,f60',
            'fields2': 'f51,f52,f53,f54,f55,f58',
        }
    }

    # 宏观指标secid配置
    _MACRO_SECIDS = {
        'dxy': '100.UDI',      # 美元指数
        'gold': '101.GC00Y',   # COMEX黄金
        'oil': '102.CL00Y',    # NYMEX原油
        'usdcnh': '133.USDCNH', # 美元兑离岸人民币
    }

    def _get_eastmoney_data(self, secid: str) -> dict:
        """
        统一的东方财富数据获取方法 - 使用trends2接口

        Args:
            secid: 股票/期货代码，如 '100.UDI', '101.GC00Y', '133.USDCNH'

        Returns:
            {'current': float, 'prev_close': float, 'change_pct': float, 'name': str}
        """
        import requests
        import time

        params = self._EASTMONEY_API['params'].copy()
        params['secid'] = secid
        params['_'] = int(time.time() * 1000)

        # 构建完整URL用于调试
        full_url = f"{self._EASTMONEY_API['base_url']}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"
        logger.info(f"东方财富请求URL: {full_url}")

        # 添加延时避免频率限制
        time.sleep(0.5)

        # 重试3次
        for attempt in range(3):
            try:
                resp = requests.get(
                    self._EASTMONEY_API['base_url'],
                    params=params,
                    timeout=10
                )

                if resp.status_code == 200:
                    data = resp.json()
                    logger.info(f"东方财富返回数据: {data}")  # 调试：打印完整返回
                    if 'data' in data and data['data']:
                        d = data['data']
                        logger.info(f"数据字段: {d.keys()}")  # 调试：打印所有字段名

                        # trends2接口字段解析
                        # 根据东方财富文档，分时数据字段：
                        # f52: 最新价, f53: 涨跌额, f54: 涨跌幅, f55: 成交量?, f58: 昨收?
                        current = float(d.get('f52', 0)) if d.get('f52') is not None else 0
                        change_amt = float(d.get('f53', 0)) if d.get('f53') is not None else 0
                        change_pct = float(d.get('f54', 0)) if d.get('f54') is not None else 0
                        prev_close = float(d.get('f60', 0)) if d.get('f60') is not None else 0

                        # 如果没有昨收但有最新价和涨跌额，计算昨收
                        if prev_close == 0 and current > 0 and change_amt != 0:
                            prev_close = current - change_amt

                        # 如果涨跌幅为0但有最新价和昨收，计算涨跌幅
                        if change_pct == 0 and current > 0 and prev_close > 0:
                            change_pct = (current - prev_close) / prev_close * 100

                        logger.info(f"解析结果: current={current}, prev_close={prev_close}, change_pct={change_pct}")

                        return {
                            'current': current,
                            'prev_close': prev_close,
                            'change_pct': change_pct,
                            'name': d.get('f58', ''),
                            'code': secid,
                        }
                    else:
                        logger.debug(f"东方财富API返回空数据 {secid}")
                        return {}
                else:
                    logger.debug(f"东方财富API返回错误状态码 {secid}: {resp.status_code}")

            except Exception as e:
                logger.debug(f"东方财富API获取失败 {secid} (attempt {attempt+1}/3): {e}")
                if attempt < 2:
                    time.sleep(1)  # 重试前等待
                continue

        return {}

    def get_usd_cny_rate(self) -> dict:
        """获取美元人民币汇率 - 东方财富离岸人民币"""
        data = self._get_eastmoney_data(self._MACRO_SECIDS['usdcnh'])

        if data and data['current'] > 0:
            logger.info(f"汇率获取成功: {data['current']:.4f}, 涨跌: {data['change_pct']:.2f}%")
            return {
                'current': round(data['current'], 4),
                'prev_close': round(data['prev_close'], 4) if data['prev_close'] > 0 else None,
                'change_pct': round(data['change_pct'], 2),
                'change_5d': data['change_pct'],
                'source': '东方财富-USDCNH',
                'date': None
            }

        # 备用: akshare
        try:
            import akshare as ak
            df = ak.fx_spot_quote()
            if not df.empty:
                usd_row = df[df['货币对'] == 'USD/CNY']
                if not usd_row.empty:
                    buy = usd_row.iloc[0].get('买报价', 0)
                    sell = usd_row.iloc[0].get('卖报价', 0)
                    if buy > 0 and sell > 0:
                        current = (buy + sell) / 2
                    elif sell > 0:
                        current = sell
                    elif buy > 0:
                        current = buy
                    else:
                        current = 6.9

                    if current > 0 and not __import__('math').isnan(current):
                        return {
                            'current': round(float(current), 4),
                            'buy': round(float(buy), 4) if buy > 0 else None,
                            'sell': round(float(sell), 4) if sell > 0 else None,
                            'change_5d': 0,
                            'source': 'akshare',
                            'date': None
                        }
        except Exception as e:
            logger.debug(f"akshare汇率接口失败: {e}")

        logger.warning("获取汇率失败，使用默认值")
        return {'current': 6.9, 'change_5d': 0, 'source': '默认', 'date': None}

    def get_north_money_flow(self) -> dict:
        """获取北向资金流向"""
        try:
            import akshare as ak
            # 使用akshare直接获取北向资金
            df = ak.stock_hsgt_fund_flow_summary_em()
            if not df.empty and '资金方向' in df.columns and '成交净买额' in df.columns:
                # 查找北向资金数据
                north_df = df[df['资金方向'] == '北向']
                if not north_df.empty:
                    # 计算北向资金总和（沪股通+深股通）
                    today = float(north_df['成交净买额'].sum())
                    # 获取净流入
                    inflow = float(north_df['资金净流入'].sum()) if '资金净流入' in north_df.columns else today

                    logger.info(f"北向资金获取成功: 净买入{today}亿, 净流入{inflow}亿")

                    return {
                        'today': round(today, 2),
                        'inflow': round(inflow, 2),
                        'recent_3d_avg': round(today, 2),
                        'recent_5d_avg': round(today, 2),
                        'detail': north_df[['板块', '成交净买额', '资金净流入']].to_dict('records') if not north_df.empty else []
                    }
        except Exception as e:
            logger.debug(f"北向资金接口1失败: {e}")

        # 备用接口
        try:
            import akshare as ak
            df = ak.stock_hsgt_hist_em(symbol="沪股通")
            if not df.empty:
                latest = df.iloc[-1]
                # 尝试不同的字段名
                inflow = float(latest.get('净买入额', latest.get('净流入', 0)))
                logger.info(f"北向资金获取成功(备用): {inflow}亿")
                return {'today': round(inflow, 2), 'recent_3d_avg': round(inflow, 2), 'recent_5d_avg': round(inflow, 2)}
        except Exception as e:
            logger.debug(f"北向资金接口2失败: {e}")

        logger.warning("获取北向资金失败，使用默认值")
        return {'recent_3d_avg': 0, 'today': 0}

    def get_gold_price(self) -> dict:
        """获取黄金价格 - 东方财富COMEX黄金"""
        data = self._get_eastmoney_data(self._MACRO_SECIDS['gold'])

        if data and data['current'] > 0:
            change = data['current'] - data['prev_close'] if data['prev_close'] > 0 else 0
            logger.info(f"黄金价格获取成功: ${data['current']:.2f}, 涨跌: {data['change_pct']:.2f}%")
            return {
                'current': round(data['current'], 2),
                'change': round(change, 2),
                'change_pct': round(data['change_pct'], 2),
                'change_5d': data['change_pct'],
                'source': '东方财富-COMEX',
                'unit': 'USD/盎司',
                'note': ''
            }

        # 备用: 新浪期货
        try:
            import akshare as ak
            df = ak.futures_zh_spot(symbol="AU0")
            if not df.empty and len(df) > 0:
                latest = df.iloc[0]
                current = float(latest.get('current_price', 0))
                last_settle = float(latest.get('last_settle_price', current))
                if current > 0 and last_settle > 0:
                    change = current - last_settle
                    change_pct = (change / last_settle) * 100
                    logger.info(f"黄金价格获取成功(新浪): {current}, 涨跌:{change_pct:.2f}%")
                    return {
                        'current': round(current, 2),
                        'change': round(change, 2),
                        'change_pct': round(change_pct, 2),
                        'change_5d': change_pct,
                        'source': '新浪期货',
                        'unit': 'CNY/克',
                        'note': ''
                    }
        except Exception as e:
            logger.debug(f"新浪黄金期货接口失败: {e}")

        logger.warning("获取黄金价格失败，使用默认值")
        return {'current': 2000, 'change': 0, 'change_pct': 0, 'change_5d': 0, 'unit': 'USD/盎司', 'source': '默认', 'note': '数据暂不可用'}

    def get_dxy_index(self) -> dict:
        """获取美元指数 - 东方财富"""
        data = self._get_eastmoney_data(self._MACRO_SECIDS['dxy'])

        if data and data['current'] > 0:
            logger.info(f"美元指数获取成功: {data['current']:.2f}, 涨跌: {data['change_pct']:.2f}%")
            return {
                'current': round(data['current'], 2),
                'prev_close': round(data['prev_close'], 2) if data['prev_close'] > 0 else None,
                'change_pct': round(data['change_pct'], 2),
                'change_5d': data['change_pct'],
                'source': '东方财富',
                'note': ''
            }

        # 备用: 新浪财经外汇
        try:
            import akshare as ak
            df = ak.fx_sina_quote()
            if not df.empty and len(df) > 0:
                for _, row in df.iterrows():
                    name = str(row.get('name', ''))
                    if 'EURUSD' in name or ('欧元' in name and '美元' in name):
                        eur_change_pct = float(row.get('chg', 0))
                        dxy_change = -0.6 * eur_change_pct
                        logger.info(f"美元指数通过EURUSD估算: {dxy_change:.2f}%")
                        return {
                            'current': 103.5,
                            'change_pct': round(dxy_change, 2),
                            'change_5d': dxy_change,
                            'source': '新浪财经',
                            'note': '通过EURUSD估算'
                        }
        except Exception as e:
            logger.debug(f"新浪财经外汇接口失败: {e}")

        logger.warning("美元指数数据暂不可用，返回默认值")
        return {'current': 103.5, 'change_pct': 0, 'change_5d': 0, 'source': '默认', 'note': '数据暂不可用'}

    def get_oil_price(self) -> dict:
        """获取原油价格 - 东方财富NYMEX原油"""
        data = self._get_eastmoney_data(self._MACRO_SECIDS['oil'])

        if data and data['current'] > 0:
            change = data['current'] - data['prev_close'] if data['prev_close'] > 0 else 0
            logger.info(f"原油价格获取成功: ${data['current']:.2f}, 涨跌: {data['change_pct']:.2f}%")
            return {
                'current': round(data['current'], 2),
                'change': round(change, 2),
                'change_pct': round(data['change_pct'], 2),
                'change_5d': data['change_pct'],
                'source': '东方财富-NYMEX',
                'type': 'WTI原油',
                'unit': 'USD/桶',
                'note': ''
            }

        # 备用: 新浪期货 - 上海原油
        try:
            import akshare as ak
            df = ak.futures_zh_spot(symbol="SC0")
            if not df.empty and len(df) > 0:
                latest = df.iloc[0]
                current = float(latest.get('current_price', 0))
                last_settle = float(latest.get('last_settle_price', current))
                if current > 0 and last_settle > 0:
                    change = current - last_settle
                    change_pct = (change / last_settle) * 100
                    logger.info(f"原油价格获取成功(上海原油): {current}, 涨跌:{change_pct:.2f}%")
                    return {
                        'current': round(current, 2),
                        'change': round(change, 2),
                        'change_pct': round(change_pct, 2),
                        'change_5d': change_pct,
                        'source': '新浪期货',
                        'type': '上海原油',
                        'unit': 'CNY/桶',
                        'note': ''
                    }
        except Exception as e:
            logger.debug(f"上海原油接口失败: {e}")

        logger.warning("获取原油价格失败，使用默认值")
        return {'current': 75, 'change': 0, 'change_pct': 0, 'change_5d': 0, 'type': 'WTI原油', 'unit': 'USD/桶', 'source': '默认', 'note': '数据暂不可用'}

    # ========== 技术面因子 ==========

    def get_market_breadth(self) -> dict:
        """
        获取市场涨跌家数
        Returns: {'up': 上涨家数, 'down': 下跌家数, 'ratio': 涨跌比}
        """
        # 方法1: 东方财富接口（字段最全）
        try:
            import akshare as ak
            df = ak.stock_zh_a_spot_em()

            if not df.empty and '涨跌幅' in df.columns:
                up_count = len(df[df['涨跌幅'] > 0])
                down_count = len(df[df['涨跌幅'] < 0])
                flat_count = len(df[df['涨跌幅'] == 0])
                total = len(df)

                logger.info(f"东方财富接口获取涨跌家数: 涨{up_count}/跌{down_count}/平{flat_count}, 总计{total}")

                return {
                    'up': up_count,
                    'down': down_count,
                    'flat': flat_count,
                    'total': total,
                    'up_ratio': up_count / total if total > 0 else 0.5,
                    'breadth_score': (up_count - down_count) / total if total > 0 else 0
                }
        except Exception as e:
            logger.debug(f"东方财富接口失败: {e}")

        # 方法2: 新浪接口
        try:
            import akshare as ak
            df = ak.stock_zh_a_spot()

            if not df.empty:
                # 新浪接口字段名：涨跌幅是 '涨跌幅', 涨跌额是 '涨跌额'
                if '涨跌幅' in df.columns:
                    up_count = len(df[df['涨跌幅'] > 0])
                    down_count = len(df[df['涨跌幅'] < 0])
                    flat_count = len(df[df['涨跌幅'] == 0])
                elif '涨跌额' in df.columns:
                    up_count = len(df[df['涨跌额'] > 0])
                    down_count = len(df[df['涨跌额'] < 0])
                    flat_count = len(df[df['涨跌额'] == 0])
                else:
                    logger.warning(f"新浪接口无法找到涨跌字段，可用列: {df.columns.tolist()}")
                    raise ValueError("无法找到涨跌字段")

                total = len(df)

                logger.info(f"新浪接口获取涨跌家数: 涨{up_count}/跌{down_count}/平{flat_count}")

                return {
                    'up': up_count,
                    'down': down_count,
                    'flat': flat_count,
                    'total': total,
                    'up_ratio': up_count / total if total > 0 else 0.5,
                    'breadth_score': (up_count - down_count) / total if total > 0 else 0
                }
        except Exception as e:
            logger.debug(f"新浪接口失败: {e}")

        # 方法3: 使用涨停股池间接获取（当实时行情失败时）
        try:
            import akshare as ak
            from datetime import datetime
            today = datetime.now().strftime('%Y%m%d')

            # 获取今日所有涨停和跌停数据
            df_zt = ak.stock_zt_pool_em(date=today)
            zt_count = len(df_zt) if not df_zt.empty else 0

            # 尝试获取跌停数据
            try:
                # 使用stock_zt_pool_dtgc_em获取跌停数据
                df_dt = ak.stock_zt_pool_dtgc_em(date=today)
                dt_count = len(df_dt) if not df_dt.empty else 0
            except:
                dt_count = 0

            # 估算涨跌家数（基于经验值）
            # 总股票约5000只，涨停+跌停约占活跃股票的10%
            estimated_total = 5000
            estimated_active = (zt_count + dt_count) * 10  # 活跃股票约为涨跌停的10倍

            # 如果涨停多于跌停，估算涨多跌少
            if zt_count > dt_count:
                up_ratio = 0.6
            elif zt_count < dt_count:
                up_ratio = 0.4
            else:
                up_ratio = 0.5

            up_count = int(estimated_total * up_ratio)
            down_count = estimated_total - up_count

            logger.info(f"通过涨跌停估算涨跌家数: 涨{up_count}/跌{down_count} (涨停{zt_count}/跌停{dt_count})")

            return {
                'up': up_count,
                'down': down_count,
                'flat': 0,
                'total': estimated_total,
                'up_ratio': up_ratio,
                'breadth_score': (up_count - down_count) / estimated_total,
                'note': f'通过涨跌停估算(涨停{zt_count}/跌停{dt_count})'
            }
        except Exception as e:
            logger.debug(f"涨跌停估算失败: {e}")

        # 默认值（只在所有接口都失败时返回）
        logger.error("所有接口获取市场涨跌家数失败，使用默认值")
        return {'up': 2500, 'down': 2500, 'flat': 0, 'total': 5000, 'up_ratio': 0.5, 'breadth_score': 0, 'note': '数据获取失败'}

    def _get_index_history(self, code: str, days: int = 120) -> pd.DataFrame:
        """
        获取指数历史数据（用于技术分析）

        Args:
            code: 指数代码
            days: 获取天数

        Returns:
            DataFrame with columns: date, open, high, low, close, volume
        """
        try:
            import akshare as ak
            from datetime import datetime, timedelta

            # 方法1: 使用东方财富历史数据接口（最可靠）
            try:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days + 30)  # 多取一些数据
                start_str = start_date.strftime('%Y%m%d')
                end_str = end_date.strftime('%Y%m%d')

                df = ak.index_zh_a_hist(symbol=code, period="daily", start_date=start_str, end_date=end_str)
                if not df.empty and len(df) >= 20:
                    # 标准化列名
                    df = df.rename(columns={
                        '日期': 'date',
                        '开盘': 'open',
                        '收盘': 'close',
                        '最高': 'high',
                        '最低': 'low',
                        '成交量': 'volume'
                    })
                    df = df.tail(days).reset_index(drop=True)
                    logger.debug(f"东方财富接口获取 {code} 成功，共 {len(df)} 条")
                    return df
            except Exception as e:
                logger.debug(f"index_zh_a_hist 失败: {e}")

            # 方法2: 使用腾讯财经接口（新浪源）
            try:
                # 转换代码格式 000300 -> sh000300
                if code.startswith('0'):
                    tx_code = f'sh{code}'
                else:
                    tx_code = f'sz{code}'

                df = ak.stock_zh_index_daily_tx(symbol=tx_code)
                if not df.empty and len(df) >= 20:
                    df = df.rename(columns={
                        'amount': 'volume'
                    })
                    df = df.tail(days).reset_index(drop=True)
                    logger.debug(f"腾讯接口获取 {code} 成功，共 {len(df)} 条")
                    return df
            except Exception as e:
                logger.debug(f"stock_zh_index_daily_tx 失败: {e}")

            # 方法3: 使用新浪财经接口
            try:
                if code.startswith('0'):
                    sina_code = f'sh{code}'
                else:
                    sina_code = f'sz{code}'

                df = ak.stock_zh_index_daily(symbol=sina_code)
                if not df.empty and len(df) >= 20:
                    df = df.tail(days).reset_index(drop=True)
                    logger.debug(f"新浪接口获取 {code} 成功，共 {len(df)} 条")
                    return df
            except Exception as e:
                logger.debug(f"stock_zh_index_daily 失败: {e}")

            logger.warning(f"所有接口获取指数历史数据失败 {code}")
            return pd.DataFrame()
        except Exception as e:
            logger.debug(f"获取指数历史数据失败 {code}: {e}")
            return pd.DataFrame()

    def _dow_theory_analysis(self, df: pd.DataFrame) -> dict:
        """
        道氏理论分析

        核心原则：
        1. 三种趋势：主要趋势(数月-数年)、次要趋势(3周-3月)、短期趋势(<3周)
        2. 指数相互验证：不同指数应相互确认
        3. 成交量验证：趋势需要成交量配合
        4. 收盘价最重要
        """
        if df.empty or len(df) < 60:
            return {'primary_trend': 'UNKNOWN', 'secondary_trend': 'UNKNOWN', 'note': '数据不足'}

        closes = df['close'].values
        volumes = df['volume'].values if 'volume' in df.columns else None

        # 1. 主要趋势判断 (使用60日均线)
        ma60 = df['close'].rolling(60).mean().iloc[-1]
        ma20 = df['close'].rolling(20).mean().iloc[-1]
        current = closes[-1]

        if current > ma60 * 1.05:
            primary_trend = 'BULL'  # 牛市（主要上升趋势）
            primary_desc = '主要上升趋势'
        elif current < ma60 * 0.95:
            primary_trend = 'BEAR'  # 熊市（主要下降趋势）
            primary_desc = '主要下降趋势'
        else:
            primary_trend = 'SIDEWAYS'  # 横盘整理
            primary_desc = '主要趋势横盘'

        # 2. 次要趋势判断 (使用20日均线与60日均线的关系)
        if ma20 > ma60 * 1.02:
            secondary_trend = 'UP'
            secondary_desc = '次要趋势上升'
        elif ma20 < ma60 * 0.98:
            secondary_trend = 'DOWN'
            secondary_desc = '次要趋势下降'
        else:
            secondary_trend = 'SIDEWAYS'
            secondary_desc = '次要趋势震荡'

        # 3. 计算趋势强度
        high_60 = df['high'].tail(60).max()
        low_60 = df['low'].tail(60).min()
        range_60 = high_60 - low_60

        if range_60 > 0:
            position_in_range = (current - low_60) / range_60
        else:
            position_in_range = 0.5

        # 4. 成交量分析（如果有数据）
        volume_signal = 'neutral'
        if volumes is not None and len(volumes) >= 20:
            recent_vol = volumes[-5:].mean()
            avg_vol = volumes[-20:].mean()

            if primary_trend == 'BULL' and recent_vol > avg_vol * 1.2:
                volume_signal = 'confirming'  # 上涨放量，确认趋势
            elif primary_trend == 'BULL' and recent_vol < avg_vol * 0.8:
                volume_signal = 'warning'  # 上涨缩量，警示
            elif primary_trend == 'BEAR' and recent_vol > avg_vol * 1.2:
                volume_signal = 'confirming'  # 下跌放量，确认趋势
            else:
                volume_signal = 'neutral'

        return {
            'primary_trend': primary_trend,
            'primary_desc': primary_desc,
            'secondary_trend': secondary_trend,
            'secondary_desc': secondary_desc,
            'position_in_range': round(float(position_in_range), 2),
            'ma60': round(float(ma60), 2),
            'ma20': round(float(ma20), 2),
            'volume_signal': volume_signal,
            'trend_strength': self._calculate_trend_strength(df)
        }

    def _calculate_trend_strength(self, df: pd.DataFrame) -> dict:
        """计算趋势强度指标"""
        if len(df) < 20:
            return {'adx': 0, 'strength': 'weak'}

        closes = df['close'].values
        highs = df['high'].values
        lows = df['low'].values

        # 简化ADX计算
        tr_list = []
        plus_dm_list = []
        minus_dm_list = []

        for i in range(1, min(15, len(closes))):
            tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
            tr_list.append(tr)

            plus_dm = highs[i] - highs[i-1] if highs[i] - highs[i-1] > lows[i-1] - lows[i] else 0
            minus_dm = lows[i-1] - lows[i] if lows[i-1] - lows[i] > highs[i] - highs[i-1] else 0

            plus_dm_list.append(max(plus_dm, 0))
            minus_dm_list.append(max(minus_dm, 0))

        if sum(tr_list) > 0:
            plus_di = 100 * sum(plus_dm_list) / sum(tr_list)
            minus_di = 100 * sum(minus_dm_list) / sum(tr_list)
            dx = abs(plus_di - minus_di) / (plus_di + minus_di) * 100 if (plus_di + minus_di) > 0 else 0
        else:
            dx = 0

        if dx > 40:
            strength = 'strong'
        elif dx > 25:
            strength = 'moderate'
        else:
            strength = 'weak'

        return {'adx': round(dx, 1), 'strength': strength}

    def _elliott_wave_analysis(self, df: pd.DataFrame) -> dict:
        """
        波浪理论分析

        核心概念：
        1. 5浪推动（1-2-3-4-5）
        2. 3浪调整（A-B-C）
        3. 斐波那契比例关系
        4. 浪的识别基于高低点
        """
        if df.empty or len(df) < 30:
            return {'wave_count': 'UNKNOWN', 'current_phase': 'unknown', 'note': '数据不足'}

        closes = df['close'].values
        highs = df['high'].values
        lows = df['low'].values

        # 识别局部极值点（简化版）
        peaks = []
        troughs = []

        window = 3
        for i in range(window, len(closes) - window):
            # 峰值
            if highs[i] == max(highs[i-window:i+window+1]):
                peaks.append((i, highs[i]))
            # 谷值
            if lows[i] == min(lows[i-window:i+window+1]):
                troughs.append((i, lows[i]))

        # 分析最近的趋势结构
        if len(peaks) < 2 or len(troughs) < 2:
            return {'wave_count': 'INSUFFICIENT_DATA', 'current_phase': 'unknown'}

        # 获取最近的极值
        recent_peaks = peaks[-3:]
        recent_troughs = troughs[-3:]

        # 判断当前处于什么阶段
        current_price = float(closes[-1])
        last_peak_price = float(recent_peaks[-1][1]) if recent_peaks else current_price
        last_trough_price = float(recent_troughs[-1][1]) if recent_troughs else current_price

        # 计算斐波那契回调位
        if last_peak_price != last_trough_price:
            fib_range = last_peak_price - last_trough_price
            fib_382 = last_peak_price - fib_range * 0.382
            fib_500 = last_peak_price - fib_range * 0.500
            fib_618 = last_peak_price - fib_range * 0.618
        else:
            fib_382 = fib_500 = fib_618 = current_price

        # 判断当前位置
        if current_price > last_peak_price * 0.98:
            phase = '可能的第5浪或顶部'
        elif current_price > fib_382:
            phase = '可能的第3浪或第4浪调整'
        elif current_price > fib_618:
            phase = '可能的调整浪B或第2浪'
        else:
            phase = '可能的底部或调整浪C'

        # 计算浪的结构特征
        if len(recent_peaks) >= 2 and len(recent_troughs) >= 2:
            # 检查是否符合5浪结构特征
            wave_1 = recent_troughs[0][1] if recent_troughs else 0
            wave_2 = recent_peaks[0][1] if recent_peaks else 0

            # 计算波动幅度
            recent_volatility = np.std(closes[-20:]) / np.mean(closes[-20:]) * 100 if len(closes) >= 20 else 0

            wave_structure = {
                'recent_peaks': [(str(df.iloc[p[0]]['date']), round(float(p[1]), 2)) for p in recent_peaks],
                'recent_troughs': [(str(df.iloc[t[0]]['date']), round(float(t[1]), 2)) for t in recent_troughs],
                'volatility_pct': round(float(recent_volatility), 2),
                'fib_382': round(float(fib_382), 2),
                'fib_500': round(float(fib_500), 2),
                'fib_618': round(float(fib_618), 2),
            }
        else:
            wave_structure = {}

        return {
            'wave_count': int(len(peaks) + len(troughs)),
            'current_phase': phase,
            'last_peak': round(float(last_peak_price), 2),
            'last_trough': round(float(last_trough_price), 2),
            'current_vs_peak': round(float((current_price - last_peak_price) / last_peak_price * 100), 2),
            'structure': wave_structure
        }

    def get_index_performance(self) -> dict:
        """
        获取主要指数表现（结合道氏理论和波浪理论）
        Returns: {
            '沪深300': {
                'change': 涨跌幅,
                'trend': 趋势,
                'dow_theory': {...},      # 道氏理论分析
                'elliott_wave': {...}     # 波浪理论分析
            },
            ...
        }
        """
        indices = {
            '沪深300': ('000300', 'sh000300'),
            '中证1000': ('000852', 'sh000852'),
            '创业板': ('399006', 'sz399006'),
            '上证指数': ('000001', 'sh000001')
        }

        result = {}

        # 方法1: 尝试使用新浪实时行情接口（更可靠）
        try:
            import akshare as ak
            spot_df = ak.stock_zh_index_spot_sina()

            for name, (em_code, sina_code) in indices.items():
                try:
                    # 新浪接口使用 sh/sz 前缀
                    idx_row = spot_df[spot_df['代码'] == sina_code]
                    if not idx_row.empty:
                        row = idx_row.iloc[0]
                        change_pct = float(row.get('涨跌幅', 0))
                        close = float(row.get('最新价', 0))
                        # 根据涨跌幅判断趋势
                        trend = 'UP' if change_pct > 0 else 'DOWN' if change_pct < 0 else 'NEUTRAL'

                        result[name] = {
                            'change': round(change_pct, 2),
                            'trend': trend,
                            'close': close
                        }
                        logger.debug(f"获取指数 {name}: {change_pct:+.2f}%")
                except Exception as e:
                    logger.debug(f"新浪接口获取 {name} 失败: {e}")
        except Exception as e:
            logger.debug(f"新浪实时行情接口失败: {e}")

        # 方法2: 使用历史数据接口（备用）
        if len(result) < len(indices):
            try:
                import akshare as ak
                from datetime import datetime, timedelta

                end_date = datetime.now()
                start_date = end_date - timedelta(days=30)
                start_str = start_date.strftime('%Y%m%d')
                end_str = end_date.strftime('%Y%m%d')

                for name, (em_code, _) in indices.items():
                    if name in result:
                        continue
                    try:
                        df = ak.index_zh_a_hist(symbol=em_code, period="daily", start_date=start_str, end_date=end_str)

                        if not df.empty and len(df) >= 2:
                            latest = df.iloc[-1]
                            prev = df.iloc[-2]
                            change_pct = (latest['收盘'] - prev['收盘']) / prev['收盘'] * 100 if prev['收盘'] > 0 else 0

                            if len(df) >= 5:
                                ma5 = df['收盘'].tail(5).mean()
                                trend = 'UP' if latest['收盘'] > ma5 else 'DOWN'
                            else:
                                trend = 'NEUTRAL'

                            result[name] = {
                                'change': round(change_pct, 2),
                                'trend': trend,
                                'close': latest['收盘']
                            }
                    except Exception as e:
                        logger.debug(f"历史数据获取 {name} 失败: {e}")

            except Exception as e:
                logger.debug(f"历史数据接口失败: {e}")

        # 添加道氏理论和波浪理论分析
        for name, (em_code, _) in indices.items():
            if name in result:
                # 获取历史数据进行技术分析
                hist_df = self._get_index_history(em_code, days=90)

                if not hist_df.empty:
                    # 道氏理论分析
                    result[name]['dow_theory'] = self._dow_theory_analysis(hist_df)

                    # 波浪理论分析
                    result[name]['elliott_wave'] = self._elliott_wave_analysis(hist_df)
                else:
                    result[name]['dow_theory'] = {'note': '无法获取历史数据'}
                    result[name]['elliott_wave'] = {'note': '无法获取历史数据'}

        # 填充缺失的指数
        for name in indices.keys():
            if name not in result:
                result[name] = {
                    'change': 0,
                    'trend': 'NEUTRAL',
                    'close': 0,
                    'dow_theory': {'note': '数据缺失'},
                    'elliott_wave': {'note': '数据缺失'}
                }

        # 添加跨指数验证（道氏理论原则）
        result['inter_index_validation'] = self._validate_across_indices(result)

        return result

    def _validate_across_indices(self, indices_data: dict) -> dict:
        """
        跨指数验证（道氏理论原则）
        主要趋势应该被不同指数相互确认
        """
        # 排除非指数键
        index_names = [k for k in indices_data.keys() if k not in ['inter_index_validation']]

        if len(index_names) < 2:
            return {'validation': 'INSUFFICIENT_DATA', 'note': '指数数据不足'}

        # 统计各主要趋势方向
        primary_trends = []
        for name in index_names:
            if 'dow_theory' in indices_data[name]:
                trend = indices_data[name]['dow_theory'].get('primary_trend', 'UNKNOWN')
                primary_trends.append((name, trend))

        # 判断是否一致
        trend_counts = {}
        for _, trend in primary_trends:
            trend_counts[trend] = trend_counts.get(trend, 0) + 1

        dominant_trend = max(trend_counts.items(), key=lambda x: x[1]) if trend_counts else ('UNKNOWN', 0)
        consistency = dominant_trend[1] / len(primary_trends) if primary_trends else 0

        if consistency >= 0.75:
            validation = 'CONFIRMED'
            note = f'主要趋势一致（{dominant_trend[0]}），相互确认'
        elif consistency >= 0.5:
            validation = 'PARTIAL'
            note = '主要趋势部分确认，存在分歧'
        else:
            validation = 'DIVERGENCE'
            note = '主要趋势分歧明显，信号不一致'

        return {
            'validation': validation,
            'consistency': round(consistency, 2),
            'dominant_trend': dominant_trend[0],
            'trend_details': primary_trends,
            'note': note
        }

    def get_sector_strength(self) -> dict:
        """
        获取板块强度对比（进攻 vs 防守）
        """
        try:
            import akshare as ak
            # 尝试多个接口获取行业板块数据
            try:
                df = ak.stock_board_industry_name_em()
                # 获取涨幅列
                if '涨跌幅' in df.columns:
                    change_col = '涨跌幅'
                elif '最新价' in df.columns and '昨收' in df.columns:
                    df['涨跌幅'] = (df['最新价'] - df['昨收']) / df['昨收'] * 100
                    change_col = '涨跌幅'
                else:
                    return {'offensive_avg': 0, 'defensive_avg': 0, 'bias': 0, 'leader': '中性', 'note': '数据格式不支持'}

                name_col = '板块名称' if '板块名称' in df.columns else df.columns[0]
            except:
                # 备用接口
                df = ak.stock_sector_detail(symbol="半导体")
                return {'offensive_avg': 0, 'defensive_avg': 0, 'bias': 0, 'leader': '中性', 'note': '使用备用数据'}

            offensive_sum = 0
            offensive_count = 0
            defensive_sum = 0
            defensive_count = 0

            for _, row in df.iterrows():
                sector_name = str(row.get(name_col, ''))
                try:
                    change = float(row.get(change_col, 0))
                except:
                    continue

                # 判断属于进攻还是防守
                is_offensive = any(s in sector_name for s in self.offensive_sectors)
                is_defensive = any(s in sector_name for s in self.defensive_sectors)

                if is_offensive:
                    offensive_sum += change
                    offensive_count += 1
                elif is_defensive:
                    defensive_sum += change
                    defensive_count += 1

            offensive_avg = offensive_sum / offensive_count if offensive_count > 0 else 0
            defensive_avg = defensive_sum / defensive_count if defensive_count > 0 else 0

            return {
                'offensive_avg': round(offensive_avg, 2),
                'defensive_avg': round(defensive_avg, 2),
                'offensive_count': offensive_count,
                'defensive_count': defensive_count,
                'bias': round(offensive_avg - defensive_avg, 2),
                'leader': '进攻' if offensive_avg > defensive_avg else '防守' if defensive_avg > offensive_avg else '中性'
            }
        except Exception as e:
            logger.warning(f"获取板块强度失败: {e}")
            return {'offensive_avg': 0, 'defensive_avg': 0, 'bias': 0, 'leader': '中性', 'note': str(e)}

    def _get_limit_threshold(self, symbol: str) -> dict:
        """
        根据股票代码获取涨跌幅限制阈值

        Args:
            symbol: 股票代码 (如 '300001.SZ', '688001.SH', '000001.SZ')

        Returns:
            {'up': 涨停阈值, 'down': 跌停阈值, 'is_st': 是否ST股}
        """
        # 提取纯数字代码
        code = symbol.split('.')[0] if '.' in symbol else symbol

        # 判断是否为ST股（简化判断：代码中包含ST标识或名称中包含ST）
        # 注意：这里只做基本判断，精确判断需要结合股票名称
        is_st = False

        # 创业板 (300/301开头): 20%涨跌幅
        if code.startswith('300') or code.startswith('301'):
            return {'up': 19.8, 'down': -19.8, 'is_st': False, 'type': '创业板'}

        # 科创板 (688/689开头): 20%涨跌幅
        if code.startswith('688') or code.startswith('689'):
            return {'up': 19.8, 'down': -19.8, 'is_st': False, 'type': '科创板'}

        # 北交所 (8/43/83/87开头): 30%涨跌幅
        if code.startswith('8') or code.startswith('43') or code.startswith('83') or code.startswith('87'):
            return {'up': 29.7, 'down': -29.7, 'is_st': False, 'type': '北交所'}

        # ST股 (需要名称判断，这里简化处理)
        if is_st:
            return {'up': 4.95, 'down': -4.95, 'is_st': True, 'type': 'ST'}

        # 主板/中小板 (00/60/68开头): 10%涨跌幅
        return {'up': 9.9, 'down': -9.9, 'is_st': False, 'type': '主板'}

    def _calculate_limit_up_down(self, df_spot: pd.DataFrame) -> dict:
        """
        计算涨跌停家数，根据股票类型区分涨跌幅限制

        Args:
            df_spot: 实时行情数据，需包含 '代码' 和 '涨跌幅' 列

        Returns:
            {'zt_count': 涨停数, 'dt_count': 跌停数, 'zt_breakdown': 分类统计}
        """
        if df_spot.empty or '涨跌幅' not in df_spot.columns:
            return {'zt_count': 0, 'dt_count': 0}

        # 确保有代码列
        code_col = None
        for col in ['代码', 'symbol', '股票代码']:
            if col in df_spot.columns:
                code_col = col
                break

        if not code_col:
            # 无法识别代码，使用默认10%判断
            zt_count = len(df_spot[df_spot['涨跌幅'] >= 9.9])
            dt_count = len(df_spot[df_spot['涨跌幅'] <= -9.9])
            return {'zt_count': zt_count, 'dt_count': dt_count}

        zt_counts = {'主板': 0, '创业板': 0, '科创板': 0, '北交所': 0, 'ST': 0}
        dt_counts = {'主板': 0, '创业板': 0, '科创板': 0, '北交所': 0, 'ST': 0}

        for _, row in df_spot.iterrows():
            code = str(row[code_col])
            change = float(row['涨跌幅'])

            # 获取阈值
            threshold = self._get_limit_threshold(code)
            stock_type = threshold['type']

            if change >= threshold['up']:
                zt_counts[stock_type] = zt_counts.get(stock_type, 0) + 1
            elif change <= threshold['down']:
                dt_counts[stock_type] = dt_counts.get(stock_type, 0) + 1

        total_zt = sum(zt_counts.values())
        total_dt = sum(dt_counts.values())

        return {
            'zt_count': total_zt,
            'dt_count': total_dt,
            'zt_breakdown': zt_counts,
            'dt_breakdown': dt_counts
        }

    def get_limit_up_stats(self) -> dict:
        """
        获取涨停统计 - 优先使用同花顺数据中心
        """
        try:
            # 方法1: 使用同花顺数据中心（最准确）
            try:
                from .tonghuashun import get_limit_up_count_from_ths
                result = get_limit_up_count_from_ths()
                zt_count = result.get('zt_count', 0)
                source = result.get('source', '同花顺')
                logger.info(f"涨停统计: {zt_count}家, 来源: {source}")

                if zt_count > 0:
                    # 评估市场情绪
                    if zt_count >= 80:
                        sentiment = '极热'
                    elif zt_count >= 50:
                        sentiment = '活跃'
                    elif zt_count >= 30:
                        sentiment = '正常'
                    elif zt_count >= 15:
                        sentiment = '低迷'
                    else:
                        sentiment = '冷清'

                    return {
                        'zt_count': zt_count,
                        'hot_sectors': 0,
                        'max_sector_zt': 0,
                        'sentiment': sentiment,
                        'assessment': f'{zt_count}家涨停',
                        'source': source
                    }
            except Exception as e:
                logger.debug(f"同花顺涨停统计失败: {e}")

            # 备用：使用akshare
            import akshare as ak
            from datetime import datetime

            zt_count = 0
            source = ""

            # 方法2: 使用东方财富实时行情（区分涨跌幅限制）
            try:
                df_spot = ak.stock_zh_a_spot_em()
                if not df_spot.empty and '涨跌幅' in df_spot.columns:
                    limit_stats = self._calculate_limit_up_down(df_spot)
                    zt_count = limit_stats['zt_count']
                    source = "东财实时行情"
                    zt_breakdown = limit_stats.get('zt_breakdown', {})
                    logger.info(f"涨停统计: {zt_count}家, 来源: {source}")
                    logger.info(f"  分类: 主板{zt_breakdown.get('主板', 0)}/创业板{zt_breakdown.get('创业板', 0)}/科创板{zt_breakdown.get('科创板', 0)}")
            except Exception as e:
                logger.debug(f"东财实时行情统计涨停失败: {e}")

            # 方法3: 使用涨停股池
            if zt_count == 0:
                try:
                    today = datetime.now().strftime('%Y%m%d')
                    df_zt = ak.stock_zt_pool_em(date=today)
                    zt_count = len(df_zt)
                    source = "涨停股池"
                    logger.info(f"涨停统计: {zt_count}家, 来源: {source}")
                except Exception as e:
                    logger.debug(f"涨停股池接口失败: {e}")

            # 评估市场情绪
            if zt_count >= 80:
                sentiment = '极热'
            elif zt_count >= 50:
                sentiment = '活跃'
            elif zt_count >= 30:
                sentiment = '正常'
            elif zt_count >= 15:
                sentiment = '低迷'
            else:
                sentiment = '冷清'

            return {
                'zt_count': zt_count,
                'hot_sectors': 0,
                'max_sector_zt': 0,
                'sentiment': sentiment,
                'assessment': f'{zt_count}家涨停',
                'source': source
            }
        except Exception as e:
            logger.warning(f"获取涨停统计失败: {e}")
            return {'zt_count': 0, 'hot_sectors': 0, 'max_sector_zt': 0, 'sentiment': '未知', 'assessment': '数据获取失败', 'source': 'error'}

    def get_limit_down_stats(self) -> dict:
        """
        获取跌停统计 - 优先使用同花顺数据中心
        """
        try:
            # 方法1: 使用同花顺数据中心（最准确）
            try:
                from .tonghuashun import get_limit_down_count_from_ths
                result = get_limit_down_count_from_ths()
                dt_count = result.get('dt_count', 0)
                source = result.get('source', '同花顺')
                logger.info(f"跌停统计: {dt_count}家, 来源: {source}")

                if dt_count >= 0:  # 同花顺返回0也是有效数据
                    # 评估恐慌情绪
                    if dt_count >= 50:
                        panic = '严重恐慌'
                        risk_level = 5
                    elif dt_count >= 30:
                        panic = '恐慌'
                        risk_level = 4
                    elif dt_count >= 15:
                        panic = '担忧'
                        risk_level = 3
                    elif dt_count >= 5:
                        panic = '轻微担忧'
                        risk_level = 2
                    else:
                        panic = '正常'
                        risk_level = 1

                    return {
                        'dt_count': dt_count,
                        'dt_normal': dt_count,
                        'dt_st': 0,
                        'panic': panic,
                        'risk_level': risk_level,
                        'assessment': f'{dt_count}家跌停',
                        'source': source
                    }
            except Exception as e:
                logger.debug(f"同花顺跌停统计失败: {e}")

            # 备用：使用akshare
            import akshare as ak
            from datetime import datetime
            dt_count = 0
            dt_st_count = 0
            source = ""

            # 方法2: 使用东方财富实时行情（区分涨跌幅限制）
            try:
                df_spot = ak.stock_zh_a_spot_em()
                if not df_spot.empty and '涨跌幅' in df_spot.columns:
                    limit_stats = self._calculate_limit_up_down(df_spot)
                    dt_count = limit_stats['dt_count']
                    source = "东财实时行情"
                    dt_breakdown = limit_stats.get('dt_breakdown', {})
                    logger.info(f"跌停统计: {dt_count}家, 来源: {source}")
                    logger.info(f"  分类: 主板{dt_breakdown.get('主板', 0)}/创业板{dt_breakdown.get('创业板', 0)}/科创板{dt_breakdown.get('科创板', 0)}")
            except Exception as e:
                logger.debug(f"东财实时行情统计跌停失败: {e}")

            # 方法3: 使用跌停股池接口
            if dt_count == 0:
                try:
                    today = datetime.now().strftime('%Y%m%d')
                    df_dt = ak.stock_zt_pool_dtgc_em(date=today)
                    if df_dt is not None and not df_dt.empty:
                        dt_count = len(df_dt)
                        source = "跌停股池"
                        logger.info(f"跌停统计: {dt_count}家, 来源: {source}")
                except Exception as e:
                    logger.debug(f"跌停股池接口失败: {e}")

            # 总跌停数 = 普通跌停 + ST跌停
            total_dt = dt_count + dt_st_count

            # 评估恐慌情绪
            if total_dt >= 50:
                panic = '严重恐慌'
                risk_level = 5
            elif total_dt >= 30:
                panic = '恐慌'
                risk_level = 4
            elif total_dt >= 15:
                panic = '担忧'
                risk_level = 3
            elif total_dt >= 5:
                panic = '轻微担忧'
                risk_level = 2
            else:
                panic = '正常'
                risk_level = 1

            return {
                'dt_count': total_dt,
                'dt_normal': dt_count,
                'dt_st': dt_st_count,
                'panic': panic,
                'risk_level': risk_level,
                'assessment': f'{total_dt}家跌停',
                'source': source
            }
        except Exception as e:
            logger.warning(f"获取跌停统计失败: {e}")
            return {'dt_count': 0, 'dt_normal': 0, 'dt_st': 0, 'panic': '未知', 'risk_level': 0, 'assessment': '数据获取失败', 'source': 'error'}

    def calculate_technical_score(self, breadth: dict, indices: dict, sectors: dict,
                                   zt_stats: dict, dt_stats: dict) -> dict:
        """
        计算技术面综合评分
        总分100分，各项权重：
        - 涨跌家数: 25分
        - 指数趋势: 25分
        - 涨跌停对比: 20分
        - 板块风格: 15分
        - 跨指数验证: 15分
        """
        scores = {}
        details = []

        # 1. 涨跌家数评分 (0-25分)
        up_ratio = breadth.get('up_ratio', 0.5)
        if up_ratio >= 0.7:
            scores['breadth'] = 25
            details.append(f"普涨格局({breadth.get('up', 0)}家上涨) +25")
        elif up_ratio >= 0.55:
            scores['breadth'] = 20
            details.append(f"涨多跌少({int(up_ratio*100)}%上涨) +20")
        elif up_ratio >= 0.45:
            scores['breadth'] = 12
            details.append(f"涨跌均衡 +12")
        elif up_ratio >= 0.3:
            scores['breadth'] = 5
            details.append(f"跌多涨少 +5")
        else:
            scores['breadth'] = 0
            details.append(f"普跌格局({breadth.get('down', 0)}家下跌) +0")

        # 2. 指数趋势评分 (0-25分)
        index_names = [k for k in indices.keys() if k not in ['inter_index_validation']]
        up_indices = sum(1 for name in index_names if indices.get(name, {}).get('change', 0) > 0)
        total_indices = len(index_names) if index_names else 4

        if up_indices == total_indices:
            scores['indices'] = 25
            details.append(f"指数全线上涨({up_indices}/{total_indices}) +25")
        elif up_indices >= total_indices * 0.75:
            scores['indices'] = 20
            details.append(f"指数多数上涨({up_indices}/{total_indices}) +20")
        elif up_indices >= total_indices * 0.5:
            scores['indices'] = 12
            details.append(f"指数涨跌参半({up_indices}/{total_indices}) +12")
        elif up_indices >= total_indices * 0.25:
            scores['indices'] = 5
            details.append(f"指数多数下跌({up_indices}/{total_indices}) +5")
        else:
            scores['indices'] = 0
            details.append(f"指数全线下跌 +0")

        # 3. 涨跌停对比评分 (0-20分)
        zt = zt_stats.get('zt_count', 0)
        dt = dt_stats.get('dt_count', 0)
        zt_dt_ratio = zt / (dt + 1)  # 避免除以0

        if zt >= 80 and dt <= 5:
            scores['zt_dt'] = 20
            details.append(f"涨停极多跌停极少({zt}:{dt}) +20")
        elif zt >= 50 and dt <= 10:
            scores['zt_dt'] = 17
            details.append(f"涨停活跃跌停少({zt}:{dt}) +17")
        elif zt >= 30 and dt <= 15:
            scores['zt_dt'] = 14
            details.append(f"涨停正常({zt}:{dt}) +14")
        elif zt >= 15 and dt <= 20:
            scores['zt_dt'] = 10
            details.append(f"涨停一般({zt}:{dt}) +10")
        elif zt > dt:
            scores['zt_dt'] = max(5, min(8, int(zt_dt_ratio * 3)))
            details.append(f"涨停多于跌停({zt}:{dt}) +{scores['zt_dt']}")
        elif zt == dt:
            scores['zt_dt'] = 5
            details.append(f"涨跌停平衡({zt}:{dt}) +5")
        else:
            scores['zt_dt'] = max(0, 5 - dt)
            details.append(f"跌停多于涨停({zt}:{dt}) +{scores['zt_dt']}")

        # 4. 板块风格评分 (0-15分)
        bias = sectors.get('bias', 0)
        leader = sectors.get('leader', '中性')

        if leader == '进攻' and bias > 2:
            scores['sectors'] = 15
            details.append(f"进攻板块强势领先(+{bias:.1f}%) +15")
        elif leader == '进攻':
            scores['sectors'] = 12
            details.append(f"进攻板块领先 +12")
        elif leader == '中性':
            scores['sectors'] = 8
            details.append(f"板块风格中性 +8")
        elif leader == '防守' and bias < -2:
            scores['sectors'] = 3
            details.append(f"防守板块避险({bias:.1f}%) +3")
        else:
            scores['sectors'] = 5
            details.append(f"防守板块领先 +5")

        # 5. 跨指数验证评分 (0-15分)
        validation = indices.get('inter_index_validation', {})
        consistency = validation.get('consistency', 0)

        if consistency >= 0.9:
            scores['validation'] = 15
            details.append(f"指数高度共振({int(consistency*100)}%) +15")
        elif consistency >= 0.75:
            scores['validation'] = 12
            details.append(f"指数相互确认({int(consistency*100)}%) +12")
        elif consistency >= 0.5:
            scores['validation'] = 8
            details.append(f"指数部分确认({int(consistency*100)}%) +8")
        else:
            scores['validation'] = max(0, int(consistency * 15))
            details.append(f"指数分歧({int(consistency*100)}%) +{scores['validation']}")

        # 计算总分
        total_score = sum(scores.values())

        # 评级
        if total_score >= 85:
            grade = '极强'
            signal = '强烈看多'
        elif total_score >= 70:
            grade = '强势'
            signal = '看多'
        elif total_score >= 55:
            grade = '偏强'
            signal = '谨慎看多'
        elif total_score >= 40:
            grade = '中性'
            signal = '观望'
        elif total_score >= 25:
            grade = '偏弱'
            signal = '谨慎看空'
        elif total_score >= 10:
            grade = '弱势'
            signal = '看空'
        else:
            grade = '极弱'
            signal = '强烈看空'

        return {
            'total_score': total_score,
            'max_score': 100,
            'grade': grade,
            'signal': signal,
            'breakdown': scores,
            'details': details,
            'zt_dt_ratio': round(zt_dt_ratio, 2),
            'zt_count': zt,
            'dt_count': dt
        }

    # ========== 综合判断 ==========

    def get_market_status(self) -> dict:
        """
        综合判断市场状态（宏观+技术）
        返回包含宏观评分、技术面评分的完整评估
        """
        # 1. 宏观因子
        currency = self.get_usd_cny_rate()
        north_money = self.get_north_money_flow()
        gold = self.get_gold_price()
        dxy = self.get_dxy_index()  # 美元指数
        oil = self.get_oil_price()  # 原油价格

        # 2. 技术面因子
        breadth = self.get_market_breadth()
        indices = self.get_index_performance()
        sectors = self.get_sector_strength()
        zt_stats = self.get_limit_up_stats()
        dt_stats = self.get_limit_down_stats()

        # 3. 计算技术面综合评分（0-100分）
        tech_score_detail = self.calculate_technical_score(
            breadth, indices, sectors, zt_stats, dt_stats
        )

        # 4. 宏观风险评分（0-10分，风险越高分数越高）
        macro_risk_score = 0
        macro_reasons = []

        # 汇率风险
        if currency['change_5d'] > 0.02:
            macro_risk_score += 3
            macro_reasons.append("汇率快速贬值(+3)")
        elif currency['change_5d'] > 0.01:
            macro_risk_score += 1
            macro_reasons.append("汇率贬值(+1)")

        # 北向资金风险
        if north_money['recent_3d_avg'] < -50:
            macro_risk_score += 3
            macro_reasons.append("北向资金大幅流出(+3)")
        elif north_money['recent_3d_avg'] < -20:
            macro_risk_score += 1
            macro_reasons.append("北向资金流出(+1)")

        # 避险情绪（黄金）
        if gold.get('change_pct', 0) > 2:
            macro_risk_score += 1
            macro_reasons.append("黄金大涨，避险情绪(+1)")

        # 美元指数风险（美元走强对新兴市场是压力）
        if dxy.get('change_pct', 0) > 1:
            macro_risk_score += 2
            macro_reasons.append("美元指数走强，资金外流压力(+2)")
        elif dxy.get('change_pct', 0) > 0.5:
            macro_risk_score += 1
            macro_reasons.append("美元指数偏强(+1)")

        # 原油价格风险（高油价推高通胀）
        if oil.get('change_pct', 0) > 5:
            macro_risk_score += 2
            macro_reasons.append("油价大涨，通胀担忧(+2)")
        elif oil.get('change_pct', 0) > 3:
            macro_risk_score += 1
            macro_reasons.append("油价上涨(+1)")

        # 宏观评分转换为0-100分制（风险低=高分）
        macro_score = max(0, 100 - macro_risk_score * 8)

        # 5. 综合评分（技术面60% + 宏观40%）
        tech_weight = 0.6
        macro_weight = 0.4

        composite_score = (
            tech_score_detail['total_score'] * tech_weight +
            macro_score * macro_weight
        )

        # 6. 最终市场状态判断
        # 技术面信号
        tech_signal = tech_score_detail['signal']
        tech_grade = tech_score_detail['grade']

        # 综合判断
        if composite_score >= 75:
            regime = 'AGGRESSIVE'
            regime_desc = '进攻'
            action = '积极做多，重仓参与'
        elif composite_score >= 55:
            regime = 'CAUTIOUS_AGGRESSIVE'
            regime_desc = '偏进攻'
            action = '适度参与，控制仓位'
        elif composite_score >= 40:
            regime = 'NEUTRAL'
            regime_desc = '中性'
            action = '观望为主，轻仓试错'
        elif composite_score >= 25:
            regime = 'CAUTIOUS_DEFENSIVE'
            regime_desc = '偏防御'
            action = '降低仓位，防御为主'
        else:
            regime = 'DEFENSIVE'
            regime_desc = '防御'
            action = '空仓或极轻仓，规避风险'

        # 仓位建议（基于综合评分）
        position_pct = min(1.0, max(0.1, composite_score / 100))

        return {
            'regime': regime,
            'regime_desc': regime_desc,
            'action': action,
            'composite_score': round(composite_score, 1),
            'tech_score': {
                'raw_score': tech_score_detail['total_score'],
                'weighted': round(tech_score_detail['total_score'] * tech_weight, 1),
                'grade': tech_grade,
                'signal': tech_signal,
                'breakdown': tech_score_detail['breakdown'],
                'details': tech_score_detail['details'],
                'zt_dt': {
                    'zt': tech_score_detail['zt_count'],
                    'dt': tech_score_detail['dt_count'],
                    'ratio': tech_score_detail['zt_dt_ratio']
                }
            },
            'macro_score': {
                'raw_score': macro_score,
                'weighted': round(macro_score * macro_weight, 1),
                'risk_score': macro_risk_score,
                'risk_factors': macro_reasons
            },
            'position_suggestion': f"{position_pct:.0%}",
            'macro': {
                'currency': currency,
                'north_money': north_money,
                'gold': gold,
                'dxy': dxy,  # 美元指数
                'oil': oil   # 原油价格
            },
            'technical': {
                'breadth': breadth,
                'indices': indices,
                'sectors': sectors,
                'zt_stats': zt_stats,
                'dt_stats': dt_stats
            },
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

    def get_position_multiplier(self) -> float:
        """根据市场状态返回仓位乘数"""
        status = self.get_market_status()

        if status['regime'] == 'AGGRESSIVE':
            return 1.0
        elif status['regime'] == 'NEUTRAL':
            return 0.7
        else:
            return 0.4

    def get_sector_preference(self) -> list:
        """根据市场状态返回推荐的板块方向"""
        status = self.get_market_status()

        if status['regime'] == 'DEFENSIVE':
            return ['黄金', '军工', '医药', '公用事业']
        elif status['regime'] == 'AGGRESSIVE':
            return ['科技', '新能源', '消费', '券商']
        else:
            return ['中特估', '高股息', '半导体']


if __name__ == "__main__":
    regime = MarketRegime()

    print("市场状态分析 V3 - 量化评分系统")
    print("=" * 60)

    status = regime.get_market_status()

    print(f"\n【综合判断】")
    print(f"  市场状态: {status['regime_desc']} ({status['regime']})")
    print(f"  综合评分: {status['composite_score']:.1f}/100")
    print(f"  操作建议: {status['action']}")
    print(f"  仓位建议: {status['position_suggestion']}")

    print(f"\n【技术面评分】{status['tech_score']['signal']} ({status['tech_score']['grade']})")
    print(f"  原始分: {status['tech_score']['raw_score']}/100 → 加权: {status['tech_score']['weighted']}")
    print(f"  涨停/跌停: {status['tech_score']['zt_dt']['zt']}/{status['tech_score']['zt_dt']['dt']} (比:{status['tech_score']['zt_dt']['ratio']})")
    print(f"  明细:")
    for detail in status['tech_score']['details']:
        print(f"    • {detail}")

    print(f"\n【宏观评分】{status['macro_score']['raw_score']:.0f}/100 (加权:{status['macro_score']['weighted']})")
    print(f"  风险因子: {status['macro_score']['risk_score']}/10")
    if status['macro_score']['risk_factors']:
        for factor in status['macro_score']['risk_factors']:
            print(f"    • {factor}")
    else:
        print(f"    • 无显著风险因子")

    print(f"\n【宏观因子】")
    print(f"  汇率: {status['macro']['currency']['current']:.4f}")
    print(f"  北向: {status['macro']['north_money']['today']:.1f}亿")
    print(f"  黄金: {status['macro']['gold']['current']:.2f}")

    print(f"\n【技术面详情】")
    tech = status['technical']
    print(f"  涨跌家数: {tech['breadth']['up']}涨/{tech['breadth']['down']}跌/{tech['breadth']['flat']}平")
    print(f"  上涨比例: {tech['breadth']['up_ratio']*100:.1f}%")
    print(f"  涨停情绪: {tech['zt_stats']['assessment']}")
    print(f"  跌停风险: {tech['dt_stats']['assessment']} - {tech['dt_stats']['panic']}")
    print(f"  板块风格: {tech['sectors']['leader']}主导 (偏差{tech['sectors']['bias']:.2f}%)")
    print(f"  指数表现:")
    for name, data in tech['indices'].items():
        if name != 'inter_index_validation':
            trend_emoji = "📈" if data.get('change', 0) > 0 else "📉" if data.get('change', 0) < 0 else "➡️"
            print(f"    {trend_emoji} {name}: {data['change']:+.2f}%")

    # 跨指数验证
    validation = tech['indices'].get('inter_index_validation', {})
    if validation:
        print(f"  跨指数验证: {validation.get('note', '')}")

    print(f"\n【推荐方向】")
    print(f"  板块: {', '.join(regime.get_sector_preference())}")
