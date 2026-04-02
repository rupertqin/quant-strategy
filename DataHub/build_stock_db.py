#!/usr/bin/env python3
"""
A股股票基本信息数据库构建工具

用途：从 baostock/akshare 获取所有A股公司的基本信息并保存为CSV
执行频率：不定期（如季度、半年或需要更新时），不包含在日常任务中

使用方式:
    python -m DataHub.build_stock_db           # 直接运行
    python ShortTerm/run_scanner.py build-db   # 通过scanner运行
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


class StockDatabaseBuilder:
    """A股股票基本信息数据库构建器"""
    
    def __init__(self):
        """初始化构建器"""
        self.base_dir = Path(__file__).parent.parent
        self.storage_dir = self.base_dir / "storage"
        self.csv_path = self.storage_dir / "stock_basic_info.csv"
        
        # 确保存储目录存在
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        # baostock 登录状态
        self._baostock_logged_in = False
        
    def _baostock_login(self):
        """登录 baostock"""
        if self._baostock_logged_in:
            return True
            
        try:
            import baostock as bs
            lg = bs.login()
            if lg.error_code == "0":
                self._baostock_logged_in = True
                logger.info("baostock 登录成功")
                return True
            else:
                logger.error(f"baostock 登录失败: {lg.error_msg}")
                return False
        except Exception as e:
            logger.error(f"baostock 登录异常: {e}")
            return False
    
    def _baostock_logout(self):
        """登出 baostock"""
        if self._baostock_logged_in:
            try:
                import baostock as bs
                bs.logout()
                self._baostock_logged_in = False
                logger.info("baostock 登出成功")
            except Exception as e:
                logger.warning(f"baostock 登出异常: {e}")
    
    def fetch_all_stocks_from_baostock(self) -> pd.DataFrame:
        """
        从 baostock 获取所有A股列表及详细信息
        
        Returns:
            DataFrame with columns: symbol, name, exchange, ipo_date, out_date, 
                                   type, status, industry, industry_classification
        """
        if not self._baostock_login():
            raise Exception("无法登录 baostock")
        
        import baostock as bs
        from datetime import datetime, timedelta
        
        # 尝试获取最近交易日的数据
        today = datetime.now()
        stocks = []
        
        # 尝试今天和过去5天
        for days_back in range(0, 6):
            date_str = (today - timedelta(days=days_back)).strftime("%Y-%m-%d")
            rs = bs.query_all_stock(day=date_str)
            
            if rs.error_code == '0':
                temp_stocks = []
                while rs.next():
                    row = rs.get_row_data()
                    temp_stocks.append(row)
                
                if temp_stocks:  # 如果有数据，使用这个日期的结果
                    stocks = temp_stocks
                    logger.info(f"使用日期 {date_str} 获取到 {len(stocks)} 只股票")
                    break
        
        if not stocks:
            raise Exception("无法从 baostock 获取股票列表（尝试了最近5个交易日）")
        
        # 处理数据 - 只保留真正的A股股票（过滤指数、基金等）
        # baostock 返回格式: [code, status, name]，如 ['sh.600000', '1', '浦发银行']
        result = []
        for row in stocks:
            if len(row) < 3:
                continue
                
            code = row[0]      # 如 sh.600000
            # row[1] 是状态码 '1'，忽略
            name = row[2]      # 如 浦发银行
            
            # 提取数字代码
            if code.startswith("sh."):
                num_code = code.replace("sh.", "")
                exchange = "SH"
            elif code.startswith("sz."):
                num_code = code.replace("sz.", "")
                exchange = "SZ"
            else:
                continue
            
            # 过滤：只保留真正的A股股票
            # 上海: 600xxx, 601xxx, 603xxx, 605xxx, 688xxx(科创板)
            # 深圳: 000xxx, 001xxx, 002xxx, 003xxx, 300xxx, 301xxx(创业板)
            is_a_stock = False
            if exchange == "SH":
                if (num_code.startswith('600') or num_code.startswith('601') or 
                    num_code.startswith('603') or num_code.startswith('605') or
                    num_code.startswith('688')):
                    is_a_stock = True
            elif exchange == "SZ":
                if (num_code.startswith('000') or num_code.startswith('001') or
                    num_code.startswith('002') or num_code.startswith('003') or
                    num_code.startswith('300') or num_code.startswith('301')):
                    is_a_stock = True
            
            if not is_a_stock:
                continue
            
            symbol = num_code + "." + exchange
            
            result.append({
                'symbol': symbol,
                'code': code,  # 保留原始代码用于后续查询
                'name': name,
                'exchange': exchange,
            })
        
        df = pd.DataFrame(result)
        logger.info(f"从 baostock 获取到 {len(df)} 只A股")
        
        # 获取详细基本信息（上市日期、类型、状态等）
        df = self._enrich_stock_basic_info(df)
        
        # 获取行业分类
        df = self._enrich_stock_industry(df)
        
        return df
    
    def _enrich_stock_basic_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        通过 query_stock_basic 获取股票的详细基本信息
        
        Args:
            df: 包含 code 列的DataFrame
            
        Returns:
            添加了基本信息的DataFrame
        """
        import baostock as bs
        
        basic_info_list = []
        total = len(df)
        
        logger.info(f"正在获取 {total} 只股票的基本信息...")
        
        for idx, row in df.iterrows():
            code = row['code']  # sh.600000 格式
            
            try:
                rs = bs.query_stock_basic(code=code)
                if rs.error_code == '0' and rs.next():
                    data = rs.get_row_data()
                    # 返回: code, code_name, ipoDate, outDate, type, status
                    basic_info_list.append({
                        'symbol': row['symbol'],
                        'ipo_date': data[2] if len(data) > 2 else '',
                        'out_date': data[3] if len(data) > 3 else '',  # 退市日期
                        'security_type': data[4] if len(data) > 4 else '',  # 证券类型
                        'status': data[5] if len(data) > 5 else '',  # 上市状态
                    })
                else:
                    basic_info_list.append({
                        'symbol': row['symbol'],
                        'ipo_date': '',
                        'out_date': '',
                        'security_type': '',
                        'status': '',
                    })
            except Exception as e:
                logger.warning(f"获取 {code} 基本信息失败: {e}")
                basic_info_list.append({
                    'symbol': row['symbol'],
                    'ipo_date': '',
                    'out_date': '',
                    'security_type': '',
                    'status': '',
                })
            
            # 每100只打印进度
            if (idx + 1) % 100 == 0:
                logger.info(f"  进度: {idx + 1}/{total}")
        
        df_basic = pd.DataFrame(basic_info_list)
        df = df.merge(df_basic, on='symbol', how='left')
        
        logger.info(f"基本信息获取完成")
        return df
    
    def _enrich_stock_industry(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        通过 query_stock_industry 获取股票行业分类
        
        Args:
            df: 包含 code 列的DataFrame
            
        Returns:
            添加了行业信息的DataFrame
        """
        import baostock as bs
        
        logger.info(f"正在获取行业分类信息...")
        
        # 批量获取行业分类（不需要逐只查询）
        rs = bs.query_stock_industry()
        
        if rs.error_code != '0':
            logger.warning(f"查询行业分类失败: {rs.error_msg}")
            df['industry'] = ''
            df['industry_classification'] = ''
            df['industry_update_date'] = ''
            return df
        
        # 构建行业字典
        industry_dict = {}
        while rs.next():
            row = rs.get_row_data()
            # 格式: updateDate, code, code_name, industry, industryClassification
            if len(row) >= 5:
                bs_code = row[1]  # sh.600000
                if bs_code.startswith("sh."):
                    symbol = bs_code.replace("sh.", "") + ".SH"
                elif bs_code.startswith("sz."):
                    symbol = bs_code.replace("sz.", "") + ".SZ"
                else:
                    continue
                
                industry_dict[symbol] = {
                    'industry_update_date': row[0],
                    'industry': row[3],
                    'industry_classification': row[4],
                }
        
        # 合并到df
        df['industry'] = df['symbol'].map(lambda x: industry_dict.get(x, {}).get('industry', ''))
        df['industry_classification'] = df['symbol'].map(lambda x: industry_dict.get(x, {}).get('industry_classification', ''))
        df['industry_update_date'] = df['symbol'].map(lambda x: industry_dict.get(x, {}).get('industry_update_date', ''))
        
        logger.info(f"行业分类获取完成，共 {len([x for x in df['industry'] if x])} 条记录")
        return df
    
    def fetch_all_stocks_from_akshare(self) -> pd.DataFrame:
        """
        从 akshare 获取A股列表（作为备用/补充）
        
        Returns:
            DataFrame with basic stock info
        """
        try:
            import akshare as ak
            
            # 获取上海A股
            df_sh = ak.stock_sh_a_spot_em()
            df_sh['exchange'] = 'SH'
            
            # 获取深圳A股
            df_sz = ak.stock_sz_a_spot_em()
            df_sz['exchange'] = 'SZ'
            
            # 合并
            df = pd.concat([df_sh, df_sz], ignore_index=True)
            
            # 标准化列名
            column_mapping = {
                '代码': 'code',
                '名称': 'name',
                '所属行业': 'industry',
            }
            df = df.rename(columns=column_mapping)
            
            # 构建symbol
            df['symbol'] = df['code'] + '.' + df['exchange']
            
            # 选择需要的列，akshare字段有限
            df = df[['symbol', 'name', 'exchange', 'industry']]
            df['source'] = 'akshare'
            
            # akshare备用方案字段较少
            df['ipo_date'] = ''
            df['out_date'] = ''
            df['security_type'] = ''
            df['status'] = ''
            df['industry_classification'] = '东方财富行业'
            df['industry_update_date'] = ''
            df['code'] = ''  # 原始代码，akshare不需要
            
            logger.info(f"从 akshare 获取到 {len(df)} 只股票")
            return df
            
        except Exception as e:
            logger.error(f"从 akshare 获取失败: {e}")
            return pd.DataFrame()
    
    def build_database(self, use_cache: bool = False) -> str:
        """
        构建股票基本信息数据库
        
        Args:
            use_cache: 如果CSV已存在，是否直接使用缓存而不重新获取
            
        Returns:
            CSV文件的完整路径
        """
        if use_cache and self.csv_path.exists():
            logger.info(f"使用缓存的CSV文件: {self.csv_path}")
            return str(self.csv_path)
        
        print("\n" + "="*60)
        print("开始构建A股股票基本信息数据库")
        print("="*60)
        
        # 1. 尝试从 baostock 获取
        try:
            df_stocks = self.fetch_all_stocks_from_baostock()
            source = "baostock"
            
        except Exception as e:
            logger.warning(f"baostock 获取失败，尝试 akshare: {e}")
            df_stocks = self.fetch_all_stocks_from_akshare()
            source = "akshare"
        
        finally:
            self._baostock_logout()
        
        if df_stocks.empty:
            raise Exception("所有数据源均无法获取股票列表")
        
        # 2. 添加元信息
        df_stocks['update_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df_stocks['data_source'] = source
        
        # 3. 去重（如果有多条）
        df_stocks = df_stocks.drop_duplicates(subset=['symbol'], keep='first')
        
        # 4. 排序
        df_stocks = df_stocks.sort_values(['exchange', 'symbol'])
        
        # 5. 整理列顺序（重要的放前面）
        priority_columns = [
            'symbol', 'name', 'exchange',
            'industry', 'industry_classification',
            'ipo_date', 'out_date', 'status', 'security_type',
            'industry_update_date',
            'update_time', 'data_source'
        ]
        # 只保留存在的列
        final_columns = [col for col in priority_columns if col in df_stocks.columns]
        # 添加其他可能存在的列
        other_columns = [col for col in df_stocks.columns if col not in final_columns and col != 'code']
        final_columns.extend(other_columns)
        
        df_stocks = df_stocks[final_columns]
        
        # 6. 保存为CSV
        df_stocks.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
        
        print(f"\n✓ 股票数据库构建完成！")
        print(f"  - 数据条数: {len(df_stocks)}")
        print(f"  - 数据来源: {source}")
        print(f"  - 保存路径: {self.csv_path}")
        print(f"  - 更新时间: {df_stocks['update_time'].iloc[0]}")
        print("="*60)
        
        return str(self.csv_path)
    
    def load_database(self) -> pd.DataFrame:
        """
        加载股票数据库
        
        Returns:
            DataFrame with stock basic info
        """
        if not self.csv_path.exists():
            logger.warning(f"股票数据库不存在，请先运行 build_database()")
            return pd.DataFrame()
        
        return pd.read_csv(self.csv_path, encoding='utf-8-sig')
    
    def get_name_dict(self) -> dict:
        """
        获取股票代码到名称的映射字典
        
        Returns:
            dict: {symbol: name}
        """
        df = self.load_database()
        if df.empty:
            return {}
        
        return dict(zip(df['symbol'], df['name']))
    
    def search_stock(self, keyword: str) -> pd.DataFrame:
        """
        根据关键词搜索股票
        
        Args:
            keyword: 股票代码或名称关键字
            
        Returns:
            匹配的股票列表
        """
        df = self.load_database()
        if df.empty:
            return pd.DataFrame()
        
        # 代码匹配（精确或模糊）
        code_match = df[df['symbol'].str.contains(keyword, case=False, na=False)]
        
        # 名称匹配（模糊）
        name_match = df[df['name'].str.contains(keyword, case=False, na=False)]
        
        # 合并去重
        result = pd.concat([code_match, name_match]).drop_duplicates()
        
        return result


def main():
    """命令行入口"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    builder = StockDatabaseBuilder()
    
    # 检查是否有命令行参数
    import argparse
    parser = argparse.ArgumentParser(description='A股股票基本信息数据库构建工具')
    parser.add_argument('--force', '-f', action='store_true', 
                        help='强制重新构建，忽略缓存')
    parser.add_argument('--search', '-s', type=str,
                        help='搜索股票，传入关键词')
    parser.add_argument('--info', '-i', action='store_true',
                        help='显示数据库信息')
    
    args = parser.parse_args()
    
    if args.search:
        # 搜索模式
        result = builder.search_stock(args.search)
        if result.empty:
            print(f"未找到匹配 '{args.search}' 的股票")
        else:
            print(f"\n找到 {len(result)} 条匹配结果:")
            print(result.to_string(index=False))
    
    elif args.info:
        # 显示信息
        if builder.csv_path.exists():
            df = builder.load_database()
            stat = df['update_time'].iloc[0] if not df.empty else "未知"
            print(f"\n股票数据库信息:")
            print(f"  - 文件路径: {builder.csv_path}")
            print(f"  - 股票数量: {len(df)}")
            print(f"  - 更新时间: {stat}")
            print(f"  - 数据源: {df['data_source'].iloc[0] if not df.empty else '未知'}")
        else:
            print(f"\n数据库文件不存在: {builder.csv_path}")
            print("请运行: python -m DataHub.build_stock_db")
    
    else:
        # 构建模式
        try:
            builder.build_database(use_cache=not args.force)
        except Exception as e:
            print(f"\n✗ 构建失败: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
