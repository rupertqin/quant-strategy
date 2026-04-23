#!/usr/bin/env python3
"""
资产管理器 - 添加/删除股票、指数、ETF

功能:
    1. 添加资产: 自动下载历史数据并入库
    2. 删除资产: 清理历史数据并从列表移除
    3. 自动识别资产类型（股票/指数/ETF）

用法:
    # 添加资产（自动识别类型）
    python DataHub/scripts/asset_manager.py add 600519.SH
    python DataHub/scripts/asset_manager.py add 000001.SH  # 自动识别为指数
    python DataHub/scripts/asset_manager.py add 510300.SH  # 自动识别为ETF

    # 添加恒生指数
    python DataHub/services/history_sync.py --symbol HSI.HK --override
    python DataHub/services/history_sync.py --symbol HSTECH.HK --override

    # 添加资产（手动指定类型）
    python DataHub/scripts/asset_manager.py add 600519.SH --type stock
    python DataHub/scripts/asset_manager.py add 000001.SH --type index
    python DataHub/scripts/asset_manager.py add 510300.SH --type etf

    # 删除资产
    python DataHub/scripts/asset_manager.py remove 600519.SH
    python DataHub/scripts/asset_manager.py remove 000001.SH --type index

    # 列出所有资产
    python DataHub/scripts/asset_manager.py list
    python DataHub/scripts/asset_manager.py list --type stock
    python DataHub/scripts/asset_manager.py list --type etf
"""

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Optional, Tuple
import pandas as pd
from datetime import datetime

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from lib.utils import detect_asset_type, StockCodeUtil
from DataHub.config import RAW_PRICE_DIR, RAW_ETF_PRICE_DIR, RAW_INDEX_PRICE_DIR


class AssetManager:
    """资产管理器"""

    # 资产类型配置
    ASSET_CONFIG = {
        'stock': {
            'name': '股票',
            'csv_file': 'storage/stock_basic_info.csv',
            'data_dir': RAW_PRICE_DIR,
            'required_cols': ['symbol', 'code', 'name', 'exchange', 'industry', 'list_date']
        },
        'etf': {
            'name': 'ETF',
            'csv_file': 'storage/etf_basic_info.csv',
            'data_dir': RAW_ETF_PRICE_DIR,
            'required_cols': ['symbol', 'code', 'name', 'exchange', 'etf_type']
        },
        'index': {
            'name': '指数',
            'csv_file': 'storage/official_indices.csv',
            'data_dir': RAW_INDEX_PRICE_DIR,
            'required_cols': ['symbol', 'code', 'name', 'market', 'category']
        }
    }

    def __init__(self):
        self.project_root = project_root

    def detect_type(self, symbol: str) -> Optional[str]:
        """
        自动识别资产类型

        Returns:
            'stock' / 'etf' / 'index' / None
        """
        # 1. 检查是否在指数列表中
        index_csv = self.project_root / self.ASSET_CONFIG['index']['csv_file']
        if index_csv.exists():
            df = pd.read_csv(index_csv)
            if 'symbol' in df.columns:
                if symbol in df['symbol'].values:
                    return 'index'
            elif '\ufeffsymbol' in df.columns:
                if symbol in df['\ufeffsymbol'].values:
                    return 'index'

        # 2. 检查是否在ETF列表中
        etf_csv = self.project_root / self.ASSET_CONFIG['etf']['csv_file']
        if etf_csv.exists():
            df = pd.read_csv(etf_csv)
            if symbol in df['symbol'].values:
                return 'etf'

        # 3. 检查是否在股票列表中
        stock_csv = self.project_root / self.ASSET_CONFIG['stock']['csv_file']
        if stock_csv.exists():
            df = pd.read_csv(stock_csv)
            if symbol in df['symbol'].values:
                return 'stock'

        # 4. 根据代码规则判断
        code = symbol.split('.')[0] if '.' in symbol else symbol

        # ETF规则
        if code.startswith(('51', '56', '58', '15', '16', '18')):
            return 'etf'

        # 指数规则（以000/399开头但不在股票列表）
        if code.startswith(('000', '399', '980')):
            return 'index'

        # 默认股票
        if len(code) == 6 and code.isdigit():
            return 'stock'

        return None

    def _get_basic_info(self, symbol: str, asset_type: str) -> Optional[dict]:
        """获取资产基本信息"""
        try:
            import akshare as ak

            code = symbol.split('.')[0] if '.' in symbol else symbol
            exchange = symbol.split('.')[1] if '.' in symbol else ''

            if asset_type == 'stock':
                # 使用 akshare 获取股票信息
                try:
                    df = ak.stock_individual_info_em(symbol=code)
                    if not df.empty:
                        info = {}
                        for _, row in df.iterrows():
                            info[row['item']] = row['value']
                        return {
                            'symbol': symbol,
                            'code': code,
                            'name': info.get('股票简称', code),
                            'exchange': 'SH' if exchange == 'SH' else 'SZ' if exchange == 'SZ' else 'BJ',
                            'industry': info.get('所属行业', '未知'),
                            'list_date': info.get('上市时间', '')
                        }
                except Exception:
                    pass

                # 简化版信息
                return {
                    'symbol': symbol,
                    'code': code,
                    'name': code,
                    'exchange': 'SH' if exchange == 'SH' else 'SZ' if exchange == 'SZ' else 'BJ',
                    'industry': '未知',
                    'list_date': ''
                }

            elif asset_type == 'etf':
                # ETF简化信息
                return {
                    'symbol': symbol,
                    'code': code,
                    'name': code,
                    'exchange': 'SH' if exchange == 'SH' else 'SZ',
                    'etf_type': '股票型'
                }

            elif asset_type == 'index':
                # 指数简化信息
                return {
                    'symbol': symbol,
                    'code': code,
                    'name': code,
                    'market': '上海' if exchange == 'SH' else '深圳',
                    'category': '规模指数'
                }

        except Exception as e:
            print(f"  ⚠️ 获取基本信息失败: {e}")

        return None

    def _download_history(self, symbol: str, asset_type: str) -> bool:
        """下载历史数据"""
        print(f"  📥 下载历史数据...")

        try:
            import akshare as ak

            code = symbol.split('.')[0] if '.' in symbol else symbol
            exchange = symbol.split('.')[1] if '.' in symbol else ''

            # 根据类型选择下载方式
            if asset_type == 'stock':
                # akshare 股票格式: sh600519 / sz000001
                ak_code = f"{'sh' if exchange == 'SH' else 'sz'}{code}"
                df = ak.stock_zh_a_hist(
                    symbol=code,
                    period="daily",
                    start_date="19700101",
                    end_date=datetime.now().strftime('%Y%m%d'),
                    adjust="qfq"
                )
            elif asset_type == 'etf':
                # ETF使用股票接口
                ak_code = f"{'sh' if exchange == 'SH' else 'sz'}{code}"
                df = ak.stock_zh_a_hist(
                    symbol=code,
                    period="daily",
                    start_date="19700101",
                    end_date=datetime.now().strftime('%Y%m%d'),
                    adjust="qfq"
                )
            elif asset_type == 'index':
                # 指数接口
                if code.startswith('000') or code.startswith('980'):
                    ak_code = f"sh{code}"
                else:
                    ak_code = f"sz{code}"
                df = ak.index_zh_a_hist(
                    symbol=code,
                    period="daily",
                    start_date="19700101",
                    end_date=datetime.now().strftime('%Y%m%d')
                )
            else:
                return False

            if df.empty:
                print(f"  ⚠️ 未获取到数据")
                return False

            # 标准化列名
            if asset_type in ['stock', 'etf']:
                df = df.rename(columns={
                    '日期': 'date',
                    '开盘': 'open',
                    '收盘': 'close',
                    '最高': 'high',
                    '最低': 'low',
                    '成交量': 'volume',
                    '成交额': 'amount',
                    '振幅': 'amplitude',
                    '涨跌幅': 'change_pct',
                    '涨跌额': 'change',
                    '换手率': 'turnover'
                })
            else:  # index
                df = df.rename(columns={
                    '日期': 'date',
                    '开盘': 'open',
                    '收盘': 'close',
                    '最高': 'high',
                    '最低': 'low',
                    '成交量': 'volume',
                    '成交额': 'amount',
                    '振幅': 'amplitude',
                    '涨跌幅': 'change_pct',
                    '涨跌额': 'change'
                })

            df['symbol'] = symbol

            # 确保数据目录存在
            data_dir = self.ASSET_CONFIG[asset_type]['data_dir']
            data_dir.mkdir(parents=True, exist_ok=True)

            # 保存为 parquet
            output_file = data_dir / f"{symbol}.parquet"
            df.to_parquet(output_file, index=False)

            print(f"  ✓ 已下载 {len(df)} 条记录 -> {output_file}")
            return True

        except Exception as e:
            print(f"  ✗ 下载失败: {e}")
            return False

    def _add_to_csv(self, symbol: str, asset_type: str, info: dict) -> bool:
        """添加到代码列表CSV"""
        csv_file = self.project_root / self.ASSET_CONFIG[asset_type]['csv_file']

        try:
            # 读取现有数据
            if csv_file.exists():
                df = pd.read_csv(csv_file)
                # 处理 BOM
                if '\ufeffsymbol' in df.columns:
                    df = df.rename(columns={'\ufeffsymbol': 'symbol'})
            else:
                df = pd.DataFrame(columns=self.ASSET_CONFIG[asset_type]['required_cols'])

            # 检查是否已存在
            if symbol in df['symbol'].values:
                print(f"  ℹ️ {symbol} 已在列表中")
                return True

            # 添加新行
            new_row = {col: info.get(col, '') for col in df.columns}
            new_row['symbol'] = symbol
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

            # 保存
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            print(f"  ✓ 已添加到 {csv_file}")
            return True

        except Exception as e:
            print(f"  ✗ 添加到CSV失败: {e}")
            return False

    def _remove_from_csv(self, symbol: str, asset_type: str) -> bool:
        """从代码列表CSV移除"""
        csv_file = self.project_root / self.ASSET_CONFIG[asset_type]['csv_file']

        try:
            if not csv_file.exists():
                return True

            df = pd.read_csv(csv_file)
            # 处理 BOM
            if '\ufeffsymbol' in df.columns:
                df = df.rename(columns={'\ufeffsymbol': 'symbol'})

            # 删除
            df = df[df['symbol'] != symbol]

            # 保存
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            print(f"  ✓ 已从 {csv_file} 移除")
            return True

        except Exception as e:
            print(f"  ✗ 从CSV移除失败: {e}")
            return False

    def _remove_history(self, symbol: str, asset_type: str) -> bool:
        """删除历史数据文件"""
        data_dir = self.ASSET_CONFIG[asset_type]['data_dir']
        data_file = data_dir / f"{symbol}.parquet"

        try:
            if data_file.exists():
                data_file.unlink()
                print(f"  ✓ 已删除 {data_file}")
            return True
        except Exception as e:
            print(f"  ⚠️ 删除历史数据失败: {e}")
            return False

    def add(self, symbol: str, asset_type: Optional[str] = None) -> bool:
        """
        添加资产

        Args:
            symbol: 资产代码，如 600519.SH
            asset_type: 资产类型 stock/etf/index，None则自动识别

        Returns:
            bool: 是否成功
        """
        print(f"\n{'='*60}")
        print(f"➕ 添加资产: {symbol}")
        print('='*60)

        # 标准化代码
        symbol = symbol.upper()
        if '.' not in symbol:
            # 尝试添加后缀
            symbol = StockCodeUtil.with_suffix(symbol) or symbol

        # 识别类型
        if asset_type is None:
            detected = self.detect_type(symbol)
            if detected is None:
                print(f"  ✗ 无法识别 {symbol} 的类型，请手动指定 --type")
                return False
            asset_type = detected
            print(f"  🔍 自动识别类型: {self.ASSET_CONFIG[asset_type]['name']}")
        else:
            asset_type = asset_type.lower()
            if asset_type not in self.ASSET_CONFIG:
                print(f"  ✗ 不支持的类型: {asset_type}")
                return False
            print(f"  📌 指定类型: {self.ASSET_CONFIG[asset_type]['name']}")

        # 获取基本信息
        print(f"  📋 获取基本信息...")
        info = self._get_basic_info(symbol, asset_type)
        if info:
            print(f"     名称: {info.get('name', '未知')}")

        # 下载历史数据
        if not self._download_history(symbol, asset_type):
            print(f"  ⚠️ 历史数据下载失败，继续添加到列表...")

        # 添加到CSV
        if info:
            if not self._add_to_csv(symbol, asset_type, info):
                return False

        print(f"  ✅ {symbol} 添加完成")
        return True

    def remove(self, symbol: str, asset_type: Optional[str] = None) -> bool:
        """
        删除资产

        Args:
            symbol: 资产代码
            asset_type: 资产类型，None则自动识别

        Returns:
            bool: 是否成功
        """
        print(f"\n{'='*60}")
        print(f"➖ 删除资产: {symbol}")
        print('='*60)

        # 标准化代码
        symbol = symbol.upper()

        # 识别类型
        if asset_type is None:
            detected = self.detect_type(symbol)
            if detected is None:
                print(f"  ⚠️ {symbol} 不在任何列表中，无需删除")
                return True
            asset_type = detected
            print(f"  🔍 自动识别类型: {self.ASSET_CONFIG[asset_type]['name']}")
        else:
            asset_type = asset_type.lower()
            print(f"  📌 指定类型: {self.ASSET_CONFIG[asset_type]['name']}")

        # 删除历史数据
        self._remove_history(symbol, asset_type)

        # 从CSV移除
        self._remove_from_csv(symbol, asset_type)

        print(f"  ✅ {symbol} 删除完成")
        return True

    def list_assets(self, asset_type: Optional[str] = None):
        """列出所有资产"""
        print(f"\n{'='*60}")
        print(f"📋 资产列表")
        print('='*60)

        types_to_list = [asset_type] if asset_type else ['stock', 'etf', 'index']

        for t in types_to_list:
            if t not in self.ASSET_CONFIG:
                continue

            csv_file = self.project_root / self.ASSET_CONFIG[t]['csv_file']
            if not csv_file.exists():
                continue

            try:
                df = pd.read_csv(csv_file)
                if '\ufeffsymbol' in df.columns:
                    df = df.rename(columns={'\ufeffsymbol': 'symbol'})

                print(f"\n【{self.ASSET_CONFIG[t]['name']}】共 {len(df)} 只")

                if len(df) > 0:
                    # 显示前10只
                    for _, row in df.head(10).iterrows():
                        name = row.get('name', '')
                        symbol = row.get('symbol', '')
                        print(f"  {symbol:<15} {name}")

                    if len(df) > 10:
                        print(f"  ... 还有 {len(df)-10} 只")

            except Exception as e:
                print(f"  ⚠️ 读取 {t} 列表失败: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='资产管理器 - 添加/删除股票、指数、ETF',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 添加资产（自动识别类型）
  python asset_manager.py add 600519.SH
  python asset_manager.py add 000001.SH
  python asset_manager.py add 510300.SH

  # 添加资产（手动指定类型）
  python asset_manager.py add 600519.SH --type stock
  python asset_manager.py add 000001.SH --type index
  python asset_manager.py add 510300.SH --type etf

  # 删除资产
  python asset_manager.py remove 600519.SH

  # 列出所有资产
  python asset_manager.py list
  python asset_manager.py list --type stock
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='命令')

    # add 命令
    add_parser = subparsers.add_parser('add', help='添加资产')
    add_parser.add_argument('symbol', help='资产代码，如 600519.SH')
    add_parser.add_argument('--type', choices=['stock', 'etf', 'index'],
                          help='资产类型（自动识别）')

    # remove 命令
    remove_parser = subparsers.add_parser('remove', help='删除资产')
    remove_parser.add_argument('symbol', help='资产代码')
    remove_parser.add_argument('--type', choices=['stock', 'etf', 'index'],
                             help='资产类型（自动识别）')

    # list 命令
    list_parser = subparsers.add_parser('list', help='列出资产')
    list_parser.add_argument('--type', choices=['stock', 'etf', 'index'],
                           help='只列出指定类型')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    manager = AssetManager()

    if args.command == 'add':
        success = manager.add(args.symbol, args.type)
        sys.exit(0 if success else 1)

    elif args.command == 'remove':
        success = manager.remove(args.symbol, args.type)
        sys.exit(0 if success else 1)

    elif args.command == 'list':
        manager.list_assets(args.type)


if __name__ == "__main__":
    main()
