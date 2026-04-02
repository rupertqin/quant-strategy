#!/usr/bin/env python3
"""
项目重构脚本
- 将 ShortTerm 重命名为 DailySignal (今日异动)
- 创建 PoolWatch (股票池监控)
"""

import os
import shutil
from pathlib import Path

def main():
    base_dir = Path(__file__).parent
    
    # 1. 重命名 ShortTerm -> DailySignal
    short_term = base_dir / "ShortTerm"
    daily_signal = base_dir / "DailySignal"
    
    if short_term.exists() and not daily_signal.exists():
        shutil.move(str(short_term), str(daily_signal))
        print(f"✓ 重命名: ShortTerm -> DailySignal")
    
    # 2. 创建 PoolWatch 目录结构
    pool_watch = base_dir / "PoolWatch"
    pool_watch.mkdir(exist_ok=True)
    
    # 创建基本文件
    init_file = pool_watch / "__init__.py"
    if not init_file.exists():
        init_file.write_text("""\"\"\"PoolWatch - 股票池短线监控模块\"\"\"
""")
    
    readme_file = pool_watch / "README.md"
    if not readme_file.exists():
        readme_file.write_text("""# PoolWatch - 股票池短线监控

监控 LongTerm 股票池的短线技术指标。
""")
    
    print(f"✓ 创建: PoolWatch/")
    print("\n重构完成!")
    print("请手动删除本脚本: rm restructure.py")

if __name__ == "__main__":
    main()
