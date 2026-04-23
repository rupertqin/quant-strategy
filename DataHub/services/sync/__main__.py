"""
数据同步服务 - 直接运行模块

用法:
    python -m DataHub.services.sync --today
    python -m DataHub.services.sync --sync-factors
"""

from .cli import main

if __name__ == '__main__':
    main()
