# DataHub 模块测试包

import sys
from pathlib import Path

# 将项目根目录加入 sys.path，确保测试可以导入 DataHub 包
_PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
