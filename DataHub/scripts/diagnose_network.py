#!/usr/bin/env python3
"""
网络诊断脚本 - 检查数据源连通性
"""

import os
import socket
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def check_dns(hostname):
    """检查 DNS 解析"""
    try:
        ip = socket.gethostbyname(hostname)
        print(f"✓ DNS 解析: {hostname} -> {ip}")
        return True, ip
    except Exception as e:
        print(f"✗ DNS 解析失败: {hostname} - {e}")
        return False, None


def check_tcp_connection(host, port, timeout=10):
    """检查 TCP 连接"""
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        print(f"✓ TCP 连接: {host}:{port}")
        return True
    except Exception as e:
        print(f"✗ TCP 连接失败: {host}:{port} - {e}")
        return False


def check_akshare():
    """检查 akshare 数据源"""
    print("\n" + "="*50)
    print("检查 AkShare 数据源")
    print("="*50)
    
    try:
        import akshare as ak
        print("✓ akshare 模块已安装")
    except ImportError:
        print("✗ akshare 未安装")
        return False
    
    # 尝试获取数据
    print("\n尝试获取数据 (000001.SZ 5日线)...")
    try:
        df = ak.stock_zh_a_hist(symbol="000001", period="daily", 
                                start_date="20240101", end_date="20240110",
                                adjust="")
        if df is not None and not df.empty:
            print(f"✓ 成功获取数据: {len(df)} 条")
            return True
        else:
            print("✗ 返回空数据")
            return False
    except Exception as e:
        print(f"✗ 获取失败: {type(e).__name__}: {e}")
        return False


def check_baostock():
    """检查 baostock 数据源"""
    print("\n" + "="*50)
    print("检查 BaoStock 数据源")
    print("="*50)
    
    try:
        import baostock as bs
        print("✓ baostock 模块已安装")
    except ImportError:
        print("✗ baostock 未安装")
        return False
    
    # 尝试登录
    print("\n尝试登录...")
    try:
        lg = bs.login()
        if lg.error_code != "0":
            print(f"✗ 登录失败: {lg.error_msg}")
            return False

        print(f"✓ 登录成功")
        
        # 尝试查询
        print("\n尝试查询数据 (sh.000001)...")
        try:
            rs = bs.query_history_k_data_plus("sh.000001", "date,close",
                                              start_date="2024-01-01",
                                              end_date="2024-01-10")
            if rs.error_code == "0":
                count = 0
                while rs.next():
                    count += 1
                print(f"✓ 成功获取数据: {count} 条")
                return True
            else:
                print(f"✗ 查询失败: {rs.error_msg}")
                return False
        finally:
            bs.logout()
    except Exception as e:
        print(f"✗ 操作失败: {type(e).__name__}: {e}")
        return False


def check_proxy():
    """检查代理设置"""
    print("\n" + "="*50)
    print("检查代理设置")
    print("="*50)
    
    proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy',
                  'ALL_PROXY', 'all_proxy']
    
    has_proxy = False
    for var in proxy_vars:
        value = os.environ.get(var)
        if value:
            print(f"  {var}={value}")
            has_proxy = True
    
    if not has_proxy:
        print("  未发现代理环境变量")
    
    return has_proxy


def check_local_cache():
    """检查本地缓存数据"""
    print("\n" + "="*50)
    print("检查本地缓存数据")
    print("="*50)
    
    storage_path = Path(__file__).parent.parent.parent / "storage"
    
    # 检查价格数据
    price_file = storage_path / "raw" / "prices" / "prices.parquet"
    if price_file.exists():
        try:
            import pandas as pd
            df = pd.read_parquet(price_file)
            
            # 检查数据有效性
            if df.empty or len(df.index) == 0:
                print(f"△ 价格缓存存在但为空: {price_file}")
            else:
                print(f"✓ 价格缓存: {price_file}")
                print(f"  形状: {df.shape}")
                print(f"  日期范围: {df.index[0]} ~ {df.index[-1]}")
                print(f"  股票数量: {len(df.columns)}")
        except Exception as e:
            print(f"✗ 读取价格缓存失败: {type(e).__name__}: {e}")
    else:
        print(f"✗ 价格缓存不存在: {price_file}")
    
    # 检查涨停池数据
    zt_dir = storage_path / "raw" / "zt_pool"
    if zt_dir.exists():
        try:
            files = list(zt_dir.glob("*.parquet"))
            print(f"\n✓ 涨停池缓存: {len(files)} 个文件")
            for f in sorted(files)[-3:]:
                print(f"  - {f.name}")
        except Exception as e:
            print(f"\n✗ 读取涨停池缓存失败: {type(e).__name__}: {e}")
    else:
        print(f"\n✗ 涨停池缓存不存在")


def main():
    print("="*50)
    print("量化交易系统 - 网络诊断")
    print("="*50)
    
    # 1. 检查代理
    check_proxy()
    
    # 2. 检查网络连接
    print("\n" + "="*50)
    print("检查网络连接")
    print("="*50)
    
    # 常见数据源域名
    hosts = [
        ("baostock.com", 80),
        ("www.baostock.com", 80),
        ("query.sse.com.cn", 80),
        ("query.sse.com.cn", 443),
        ("www.szse.cn", 443),
    ]
    
    for host, port in hosts:
        ok, ip = check_dns(host)
        if ok:
            check_tcp_connection(ip, port)
        time.sleep(0.5)
    
    # 3. 检查数据源
    ak_ok = check_akshare()
    bs_ok = check_baostock()
    
    # 4. 检查本地缓存
    check_local_cache()
    
    # 总结
    print("\n" + "="*50)
    print("诊断总结")
    print("="*50)
    
    if ak_ok and bs_ok:
        print("✓ 所有数据源正常")
        return 0
    elif ak_ok:
        print("△ AkShare 正常，BaoStock 异常")
        print("  建议: 可以仅使用 AkShare 作为数据源")
        return 0
    elif bs_ok:
        print("△ BaoStock 正常，AkShare 异常")
        print("  建议: 可以仅使用 BaoStock 作为数据源")
        return 0
    else:
        print("✗ 所有网络数据源都不可用")
        print("\n可能原因:")
        print("  1. 网络连接问题（检查网络/WiFi）")
        print("  2. 防火墙阻挡（企业网络常见）")
        print("  3. 数据源服务器维护")
        print("  4. 需要使用代理")
        print("\n解决方案:")
        print("  1. 检查网络连接")
        print("  2. 尝试使用手机热点")
        print("  3. 配置代理（如果需要）")
        print("  4. 使用本地缓存数据进行回测")
        return 1


if __name__ == "__main__":
    sys.exit(main())
