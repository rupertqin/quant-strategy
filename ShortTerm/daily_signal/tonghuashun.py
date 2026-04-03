"""
同花顺数据中心数据获取
"""
import requests
import re
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)


def get_limit_down_count_from_ths() -> dict:
    """
    从同花顺数据中心获取跌停股票数量
    按涨跌幅排序，统计跌幅超过-10%的股票数量
    
    Returns:
        {'dt_count': int, 'source': str}
    """
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Cookie': 'refreshStat=off; v=A9kZs-Qo1IbW2YgSnrM9SA907s62ZsSft2Hxx_uOV8N7TPcwQ7bd6EeqAUOI',
        'Pragma': 'no-cache',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"'
    }
    
    dt_count = 0
    max_pages = 10  # 最多查询10页
    
    try:
        for page in range(1, max_pages + 1):
            url = f'https://data.10jqka.com.cn/funds/ggzjl/field/zdf/order/asc/page/{page}/ajax/1/free/1/'
            
            resp = requests.get(url, headers=headers, timeout=15)
            resp.encoding = 'gbk'  # 同花顺使用GBK编码
            
            if resp.status_code != 200:
                logger.warning(f"同花顺接口返回状态码: {resp.status_code}")
                break
            
            # 解析HTML
            soup = BeautifulSoup(resp.text, 'html.parser')
            table = soup.find('table', class_='m-table')
            
            if not table:
                logger.warning(f"第{page}页没有找到数据表格")
                break
            
            # 提取涨跌幅数据
            rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
            
            page_dt_count = 0
            page_has_data = False
            
            for row in rows:
                # 查找涨跌幅列
                zdf_td = row.find('td', class_='c-fall')
                if zdf_td:
                    page_has_data = True
                    zdf_text = zdf_td.text.strip()
                    # 提取百分比数字，例如 "-11.39%" -> -11.39
                    match = re.search(r'(-?\d+\.?\d*)%', zdf_text)
                    if match:
                        zdf = float(match.group(1))
                        # 跌停定义：跌幅 >= 10% (创业板/科创板) 或 >= 9.9% (主板)
                        # 同花顺数据用-10%作为界限
                        if zdf <= -9.5:
                            dt_count += 1
                            page_dt_count += 1
                        else:
                            # 涨跌幅已经大于-9.5%，后面的股票跌幅会更小，直接返回
                            logger.info(f"同花顺数据: 第{page}页发现涨跌幅{zdf}%>-9.5%，停止查询")
                            return {
                                'dt_count': dt_count,
                                'source': f'同花顺数据中心(共查询{page}页)',
                                'note': f'统计跌幅>=9.5%的股票'
                            }
            
            if not page_has_data:
                logger.info(f"同花顺数据: 第{page}页无数据，停止查询")
                break
            
            logger.info(f"同花顺数据: 第{page}页统计到{page_dt_count}家跌停，累计{dt_count}家")
            
            # 如果这一页没有跌停股票了，后面的页也不会有
            if page_dt_count == 0 and dt_count > 0:
                break
        
        return {
            'dt_count': dt_count,
            'source': f'同花顺数据中心',
            'note': f'统计跌幅>=9.5%的股票'
        }
        
    except Exception as e:
        logger.error(f"获取同花顺数据失败: {e}")
        return {'dt_count': 0, 'source': '同花顺(失败)', 'note': str(e)}


def get_limit_up_count_from_ths() -> dict:
    """
    从同花顺数据中心获取涨停股票数量
    按涨跌幅倒序排序，统计涨幅超过10%的股票数量
    """
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Cookie': 'refreshStat=off; v=A9kZs-Qo1IbW2YgSnrM9SA907s62ZsSft2Hxx_uOV8N7TPcwQ7bd6EeqAUOI',
        'Pragma': 'no-cache',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"'
    }
    
    zt_count = 0
    max_pages = 10
    
    try:
        for page in range(1, max_pages + 1):
            # 注意：涨停用order/desc倒序排列
            url = f'https://data.10jqka.com.cn/funds/ggzjl/field/zdf/order/desc/page/{page}/ajax/1/free/1/'
            
            resp = requests.get(url, headers=headers, timeout=15)
            resp.encoding = 'gbk'
            
            if resp.status_code != 200:
                break
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            table = soup.find('table', class_='m-table')
            
            if not table:
                break
            
            rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
            
            page_zt_count = 0
            page_has_data = False
            
            for row in rows:
                # 涨停用c-rise类
                zdf_td = row.find('td', class_='c-rise')
                if zdf_td:
                    page_has_data = True
                    zdf_text = zdf_td.text.strip()
                    match = re.search(r'(-?\d+\.?\d*)%', zdf_text)
                    if match:
                        zdf = float(match.group(1))
                        if zdf >= 9.5:
                            zt_count += 1
                            page_zt_count += 1
                        else:
                            return {
                                'zt_count': zt_count,
                                'source': f'同花顺数据中心(共查询{page}页)',
                                'note': f'统计涨幅>=9.5%的股票'
                            }
            
            if not page_has_data or (page_zt_count == 0 and zt_count > 0):
                break
        
        return {
            'zt_count': zt_count,
            'source': f'同花顺数据中心',
            'note': f'统计涨幅>=9.5%的股票'
        }
        
    except Exception as e:
        logger.error(f"获取同花顺涨停数据失败: {e}")
        return {'zt_count': 0, 'source': '同花顺(失败)', 'note': str(e)}


if __name__ == '__main__':
    # 测试
    print("获取跌停数据...")
    dt_result = get_limit_down_count_from_ths()
    print(f"跌停: {dt_result}")
    
    print("\n获取涨停数据...")
    zt_result = get_limit_up_count_from_ths()
    print(f"涨停: {zt_result}")
