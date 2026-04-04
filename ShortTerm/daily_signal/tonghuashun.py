"""
同花顺数据中心数据获取
"""
import requests
import re
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)

# 统一的请求配置
_THS_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Cookie': 'vvvv=1; v=A61tP1CEuNxYRFww286hDMPQukIiCuHcaz5FsO-y6cSzZsO8t1rxrPuOVYF8',
    'Pragma': 'no-cache',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
}

_THS_BASE_URL = 'https://data.10jqka.com.cn/funds/ggzjl/field/zdf/order/{order}/page/{page}/ajax/1/free/1/'


def _fetch_ths_data(order: str, threshold: float, color_class: str, limit_type: str) -> dict:
    """
    统一获取同花顺涨跌停数据

    Args:
        order: 排序方式 'asc'(升序/跌停) 或 'desc'(降序/涨停)
        threshold: 涨跌幅阈值 (如 9.5 表示涨幅>=9.5%或跌幅<=-9.5%)
        color_class: CSS类名 'c-rise'(上涨红色) 或 'c-fall'(下跌绿色)
        limit_type: 类型描述 '涨停' 或 '跌停'

    Returns:
        {'count': int, 'source': str, 'note': str}
    """
    count = 0
    max_pages = 10

    try:
        for page in range(1, max_pages + 1):
            url = _THS_BASE_URL.format(order=order, page=page)

            resp = requests.get(url, headers=_THS_HEADERS, timeout=15)
            resp.encoding = 'gbk'

            if resp.status_code != 200:
                logger.warning(f"同花顺接口返回状态码: {resp.status_code}")
                break

            soup = BeautifulSoup(resp.text, 'html.parser')
            table = soup.find('table', class_='m-table')

            if not table:
                logger.warning(f"第{page}页没有找到数据表格")
                break

            rows = table.find('tbody').find_all('tr') if table.find('tbody') else []

            page_count = 0
            page_has_data = False

            for row in rows:
                zdf_td = row.find('td', class_=color_class)
                if zdf_td:
                    page_has_data = True
                    zdf_text = zdf_td.text.strip()
                    match = re.search(r'(-?\d+\.?\d*)%', zdf_text)
                    if match:
                        zdf = float(match.group(1))

                        # 根据类型判断阈值
                        if limit_type == '涨停' and zdf >= threshold:
                            count += 1
                            page_count += 1
                        elif limit_type == '跌停' and zdf <= -threshold:
                            count += 1
                            page_count += 1
                        else:
                            # 未达到阈值，停止查询
                            logger.info(f"同花顺数据: 第{page}页发现涨跌幅{zdf}%不满足条件，停止查询")
                            return {
                                'count': count,
                                'source': f'同花顺数据中心(共查询{page}页)',
                                'note': f'统计{limit_type}>=|{threshold}%|的股票'
                            }

            if not page_has_data:
                logger.info(f"同花顺数据: 第{page}页无数据，停止查询")
                break

            logger.info(f"同花顺数据: 第{page}页统计到{page_count}家{limit_type}，累计{count}家")

            # 如果这一页没有符合条件的股票，停止查询
            if page_count == 0 and count > 0:
                break

        return {
            'count': count,
            'source': '同花顺数据中心',
            'note': f'统计{limit_type}>=|{threshold}%|的股票'
        }

    except Exception as e:
        logger.error(f"获取同花顺{limit_type}数据失败: {e}")
        return {'count': 0, 'source': f'同花顺({limit_type}失败)', 'note': str(e)}


def get_limit_down_count_from_ths() -> dict:
    """
    从同花顺数据中心获取跌停股票数量
    按涨跌幅升序排序(asc)，统计跌幅超过-9.5%的股票数量

    Returns:
        {'dt_count': int, 'source': str, 'note': str}
    """
    result = _fetch_ths_data(
        order='asc',
        threshold=9.5,
        color_class='c-fall',
        limit_type='跌停'
    )
    return {
        'dt_count': result['count'],
        'source': result['source'],
        'note': result['note']
    }


def get_limit_up_count_from_ths() -> dict:
    """
    从同花顺数据中心获取涨停股票数量
    按涨跌幅降序排序(desc)，统计涨幅超过9.5%的股票数量

    Returns:
        {'zt_count': int, 'source': str, 'note': str}
    """
    result = _fetch_ths_data(
        order='desc',
        threshold=9.5,
        color_class='c-rise',
        limit_type='涨停'
    )
    return {
        'zt_count': result['count'],
        'source': result['source'],
        'note': result['note']
    }


if __name__ == '__main__':
    # 测试
    print("获取跌停数据...")
    dt_result = get_limit_down_count_from_ths()
    print(f"跌停: {dt_result}")

    print("\n获取涨停数据...")
    zt_result = get_limit_up_count_from_ths()
    print(f"涨停: {zt_result}")
