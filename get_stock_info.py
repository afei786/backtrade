import requests
import re
import time
import random
from datetime import datetime
import jqdatasdk
from jqdatasdk import *
import random
import time
from pysql import PySQL

def get_stock_info(international_code: str, start_date, end_date, max_retries=3) -> dict:
    """
    输入如 600519.SH，返回包含指定字段的字典，自动爬取东方财富网相关数据。
    增加重试机制，提高稳定性。
    """
    # 1. 解析交易所
    symbol = international_code.split('.')[0]
    if international_code.endswith('.XSHG'):
        eastmoney_prefix = '1'  # 东方财富 1 开头为上交所
    elif international_code.endswith('.XSHE'):
        eastmoney_prefix = '0'  # 东方财富 0 开头为深交所
    else:
        raise ValueError('市场类型错误，应为 "XSHE" 或 "XSHG"')
    
    # 东方财富股票详情接口
    url = f"https://push2.eastmoney.com/api/qt/stock/get"
    params = {
        'invt': '2',
        'fltt': '2',
        'fields': 'f2,f20,f57,f58,f86,f97,f102,f116,f117,f118,f137,f138,f140,f141,f142,f143,f144,f145,f146,f147,f148,f149,f152,f124,f125,f126,f127,f128,f129,f130,f131,f132,f133,f134,f135,f136',
        'secid': f"{eastmoney_prefix}.{symbol}",
        '_': int(datetime.now().timestamp() * 1000)
    }
    
    # 随机User-Agent列表
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
    ]
    
    # 重试机制
    for retry in range(max_retries):
        try:
            headers = {
                'User-Agent': random.choice(user_agents)
            }
            # 增加超时时间并添加随机延迟
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            data = resp.json().get('data', {})
            break
        except (requests.RequestException, ValueError) as e:
            print(f"请求失败 ({retry+1}/{max_retries}): {e}")
            if retry == max_retries - 1:  # 最后一次重试
                print(f"抓取股票 {international_code} 信息失败，跳过")
                return {}
            # 随机等待时间，避免被限流
            wait_time = random.uniform(3.0, 10.0)
            print(f"等待 {wait_time:.2f} 秒后重试...")
            time.sleep(wait_time)

    if not data:
        return {}
    
    # 股票简称
    stock_name = data.get('f58', '')
    
    # 是否st
    is_st = 'st' in stock_name.lower() or data.get('f140', '') == 'ST'

    # 市值（单位：亿元）- 尝试获取总市值和流通市值
    total_market_cap = data.get('f116', data.get('f20', ''))  # 总市值
    
    # 如果市值存在，保留两位小数
    if total_market_cap:
        try:
            total_market_cap = round(float(total_market_cap) / 100000000, 2)  # 转换为亿元
        except (ValueError, TypeError):
            total_market_cap = ''
    
    # 字典组装
    return {
        'stock_code': international_code,
        'stock_symbol': symbol,
        'stock_name': stock_name,
        'region': data.get('f128', ''),
        'industry_sector': data.get('f127', ''),
        'market_type': detect_board(symbol),
        'listed_date': start_date,
        'delisted_date': end_date,
        'market_cap': total_market_cap,
        'is_st': is_st,
    }

def detect_board(stock_code: str) -> str:
    """
    根据股票代码判断其所属板块
    """
    if stock_code.startswith(('600', '601', '603')):
        return '主板（上交所）'
    elif stock_code.startswith('688'):
        return '科创板'
    elif stock_code.startswith(('000', '001')):
        return '主板（深交所）'
    elif stock_code.startswith('002'):
        return '中小板（已并入主板）'
    elif stock_code.startswith('300'):
        return '创业板'
    else:
        return '未知板块'

def main():
    user_sql = PySQL(
        host='localhost',
        user='afei',
        password='sf123456',
        database='stock',
        port=3306
    )
    user_sql.connect()

    jqdatasdk.auth('13625559037', 'Jm123456')
    all_stocks = get_all_securities(['stock'])  # jqdata获取所有股票信息
    all_stocks['start_date'] = all_stocks['start_date'].dt.strftime('%Y-%m-%d')
    all_stocks['end_date'] = all_stocks['end_date'].dt.strftime('%Y-%m-%d')

    print(f"主板股票数量: {len(all_stocks)}")
    
    # 添加批处理记录点和进度显示
    total_stocks = len(all_stocks)
    processed_count = 0
    error_count = 0
    start_time = time.time()
    
    # 获取已处理股票列表，以便断点续传
    try:
        processed_stocks = set()
        existing_stocks = user_sql.select("stock_info", columns=["stock_code", "market_cap"])
        for stock in existing_stocks:
            if stock['market_cap']:  # 如果已经有市值数据了，则认为已处理
                processed_stocks.add(stock['stock_code'])
        
        print(f"已经处理过的股票数: {len(processed_stocks)}")
    except Exception as e:
        print(f"获取已处理股票失败: {e}")
        processed_stocks = set()
    
    for row in all_stocks.itertuples():
        stock_code = row.Index
        
        # 如果股票已经处理过，跳过
        if stock_code in processed_stocks:
            processed_count += 1
            continue
            
        try:
            info = get_stock_info(stock_code, row.start_date, row.end_date)
            if not info:
                print(f"获取股票信息失败: {stock_code}")
                error_count += 1
                continue
                
            # 更新市值信息
            user_sql.update("stock_info", {"market_cap": info['market_cap']}, f"stock_code = '{stock_code}'")
            
            processed_count += 1
            # 计算进度和预计剩余时间
            elapsed_time = time.time() - start_time
            progress = processed_count / total_stocks * 100
            if processed_count > 0:
                avg_time_per_stock = elapsed_time / processed_count
                remaining_stocks = total_stocks - processed_count
                estimated_time_left = avg_time_per_stock * remaining_stocks
                
                hours, remainder = divmod(estimated_time_left, 3600)
                minutes, seconds = divmod(remainder, 60)
                
                print(f"已抓取 {stock_code}，进度: {progress:.2f}% ({processed_count}/{total_stocks})，预计剩余时间: {int(hours)}时{int(minutes)}分{int(seconds)}秒")
            else:
                print(f"已抓取 {stock_code}，进度: {progress:.2f}% ({processed_count}/{total_stocks})")
                
        except Exception as e:
            print(f"处理股票 {stock_code} 时出错: {e}")
            error_count += 1
            
        # 随机休眠时间，避免被限流
        sleep_time = random.uniform(0.5, 2.0)
        time.sleep(sleep_time)
    
    # 打印总结信息
    print("\n================ 抓取完成 ================")
    print(f"总股票数: {total_stocks}")
    print(f"成功处理: {processed_count}")
    print(f"失败数量: {error_count}")
    print(f"总耗时: {time.time() - start_time:.2f} 秒")

    
if __name__ == '__main__':
    main()
    