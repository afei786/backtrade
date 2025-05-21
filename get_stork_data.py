import requests
import pandas as pd
import time
import random

def get_stock_k_data(international_code, start_date='2023-01-01', end_date='2025-5-16', klt=101):
    """
    获取股票K线数据
    klt: 1=1分钟, 5=5分钟, 15=15分钟, 30=30分钟, 60=60分钟, 101=日K, 102=周K, 103=月K
    """
    symbol = international_code.split('.')[0]
    if international_code.endswith('.XSHG'):
        eastmoney_prefix = '1'  # 东方财富 1 开头为上交所
    elif international_code.endswith('.XSHE'):
        eastmoney_prefix = '0'  # 东方财富 0 开头为深交所
    else:
        raise ValueError('市场类型错误，应为 "XSHE" 或 "XSHG"')
    url = f'https://push2his.eastmoney.com/api/qt/stock/kline/get'
    params = {
        'secid': f"{eastmoney_prefix}.{symbol}",
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
        'klt': klt,  # K线类型
        'fqt': 1,    # 前复权
        'beg': start_date.replace('-', ''),
        'end': end_date.replace('-', ''),
        'lmt': 10000,
    }

    headers = {
        'User-Agent': 'Mozilla/5.0'
    }

    r = requests.get(url, params=params, headers=headers)
    try:
        data = r.json()
    except Exception as e:
        print('解析JSON失败:', e)
        print('原始响应内容:', r.text)
        raise

    # 检查接口返回内容结构
    if not data or 'data' not in data or not data['data'] or 'klines' not in data['data']:
        print('接口返回异常，原始响应如下:')
        print(data)
        raise ValueError('未获取到有效K线数据，请检查股票代码、市场参数或稍后重试。')

    kline = data['data']['klines']
    # date 日期/时间 open 开盘价 close 收盘价（当前周期的收盘价） high 最高价 low 最低价 volume 成交量（股） turnover 成交额（元） 
    # amplitude 振幅（百分比） pct_change 涨跌幅（百分比）change 涨跌额（收盘-上周期收盘） turnover_rate 换手率（百分比）
    df = pd.DataFrame([i.split(',') for i in kline],
                      columns=['date', 'open', 'close', 'high', 'low', 'volume', 'turnover', 'amplitude', 'pct_change', 'change', 'turnover_rate'])

    return df

if __name__ == '__main__':
    # 批量股票示例及防屏蔽sleep
    stock_list = [
        '000002.XSHE',
    ]
    from pysql import PySQL
    user_sql = PySQL(
        host='localhost',
        user='afei',
        password='sf123456',
        database='stock',
        port=3306
    )
    user_sql.connect()
    id = 110000000
    for code in stock_list:
        df = get_stock_k_data(code, start_date='2015-05-19', end_date='2025-05-19', klt=101)
        # df.to_csv(f'{code}_k.csv', index=False, encoding='utf-8-sig')
        # 保存到数据库
        records = []
        for row in df.itertuples():
            records.append({
                "id": id,
                "stock_code": code,
                "trade_date": row.date,
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "amplitude": row.amplitude,
                "change_value": row.change,
                "pct_change": row.pct_change,
                "vol": row.volume,
                "turnover_rate": row.turnover_rate
            })
            id += 1

        user_sql.batch_insert("stock_daily_k", records)
        sleep_time = random.uniform(0.5, 2.0)
        print(f"已抓取{code} K线 ，休眠{sleep_time:.2f}秒")
        time.sleep(sleep_time)
