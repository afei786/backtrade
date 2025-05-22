import requests
import pandas as pd
import time
import random
import sys
from datetime import datetime

def get_stock_k_data(international_code, start_date='2023-01-01', end_date='2025-5-16', klt=101, max_retries=3):
    """
    获取股票K线数据
    klt: 1=1分钟, 5=5分钟, 15=15分钟, 30=30分钟, 60=60分钟, 101=日K, 102=周K, 103=月K
    max_retries: 最大重试次数
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
            r = requests.get(url, params=params, headers=headers, timeout=30)
            data = r.json()
            if not data or 'data' not in data or not data['data'] or 'klines' not in data['data']:
                print('接口返回异常，原始响应如下:')
                print(data)
                if retry == max_retries - 1:
                    raise ValueError('未获取到有效K线数据，请检查股票代码、市场参数或稍后重试。')
                wait_time = random.uniform(2.0, 5.0)
                print(f"等待 {wait_time:.2f} 秒后重试...")
                time.sleep(wait_time)
                continue
            
            kline = data['data']['klines']
            df = pd.DataFrame([i.split(',') for i in kline],
                        columns=['date', 'open', 'close', 'high', 'low', 'volume', 'turnover', 'amplitude', 'pct_change', 'change', 'turnover_rate'])
            return df
            
        except Exception as e:
            print(f"请求失败 ({retry+1}/{max_retries}): {e}")
            if retry == max_retries - 1:  # 最后一次重试
                raise
            # 随机等待时间，避免被限流
            wait_time = random.uniform(3.0, 10.0)
            print(f"等待 {wait_time:.2f} 秒后重试...")
            time.sleep(wait_time)
    
    # 如果所有重试都失败了
    raise ValueError(f"抓取股票 {international_code} K线数据失败")

if __name__ == '__main__':
    from pysql import PySQL
    
    # 连接数据库
    user_sql = PySQL(
        host='localhost',
        user='afei',
        password='sf123456',
        database='stock',
        port=3306
    )
    user_sql.connect()
    
    # 获取断点续传信息（如果有）
    last_processed = None
    try:
        # 尝试加载断点续传信息
        with open('crawl_checkpoint.txt', 'r') as f:
            last_processed = f.read().strip()
        print(f"发现断点续传信息，将从 {last_processed} 继续抓取")
    except FileNotFoundError:
        print("未找到断点续传信息，将从头开始抓取")
    
    # 是否清空数据表
    clear_table = False
    if len(sys.argv) > 1 and sys.argv[1] == '--clear':
        clear_table = True
    
    if clear_table:
        user_sql.delete('stock_daily_k', '1=1')
        print("已清空 stock_daily_k 表")
        
    # 获取已存在数据，避免重复抓取
    existing_data = {}
    try:
        print("正在获取已有数据信息...")
        # 修改：直接使用SQL语句进行分组查询，而不是使用不支持的group_by参数
        sql = "SELECT stock_code, COUNT(*) as count FROM stock_daily_k GROUP BY stock_code"
        user_sql.cursor.execute(sql)
        results = user_sql.cursor.fetchall()
        for row in results:
            existing_data[row['stock_code']] = row['count']
        print(f"已有 {len(existing_data)} 只股票的数据")
    except Exception as e:
        print(f"获取已有数据失败: {e}")
        print("继续执行，但可能会有重复数据")
    
    # 获取股票列表
    stock_info = user_sql.select('stock_info', columns=['stock_code'])
    total_stocks = len(stock_info)
    processed_count = 0
    error_count = 0
    skipped_count = 0
    start_time = time.time()
    
    print(f"总共需要抓取 {total_stocks} 只股票的数据")
    
    # 记录断点续传
    def save_checkpoint(stock_code):
        with open('crawl_checkpoint.txt', 'w') as f:
            f.write(stock_code)
    
    # 开始抓取
    found_starting_point = last_processed is None
    for code in stock_info:
        stock_code = code['stock_code']
        
        # 断点续传：如果有上次的断点，则跳过直到找到上次处理的股票
        if not found_starting_point:
            if stock_code == last_processed:
                found_starting_point = True
                print(f"找到断点 {last_processed}，开始抓取")
            else:
                skipped_count += 1
                continue
        
        # 检查是否已有数据
        if not clear_table and stock_code in existing_data and existing_data[stock_code] > 0:
            print(f"股票 {stock_code} 已有 {existing_data[stock_code]} 条数据，跳过")
            skipped_count += 1
            processed_count += 1
            continue
        
        try:
            # 保存当前处理的股票代码（断点）
            save_checkpoint(stock_code)
            
            # 抓取K线数据
            df = get_stock_k_data(stock_code, start_date='2015-05-19', end_date='2025-05-19', klt=101)
            
            # 如果数据为空，跳过
            if df.empty:
                print(f"股票 {stock_code} 没有K线数据，跳过")
                skipped_count += 1
                processed_count += 1
                continue
            
            # 处理数据并保存到数据库
            records = []
            for row in df.itertuples():
                # 确保amplitude不超过6个字符
                amplitude = row.amplitude
                if amplitude and len(amplitude) > 6:
                    amplitude = amplitude[:6]  # 截断到6个字符
                
                # 处理pct_change，确保不超过decimal(6,2)的范围
                pct_change = row.pct_change
                if pct_change:
                    try:
                        # 转换为浮点数，然后限制在-9999.99到9999.99之间
                        pct_value = float(pct_change)
                        if pct_value > 9999.99:
                            pct_value = 9999.99
                        elif pct_value < -9999.99:
                            pct_value = -9999.99
                        pct_change = str(round(pct_value, 2))
                    except (ValueError, TypeError):
                        pct_change = "0.00"  # 如果转换失败，使用默认值0
                
                # 处理turnover_rate，确保不超过decimal(6,2)的范围
                turnover_rate = row.turnover_rate
                if turnover_rate:
                    try:
                        # 转换为浮点数，然后限制在-9999.99到9999.99之间
                        rate_value = float(turnover_rate)
                        if rate_value > 9999.99:
                            rate_value = 9999.99
                        elif rate_value < -9999.99:
                            rate_value = -9999.99
                        turnover_rate = str(round(rate_value, 2))
                    except (ValueError, TypeError):
                        turnover_rate = "0.00"  # 如果转换失败，使用默认值0
                    
                records.append({
                    "stock_code": stock_code,
                    "trade_date": row.date,
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "amplitude": amplitude,
                    "change_value": row.change,
                    "pct_change": pct_change,
                    "vol": row.volume,
                    "turnover_rate": turnover_rate
                })

            # 批量插入
            if records:
                try:
                    # 使用INSERT IGNORE语法避免主键冲突
                    columns = list(records[0].keys())
                    columns_str = ", ".join([f"`{k}`" for k in columns])
                    placeholders = ", ".join(["%s"] * len(columns))
                    
                    # 按照固定的列顺序提取值
                    values = [[data[column] for column in columns] for data in records]
                    
                    # 使用INSERT IGNORE语法
                    sql = f"INSERT IGNORE INTO `stock_daily_k` ({columns_str}) VALUES ({placeholders})"
                    
                    if not user_sql.connection or not user_sql.connection.is_connected():
                        user_sql.connect()
                        
                    user_sql.cursor.executemany(sql, values)
                    user_sql.connection.commit()
                    affected_rows = user_sql.cursor.rowcount
                    print(f"成功批量插入 {affected_rows} 行数据到表 stock_daily_k（忽略了 {len(records) - affected_rows} 行重复数据）")
                except Exception as e:
                    user_sql.connection.rollback()
                    print(f"批量插入失败: {e}")
                    raise
                
            # 更新进度
            processed_count += 1
            elapsed_time = time.time() - start_time
            progress = processed_count / total_stocks * 100
            
            # 计算剩余时间
            if processed_count > 0:
                avg_time_per_stock = elapsed_time / processed_count
                remaining_stocks = total_stocks - processed_count
                est_time = avg_time_per_stock * remaining_stocks
                hours, remainder = divmod(est_time, 3600)
                minutes, seconds = divmod(remainder, 60)
                time_str = f"预计剩余时间: {int(hours)}小时{int(minutes)}分{int(seconds)}秒"
            else:
                time_str = ""
                
            print(f"已抓取 {stock_code} K线数据: {len(records)}条 进度: {progress:.2f}% ({processed_count}/{total_stocks}) {time_str}")
            
        except Exception as e:
            print(f"抓取 {stock_code} 数据时出错: {e}")
            error_count += 1
        
        # 随机休眠，避免被限流
        sleep_time = random.uniform(0.5, 2.0)
        time.sleep(sleep_time)
    
    # 清除断点记录
    try:
        import os
        os.remove('crawl_checkpoint.txt')
    except:
        pass
    
    # 打印总结
    end_time = time.time()
    total_time = end_time - start_time
    hours, remainder = divmod(total_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    print("\n===== 数据抓取完成 =====")
    print(f"总股票数: {total_stocks}")
    print(f"成功抓取: {processed_count - error_count - skipped_count}")
    print(f"错误数量: {error_count}")
    print(f"跳过数量: {skipped_count}")
    print(f"总耗时: {int(hours)}小时{int(minutes)}分{int(seconds)}秒")
