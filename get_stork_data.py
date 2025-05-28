import requests
import pandas as pd
import time
import random
import sys
from datetime import datetime
from pysql import PySQL


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

def get_index_k_data(index_code, start_date='2023-01-01', end_date='2025-5-26', klt=101, max_retries=3):
    """
    获取指数K线数据
    index_code: 指数代码，如：000300.SH（沪深300）
    klt: 1=1分钟, 5=5分钟, 15=15分钟, 30=30分钟, 60=60分钟, 101=日K, 102=周K, 103=月K
    max_retries: 最大重试次数
    """
    symbol = index_code.split('.')[0]
    if index_code.endswith('.SH'):
        eastmoney_prefix = '1'  # 东方财富 1 开头为上证
    elif index_code.endswith('.SZ'):
        eastmoney_prefix = '0'  # 东方财富 0 开头为深证
    else:
        raise ValueError('市场类型错误，应为 "SH" 或 "SZ"')

    url = f'https://push2his.eastmoney.com/api/qt/stock/kline/get'
    params = {
        'secid': f"{eastmoney_prefix}.{symbol}",
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
        'klt': klt,
        'fqt': 1,
        'beg': start_date.replace('-', ''),
        'end': end_date.replace('-', ''),
        'lmt': 10000,
    }

    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
    ]

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
                    raise ValueError('未获取到有效K线数据，请检查指数代码或稍后重试。')
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
            if retry == max_retries - 1:
                raise
            wait_time = random.uniform(3.0, 10.0)
            print(f"等待 {wait_time:.2f} 秒后重试...")
            time.sleep(wait_time)
    
    raise ValueError(f"抓取指数 {index_code} K线数据失败")

def init_database():
    """初始化数据库连接"""
    user_sql = PySQL(
        host='localhost',
        user='afei',
        password='sf123456',
        database='stock',
        port=3306
    )
    user_sql.connect()
    return user_sql

def load_checkpoint():
    """加载断点续传信息"""
    try:
        with open('crawl_checkpoint.txt', 'r') as f:
            last_processed = f.read().strip()
        print(f"发现断点续传信息，将从 {last_processed} 继续抓取")
        return last_processed
    except FileNotFoundError:
        print("未找到断点续传信息，将从头开始抓取")
        return None

def save_checkpoint(stock_code):
    """保存断点信息"""
    with open('crawl_checkpoint.txt', 'w') as f:
        f.write(stock_code)

def clear_checkpoint():
    """清除断点信息"""
    try:
        import os
        os.remove('crawl_checkpoint.txt')
    except:
        pass

def get_existing_data(user_sql, table_name='stock_daily_k'):
    """获取已存在的数据记录"""
    existing_data = {}
    try:
        # 获取表结构信息，判断是用stock_code还是index_code
        user_sql.cursor.execute(f"DESCRIBE {table_name}")
        columns = user_sql.cursor.fetchall()
        id_field = "stock_code"
        for col in columns:
            if col['Field'] == 'index_code':
                id_field = 'index_code'
                break
        
        print(f"正在获取{table_name}表已有数据信息...")
        sql = f"SELECT {id_field}, COUNT(*) as count FROM {table_name} GROUP BY {id_field}"
        user_sql.cursor.execute(sql)
        results = user_sql.cursor.fetchall()
        for row in results:
            existing_data[row[id_field]] = row['count']
        print(f"已有 {len(existing_data)} 条数据记录")
    except Exception as e:
        print(f"获取已有数据失败: {e}")
        print("继续执行，但可能会有重复数据")
    return existing_data

def process_stock_data(records, stock_code=None, klt=101):
    """处理股票数据，确保数据格式正确
    
    Args:
        records: DataFrame的行数据
        stock_code: 股票代码
        klt: K线类型，1=1分钟, 5=5分钟, 15=15分钟, 30=30分钟, 60=60分钟, 101=日K, 102=周K, 103=月K
    """
    processed_records = []
    for row in records:
        # 处理amplitude
        amplitude = row.amplitude
        if amplitude and len(amplitude) > 6:
            amplitude = amplitude[:6]
        
        # 处理pct_change
        pct_change = process_decimal_field(row.pct_change)
        
        # 处理turnover_rate
        turnover_rate = process_decimal_field(row.turnover_rate)
        
        # 处理时间格式，对于分钟级别K线数据，转换正确的时间格式
        trade_date = row.date
        if klt in [1, 5, 15, 30, 60] and len(trade_date) > 10:  # 如果是分钟K线且包含时间信息
            # 将格式从 "2023-05-26 10:30" 转换为标准格式
            trade_date = trade_date.replace(' ', 'T')
        
        processed_records.append({
            "stock_code": stock_code,  # 使用传入的股票代码，而不是从记录中获取
            "trade_date": trade_date,
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
    return processed_records

def process_decimal_field(value, max_value=9999.99):
    """处理decimal类型字段"""
    if value:
        try:
            value_float = float(value)
            value_float = max(min(value_float, max_value), -max_value)
            return str(round(value_float, 2))
        except (ValueError, TypeError):
            return "0.00"
    return "0.00"

def batch_insert_records(user_sql, records, table_name='stock_daily_k'):
    """批量插入记录到数据库"""
    if not records:
        return 0
    
    try:
        # 获取表结构信息，确定是用stock_code还是index_code
        user_sql.cursor.execute(f"DESCRIBE {table_name}")
        columns = user_sql.cursor.fetchall()
        has_stock_code = False
        has_index_code = False
        
        for col in columns:
            if col['Field'] == 'stock_code':
                has_stock_code = True
            elif col['Field'] == 'index_code':
                has_index_code = True
        
        # 如果记录中有stock_code但表中没有，需要转换为index_code
        if 'stock_code' in records[0] and not has_stock_code and has_index_code:
            for record in records:
                record['index_code'] = record.pop('stock_code')
        # 如果记录中有index_code但表中没有，需要转换为stock_code
        elif 'index_code' in records[0] and not has_index_code and has_stock_code:
            for record in records:
                record['stock_code'] = record.pop('index_code')
                
        columns = list(records[0].keys())
        columns_str = ", ".join([f"`{k}`" for k in columns])
        placeholders = ", ".join(["%s"] * len(columns))
        values = [[data[column] for column in columns] for data in records]
        
        sql = f"INSERT IGNORE INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        
        if not user_sql.connection or not user_sql.connection.is_connected():
            user_sql.connect()
            
        user_sql.cursor.executemany(sql, values)
        user_sql.connection.commit()
        affected_rows = user_sql.cursor.rowcount
        print(f"成功批量插入 {affected_rows} 行数据到表 {table_name}（忽略了 {len(records) - affected_rows} 行重复数据）")
        return affected_rows
    except Exception as e:
        user_sql.connection.rollback()
        print(f"批量插入失败: {e}")
        raise

def process_index_data(df, index_code, klt=101):
    """处理指数数据，确保数据格式正确
    
    Args:
        df: 数据框
        index_code: 指数代码
        klt: K线类型，1=1分钟, 5=5分钟, 15=15分钟, 30=30分钟, 60=60分钟, 101=日K, 102=周K, 103=月K
    """
    records = []
    for row in df.itertuples():
        # 处理amplitude
        amplitude = row.amplitude
        if amplitude and len(amplitude) > 6:
            amplitude = amplitude[:6]
        
        # 处理pct_change和turnover_rate
        pct_change = process_decimal_field(row.pct_change)
        turnover_rate = process_decimal_field(row.turnover_rate)
        
        # 处理时间格式
        trade_date = row.date
        if klt in [1, 5, 15, 30, 60] and len(trade_date) > 10:  # 如果是分钟K线且包含时间信息
            # 将格式从 "2023-05-26 10:30" 转换为标准格式
            trade_date = trade_date.replace(' ', 'T')
        
        records.append({
            "index_code": index_code,
            "trade_date": trade_date,
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
    return records

def create_table(user_sql, table_name='index_daily_k', table_type='index'):
    """创建数据表
    
    Args:
        user_sql: 数据库连接对象
        table_name: 表名
        table_type: 表类型，'index'表示指数表，'stock'表示股票表
    """
    # 根据表类型选择主键字段名
    id_field = "index_code" if table_type == 'index' else "stock_code"
    
    # 确定交易日期字段类型，如果是分钟级表，则使用DATETIME
    is_min_table = 'min' in table_name.lower()
    date_type = "DATETIME" if is_min_table else "DATE"
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {id_field} VARCHAR(20),
        trade_date {date_type},
        open DECIMAL(10,2),
        high DECIMAL(10,2),
        low DECIMAL(10,2),
        close DECIMAL(10,2),
        amplitude VARCHAR(6),
        change_value DECIMAL(10,2),
        pct_change DECIMAL(6,2),
        vol BIGINT,
        turnover_rate DECIMAL(6,2),
        PRIMARY KEY ({id_field}, trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    user_sql.cursor.execute(create_table_sql)
    user_sql.connection.commit()

def batch_insert_index_records(user_sql, records, table_name='index_daily_k'):
    """批量插入指数记录到数据库"""
    # 直接调用通用的批量插入函数
    return batch_insert_records(user_sql, records, table_name)

def crawl_stock_data(stock_codes=None, clear_table=False, table_name='stock_daily_k', table_type='stock', klt=101):
    """抓取股票数据的主函数
    
    Args:
        stock_codes: 股票代码列表
        clear_table: 是否清空表
        table_name: 表名
        table_type: 表类型
        klt: K线类型，1=1分钟, 5=5分钟, 15=15分钟, 30=30分钟, 60=60分钟, 101=日K, 102=周K, 103=月K
    """
    # 初始化数据库连接
    user_sql = init_database()
    create_table(user_sql, table_name, table_type)
    
    # 如果需要清空表
    if clear_table:
        user_sql.delete(table_name, '1=1')
        print(f"已清空 {table_name} 表")
    
    # 获取断点续传信息
    last_processed = load_checkpoint()
    
    # 获取已存在数据
    existing_data = get_existing_data(user_sql, table_name)
    
    # 如果没有指定股票代码，则获取所有股票
    if not stock_codes:
        stock_info = user_sql.select('stock_info', columns=['stock_code'])
        stock_codes = [code['stock_code'] for code in stock_info]
    
    total_stocks = len(stock_codes)
    processed_count = 0
    error_count = 0
    skipped_count = 0
    start_time = time.time()
    
    print(f"总共需要抓取 {total_stocks} 只股票的数据")
    
    # 断点续传处理
    found_starting_point = last_processed is None
    
    for stock_code in stock_codes:
        # 断点续传检查
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
            # 保存断点
            save_checkpoint(stock_code)
            
            # 抓取数据
            df = get_stock_k_data(stock_code, start_date='2015-05-19', end_date='2025-05-19', klt=klt)
            
            if df.empty:
                print(f"股票 {stock_code} 没有K线数据，跳过")
                skipped_count += 1
                processed_count += 1
                continue
            
            # 处理数据
            records = process_stock_data(df.itertuples(), stock_code, klt)
            
            # 批量插入
            if records:
                batch_insert_records(user_sql, records, table_name)
            
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
        
        # 随机休眠
        time.sleep(random.uniform(0.5, 2.0))
    
    # 清除断点
    clear_checkpoint()
    
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

def get_index_data(index_code='000300.SH', start_date='2015-01-01', end_date='2025-05-19', table_name='index_daily_k', klt=101):
    """获取指数数据的主函数"""
    # 初始化数据库连接
    user_sql = init_database()
    
    # 创建指数数据表（如果不存在）
    create_table(user_sql, table_name, 'index')
    
    try:
        # 获取数据
        print(f"正在获取 {index_code} 的K线数据...")
        df = get_index_k_data(index_code, start_date=start_date, end_date=end_date, klt=klt)
        
        if df.empty:
            print(f"指数 {index_code} 没有K线数据")
            return
        
        # 处理数据
        records = process_index_data(df, index_code, klt)
        
        # 批量插入
        if records:
            batch_insert_index_records(user_sql, records, table_name)
            print(f"成功获取并保存 {index_code} 的 {len(records)} 条K线数据")
    
    except Exception as e:
        print(f"获取指数数据时出错: {e}")
    finally:
        if user_sql.connection and user_sql.connection.is_connected():
            user_sql.connection.close()

if __name__ == '__main__':
    
    # 获取沪深300指数数据
    # index_code = ['000001.SH','399006.SZ', '000016.SH', '000688.SH','000300.SH', '000905.SH']
    # for index in index_code:
    #     get_index_data(index, start_date='2015-01-01', end_date='2025-05-19')
    
    # 获取股票数据（如果需要的话）
    # crawl_stock_data(clear_table=clear_table, stock_codes=['000001.XSHE'])

<<<<<<< HEAD
    # 获取股票1分钟K线数据
    crawl_stock_data(table_name='stock_min_k', table_type='stock', klt=1)
=======

    # 获取全部股票数据
    crawl_stock_data()

>>>>>>> f8742ab074b6fc93f104d3bb0ea2dc0f9f0007b5

