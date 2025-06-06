"""
计算股票的移动平均线

此脚本用于计算指定股票的5日线、10日线、20日线、30日线、45日线和60日线，并可将结果保存到数据库或导出为CSV文件。
所有移动平均线结果保留小数点后两位。

脚本支持两种模式：
1. 计算移动平均线并保存到单独的表或CSV文件
2. 计算移动平均线并更新到stock_daily_k表中

使用方法:
    python calculate_ma.py [选项]

参数:
    --stock_codes: 股票代码，多个代码用逗号分隔，例如: '000001.SZ,600000.SH'，默认为000001.XSHE
    --start_date: 开始日期，格式为YYYY-MM-DD，例如: '2023-01-01'，默认为2023-01-01
    --end_date: 结束日期，格式为YYYY-MM-DD，例如: '2023-12-31'，默认为2025-12-31
    --periods: 移动平均线周期，多个周期用逗号分隔，默认为5,10,20,30,45,60
    --price_col: 用于计算移动平均线的价格列，默认为close
    --output_csv: 输出CSV文件路径，默认为ma_results.csv
    --save_to_db: 是否将结果保存到数据库
    --db_table: 保存结果的数据库表名，默认为stock_ma
    --update_stock_table: 是否将均线数据更新到stock_daily_k表

示例:
    # 使用默认参数运行
    python calculate_ma.py
    
    # 计算000001.SZ和600000.SH的所有默认周期移动平均线，并保存到CSV文件
    python calculate_ma.py --stock_codes '000001.SZ,600000.SH' --start_date '2023-01-01' --end_date '2023-12-31' --output_csv 'ma_results.csv'

    # 计算000001.SZ的5日线、10日线和20日线，并保存到数据库
    python calculate_ma.py --stock_codes '000001.SZ' --start_date '2023-01-01' --end_date '2023-12-31' --periods '5,10,20' --save_to_db

    # 计算600000.SH的10日线和20日线，使用高价计算，并同时保存到CSV和数据库
    python calculate_ma.py --stock_codes '600000.SH' --start_date '2023-01-01' --end_date '2023-12-31' --periods '10,20' --price_col 'high' --output_csv 'ma_high.csv' --save_to_db
    
    # 计算000001.SZ的均线并更新到stock_daily_k表中
    python calculate_ma.py --stock_codes '000001.SZ' --start_date '2023-01-01' --end_date '2023-12-31' --update_stock_table
"""

import pandas as pd
import argparse
from datetime import datetime, timedelta
from pysql import PySQL
import os

# 数据库连接参数
DB_CONFIG = {
    'host': 'localhost',
    'user': 'afei',
    'password': 'sf123456',
    'database': 'stock',
    'port': 3306
}

def get_stock_data(stock_codes, start_date, end_date, db_conn=None):
    """
    从数据库获取指定股票在指定日期范围内的数据
    
    参数:
        stock_codes: 股票代码列表
        start_date: 开始日期，格式为'YYYY-MM-DD'
        end_date: 结束日期，格式为'YYYY-MM-DD'
        db_conn: 数据库连接对象，如果为None则创建新连接
        
    返回:
        包含股票数据的DataFrame
    """
    # 如果没有提供数据库连接，创建新连接
    close_conn = False
    if db_conn is None:
        db_conn = PySQL(**DB_CONFIG)
        db_conn.connect()
        close_conn = True
    
    try:
        # 计算实际查询的开始日期（为了计算移动平均线，需要额外的历史数据）
        # 获取60日线需要至少60天的历史数据，再加上一些余量
        query_start_date = (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=70)).strftime('%Y-%m-%d')
        
        # 创建IN查询的占位符
        if isinstance(stock_codes, str):
            stock_codes = [stock_codes]
            
        placeholders = ', '.join(['%s'] * len(stock_codes))
        where_clause = f'trade_date >= "{query_start_date}" AND trade_date <= "{end_date}" AND stock_code IN ({placeholders})'
        
        # 查询数据 - 修改为使用实际存在的列
        stock_data = db_conn.select('stock_daily_k',
                        columns=['stock_code', 'trade_date', 'open', 'high', 'low', 'close', 'change_value', 'pct_change'],
                        where=where_clause,
                        params=stock_codes)
        
        # 转换为DataFrame
        df = pd.DataFrame(stock_data)
        
        # 确保日期列为datetime类型
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            # 确保数值列为float类型
            numeric_columns = ['open', 'high', 'low', 'close', 'change_value', 'pct_change']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        print(f"成功获取 {len(df)} 条股票数据记录")
        return df
    
    finally:
        # 如果是新创建的连接，关闭它
        if close_conn and db_conn:
            db_conn.close()

def calculate_ma(df, periods=[30, 60], price_col='close'):
    """
    计算移动平均线
    
    参数:
        df: 包含股票数据的DataFrame
        periods: 移动平均线周期列表，默认为[30, 60]
        price_col: 用于计算移动平均线的价格列，默认为'close'
        
    返回:
        添加了移动平均线列的DataFrame，移动平均线结果保留小数点后两位
    """
    if df.empty:
        print("警告: 输入数据为空，无法计算移动平均线")
        return df
    
    # 确保按股票代码和日期排序
    df = df.sort_values(['stock_code', 'trade_date'])
    
    # 对每只股票分别计算移动平均线
    result_dfs = []
    for stock_code, stock_df in df.groupby('stock_code'):
        # 计算每个周期的移动平均线
        for period in periods:
            ma_col = f'ma{period}'
            # 计算移动平均线并保留小数点后两位
            stock_df[ma_col] = stock_df[price_col].rolling(window=period).mean().round(2)
        
        result_dfs.append(stock_df)
    
    # 合并结果
    result_df = pd.concat(result_dfs)
    
    # 过滤掉开始日期之前的数据（这些数据只是为了计算移动平均线）
    # result_df = result_df[result_df['trade_date'] >= pd.to_datetime(start_date)]
    
    return result_df

def add_ma_columns_to_table(db_conn, table_name='stock_daily_k', periods=[5, 10, 20, 30, 45, 60]):
    """
    向指定表添加均线字段
    
    参数:
        db_conn: 数据库连接对象
        table_name: 表名，默认为stock_daily_k
        periods: 均线周期列表，默认为[5, 10, 20, 30, 45, 60]
        
    返回:
        添加的字段数量
    """
    if not db_conn.table_exists(table_name):
        print(f"表 {table_name} 不存在")
        return 0
    
    added_columns = 0
    
    try:
        # 首先获取表结构，检查哪些均线字段已经存在
        db_conn.cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
        existing_columns = [column['Field'] for column in db_conn.cursor.fetchall()]
        
        # 为每个周期添加均线字段
        for period in periods:
            ma_col = f'ma{period}'
            if ma_col not in existing_columns:
                # 添加均线字段
                sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{ma_col}` DECIMAL(10, 2) DEFAULT NULL"
                db_conn.execute(sql)
                print(f"已添加字段 {ma_col} 到表 {table_name}")
                added_columns += 1
            else:
                print(f"字段 {ma_col} 已存在于表 {table_name}")
        
        return added_columns
    
    except Exception as e:
        print(f"添加均线字段失败: {e}")
        return 0

def parse_args():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(description='计算股票的移动平均线')
    
    # 参数
    parser.add_argument('--stock_codes', default='000001.XSHE', help='股票代码，多个代码用逗号分隔，默认为000001.XSHE')
    parser.add_argument('--start_date', default='2023-01-01', help='开始日期，格式为YYYY-MM-DD，默认为2023-01-01')
    parser.add_argument('--end_date', default='2025-12-31', help='结束日期，格式为YYYY-MM-DD，默认为2025-12-31')
    parser.add_argument('--periods', default='5,10,20,30,45,60', help='移动平均线周期，多个周期用逗号分隔，默认为5,10,20,30,45,60')
    parser.add_argument('--price_col', default='close', help='用于计算移动平均线的价格列，默认为close')
    parser.add_argument('--output_csv', default='ma_results.csv', help='输出CSV文件路径，默认为ma_results.csv')
    parser.add_argument('--save_to_db', action='store_true', default=True, help='是否将结果保存到数据库')
    parser.add_argument('--db_table', default='stock_daily_k', help='保存结果的数据库表名，默认为stock_ma')
    parser.add_argument('--update_stock_table', action='store_true', help='是否将均线数据更新到stock_daily_k表')
    
    return parser.parse_args()

def main(stock_code,start_date, end_date):
    """
    主函数
    """
    
    # 均线周期列表
    periods = [5,10,20,30,45,60]
    
    # 创建数据库连接
    db_conn = PySQL(**DB_CONFIG)
    db_conn.connect()
    
    try:
        
        # 获取股票数据
        stock_data = get_stock_data(stock_code, start_date,end_date)
        
        if stock_data.empty:
            print("错误: 未获取到股票数据")
            return
        
        # 计算移动平均线
        print(f"计算移动平均线: {', '.join(map(str, periods))}")
        result_df = calculate_ma(stock_data, periods, price_col='close')
        
        # 过滤掉开始日期之前的数据
        result_df = result_df[result_df['trade_date'] >= pd.to_datetime(start_date)]

        # 只保留股票代码、交易日期和均线数据列
        ma_columns = [col for col in result_df.columns if col.startswith('ma')]
        result_df = result_df[['stock_code', 'trade_date'] + ma_columns]

        # nan 值处理
        result_df = result_df.where(pd.notnull(result_df), 0.0)
        
        # result_df = result_df['stock_code', 'trade_date', 'ma5' ]
        # print(result_df)
        # 显示结果
        print(f"计算完成，共 {len(result_df)} 条记录")

        # 转为列表嵌套字典
        result_list = result_df.to_dict(orient='records')
        # print(f"结果数据: {result_list[:5]}...")  # 只显示前5条记录

        # 更新到数据库
        db_conn.batch_update('stock_daily_k', result_list, key_fields=['stock_code', 'trade_date'])
            
    finally:
        # 关闭数据库连接
        if db_conn:
            db_conn.close()

if __name__ == '__main__':
        # 从数据库获取数据
    user_sql = PySQL(
        host='localhost',
        user='afei',
        password='sf123456',
        database='stock',
        port=3306
    )
    user_sql.connect()
    # stock_list = ['002594.XSHE','603881.XSHG']
    stock_list = user_sql.select(
        'stock_info',
        columns=['stock_code'],
    )
    print(f"获取到 {len(stock_list)} 只股票")
    stock_list = [item['stock_code'] for item in stock_list]
    for stock in stock_list:
        # print(f"开始计算 {stock} 的均线")
        # 计算每只股票的均线
        main(stock, '2015-01-01', '2025-12-31')
    # main(stock_list, '2015-01-01', '2025-12-31') 