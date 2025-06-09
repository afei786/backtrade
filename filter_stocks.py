import pandas as pd
from datetime import datetime, timedelta
from pysql import PySQL
from datetime import datetime, date


def filter_stocks_by_price_range(sql, min_price=5, max_price=15,min_market_cap=30, 
                                 max_market_cap=180,region=None, max_days_forward=10,
                                 not_market_type=None, start_date='2024-10-01', end_date='2025-12-31'):
    """
    筛选起始日期股票价格在指定范围内的股票
    如果起始日期没有数据，顺延到下一个交易日
    
    参数:
        start_date: 起始日期，格式为 'YYYY-MM-DD'
        min_price: 最小价格，默认为5
        max_price: 最大价格，默认为15
        max_days_forward: 如果起始日期没有数据，最多向前查找的天数，默认为10
        
    返回:
        符合条件的股票代码列表
    """
    # 获取所有符合条件的股票代码
    where_clause = ["market_cap > %s", "market_cap < %s",]  # 排除ST股票
    params = [min_market_cap, max_market_cap]

    if region is not None:
        placeholders = ', '.join(['%s'] * len(region))
        where_clause.append(f'region in ({placeholders})')  # 筛选指定地域
        params.extend(region)

    if not_market_type is not None:
        placeholders = ', '.join(['%s'] * len(not_market_type))
        where_clause.append(f'market_type not in ({placeholders})')  # 排除板块
        params.extend(not_market_type)

    # 构建完整的WHERE子句
    where_str = " AND ".join(where_clause)


    stock_list = sql.select(
        'stock_info',
        columns=['stock_code'],
        where=where_str,
        params=params
    )
    stock_codes = [item['stock_code'] for item in stock_list]
    
    if not stock_codes:
        print("没有符合条件的股票")
        return []
    print(f"筛选出 {len(stock_codes)} 只股票")

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    elif isinstance(start_date, date) and not isinstance(start_date, datetime):
        # 如果是date但不是datetime，转为datetime
        start_date = datetime.combine(start_date, datetime.min.time())

    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    elif isinstance(end_date, date) and not isinstance(end_date, datetime):
        end_date = datetime.combine(end_date, datetime.min.time())

    # start_date = datetime.strptime(start_date, '%Y-%m-%d')
    # end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    # 存储符合条件的股票代码
    filtered_stocks = []
    unfiltered_stocks = []

    for stock_code in stock_codes:
        # 查询股票在指定日期范围内的交易数据
        stocks_data = sql.select(
            'stock_daily_k',
            columns=['stock_code', 'trade_date', 'open', 'high', 'low', 'close'],
            where='trade_date >= %s AND trade_date <= %s AND stock_code = %s',
            params=[start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), stock_code],
            order_by='trade_date ASC'  # 确保按日期升序排序
        )
        
        # 转换为DataFrame
        df = pd.DataFrame(stocks_data)
        
        if df.empty:
            print(f"股票 {stock_code} 在指定日期范围内没有数据")
            continue

        if df[['open', 'close', 'high', 'low']].apply(lambda col: col.between(min_price, max_price)).any().any():
            filtered_stocks.append(stock_code)
        else:
            unfiltered_stocks.append(stock_code)
    print(f"筛选出在{start_date.strftime('%Y-%m-%d')}--{end_date.strftime('%Y-%m-%d')}中价格在{min_price}--{max_price} 标的 {len(filtered_stocks)}只 ")
    return filtered_stocks

if __name__ == "__main__":
    # 示例：筛选2024-10-01起始日期价格在5-15元之间的股票
    user_sql_config = {
        'host': 'localhost',
        'user': 'afei',
        'password': 'sf123456',
        'database': 'stock',
        'port': 3306
    }
    user_sql = PySQL(**user_sql_config)
    user_sql.connect()
    
    result = filter_stocks_by_price_range(user_sql, region=['浙江板块'], not_market_type=['创业板', '科创板'],)
    
    