import pandas as pd
from datetime import datetime, timedelta
from pysql import PySQL

def filter_stocks_by_price_range(start_date, min_price=5, max_price=15,min_market_cap=30, max_market_cap=180,region=None, max_days_forward=10):
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
    # 连接数据库
    sql = PySQL(
        host='localhost',
        user='afei',
        password='sf123456',
        database='stock',
        port=3306
    )
    sql.connect()
    
    # 获取所有股票代码
    if region is None or region == 'all' or region == '-':
        where=f'market_cap > {min_market_cap} AND market_cap < {max_market_cap} AND is_st = 0'  # 排除ST股票
    else:
        where=f'market_cap > {min_market_cap} AND market_cap < {max_market_cap} AND is_st = 0 and region = "{region}"'  # 排除ST股票
        
    stock_list = sql.select(
        'stock_info',
        columns=['stock_code'],
        where=where
        
    )
    stock_codes = [item['stock_code'] for item in stock_list]
    
    print(f"获取到 {len(stock_codes)} 只股票")
    
    # 计算结束日期（起始日期 + max_days_forward天）
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = (start_date_obj + timedelta(days=max_days_forward)).strftime('%Y-%m-%d')
    
    # 存储符合条件的股票
    filtered_stocks = []
    
    # 批量处理股票，每次处理一部分以避免查询过大
    batch_size = 500
    total_batches = (len(stock_codes) + batch_size - 1) // batch_size
    
    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(stock_codes))
        batch_stocks = stock_codes[start_idx:end_idx]
        
        # 创建IN查询的占位符
        placeholders = ', '.join(['%s'] * len(batch_stocks))
        where_clause = f'trade_date >= "{start_date}" AND trade_date <= "{end_date}" AND stock_code IN ({placeholders})'
        
        # 查询数据
        stocks_data = sql.select(
            'stock_daily_k',
            columns=['stock_code', 'trade_date', 'open'],
            where=where_clause,
            params=batch_stocks,
            order_by='trade_date ASC'  # 确保按日期升序排序
        )
        
        # 转换为DataFrame
        df = pd.DataFrame(stocks_data)
        
        if not df.empty:
            # 转换日期列为datetime类型
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            
            # 创建一个字典，存储每个股票的首个交易日数据
            stock_first_day = {}
            
            # 遍历数据，找到每个股票的首个交易日
            for _, row in df.iterrows():
                stock_code = row['stock_code']
                trade_date = row['trade_date']
                
                # 如果股票尚未记录首个交易日或当前日期更早，则更新
                if stock_code not in stock_first_day:
                    stock_first_day[stock_code] = row
            
            # 处理每个股票的首个交易日数据
            for stock_code, first_day in stock_first_day.items():
                open_price = float(first_day['open'])
                
                # 检查价格是否在范围内
                if min_price <= open_price <= max_price:
                    filtered_stocks.append({
                        'stock_code': stock_code,
                        'first_trade_date': first_day['trade_date'].strftime('%Y-%m-%d'),
                        'open_price': open_price,
                        'days_from_start': (first_day['trade_date'] - start_date_obj).days
                    })
        
        # print(f"已处理 {end_idx}/{len(stock_codes)} 只股票")
    
    print(f"筛选出 {len(filtered_stocks)} 只价格在 {min_price}-{max_price} 元之间的股票")
    
    # 按照与起始日期的距离排序
    filtered_stocks.sort(key=lambda x: x['days_from_start'])
    
    return filtered_stocks

if __name__ == "__main__":
    # 示例：筛选2024-10-01起始日期价格在5-15元之间的股票
    start_date = '2024-10-01'
    result = filter_stocks_by_price_range(start_date)
    
    # 打印结果
    print("\n符合条件的股票（前10个）:")
    for stock in result[:10]:  # 只打印前10个结果
        print(f"股票代码: {stock['stock_code']}, 交易日期: {stock['first_trade_date']} (距离起始日 {stock['days_from_start']} 天), 开盘价: {stock['open_price']:.2f}")
    
    # 保存结果到CSV文件
    # df_result = pd.DataFrame(result)
    # df_result.to_csv(f"price_filtered_stocks_{start_date}.csv", index=False, encoding='utf-8')
    # print(f"\n结果已保存到 price_filtered_stocks_{start_date}.csv") 