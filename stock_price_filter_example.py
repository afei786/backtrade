from filter_stocks import filter_stocks_by_price_range
import pandas as pd
import argparse
from datetime import datetime

def main():
    """
    示例脚本，展示如何使用filter_stocks_by_price_range函数
    """
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='筛选特定价格范围的股票')
    parser.add_argument('--start_date', type=str, default='2024-10-01', help='起始日期，格式为YYYY-MM-DD')
    parser.add_argument('--min_price', type=float, default=5.0, help='最小价格')
    parser.add_argument('--max_price', type=float, default=15.0, help='最大价格')
    parser.add_argument('--max_days', type=int, default=10, help='如果起始日期没有数据，最多向前查找的天数')
    parser.add_argument('--output', type=str, help='输出CSV文件路径，默认为price_filtered_stocks_日期.csv')
    
    args = parser.parse_args()
    
    # 验证日期格式
    try:
        datetime.strptime(args.start_date, '%Y-%m-%d')
    except ValueError:
        print("错误：日期格式不正确，应为YYYY-MM-DD")
        return
    
    # 验证价格范围
    if args.min_price <= 0 or args.max_price <= 0 or args.min_price >= args.max_price:
        print("错误：价格范围不正确，应满足 0 < min_price < max_price")
        return
    
    # 设置输出文件路径
    output_file = args.output if args.output else f"price_filtered_stocks_{args.start_date}_({args.min_price:.1f}-{args.max_price:.1f}).csv"
    
    print(f"开始筛选 {args.start_date} 起始日期价格在 {args.min_price}-{args.max_price} 元之间的股票...")
    
    # 调用筛选函数
    result = filter_stocks_by_price_range(
        start_date=args.start_date,
        min_price=args.min_price,
        max_price=args.max_price,
        max_days_forward=args.max_days
    )
    
    # 打印结果统计
    print("\n结果统计:")
    print(f"共找到 {len(result)} 只符合条件的股票")
    
    # 按日期分组统计
    if result:
        df_result = pd.DataFrame(result)
        date_counts = df_result.groupby('first_trade_date').size()
        print("\n按日期分布:")
        for date, count in date_counts.items():
            print(f"  {date}: {count} 只股票")
        
        # 按价格区间统计
        price_bins = [args.min_price, args.min_price + (args.max_price - args.min_price) / 3, 
                      args.min_price + 2 * (args.max_price - args.min_price) / 3, args.max_price]
        df_result['price_range'] = pd.cut(df_result['open_price'], bins=price_bins)
        price_counts = df_result.groupby('price_range').size()
        print("\n按价格区间分布:")
        for price_range, count in price_counts.items():
            print(f"  {price_range}: {count} 只股票")
        
        # 打印前10个结果
        print("\n符合条件的股票（前10个）:")
        for i, stock in enumerate(result[:10]):
            print(f"{i+1}. 股票代码: {stock['stock_code']}, 交易日期: {stock['first_trade_date']} (距离起始日 {stock['days_from_start']} 天), 开盘价: {stock['open_price']:.2f}")
        
        # 保存结果到CSV文件
        df_result.to_csv(output_file, index=False, encoding='utf-8')
        print(f"\n结果已保存到 {output_file}")
    else:
        print("没有找到符合条件的股票")

if __name__ == "__main__":
    main() 