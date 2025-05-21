from pysql import PySQL
from my_bt import StockBacktest
import pandas as pd




if __name__ == '__main__':
    
    user_sql = PySQL(
            host='localhost',
            user='afei',
            password='sf123456',
            database='stock',
            port=3306
        )
    user_sql.connect()
    stocks_data = user_sql.select('stock_daily_k',columns=['stock_code','trade_date','open','high','low','close','change_value'],
                    where='trade_date > "2025-05-01" AND trade_date < "2025-05-20"')
    
    df = pd.DataFrame(stocks_data)
    # df['Date'] = pd.to_datetime(df['trade_date'])
    # df.set_index('Date', inplace=True)
    df = df[['stock_code','trade_date','open','high','low','close']]
    stock_list = ['000001.XSHE', '000002.XSHE']
    mybt = StockBacktest(df, initial_capital=100000,stock_list=stock_list)
    # for _ in range(len(df)):
    #     mybt.next()

    #     # 获取交易历史
    #     history = mybt.get_history()
    #     print("\n交易历史：")
    #     for action, date, price, amount in history:
    #         print(f"{action} | {date.date()} | {amount} 股 @ {price:.2f}")

