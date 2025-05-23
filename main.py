from pysql import PySQL
from my_bt import StockBacktest
import pandas as pd

def start():
    user_sql = PySQL(
        host='localhost',
        user='afei',
        password='sf123456',
        database='stock',
        port=3306
    )
    user_sql.connect()
    stock_list = ['002594.XSHE']
    
    # 创建IN查询的占位符
    placeholders = ', '.join(['%s'] * len(stock_list))
    where_clause = f'trade_date > "2025-04-06" AND trade_date < "2025-04-08" AND stock_code IN ({placeholders})'
    
    stocks_data = user_sql.select('stock_daily_k',
                    columns=['stock_code','trade_date','open','high','low','close','change_value','pct_change'],
                    where=where_clause, 
                    params=stock_list)
    
    # 准备数据
    df = pd.DataFrame(stocks_data)
    df = df[['stock_code', 'trade_date', 'open', 'high', 'low', 'close', 'change_value','pct_change']]
    
    # 设置回测股票列表
    
    # 运行回测
    mybt = MYBT(df, initial_capital=100000, stock_list=stock_list, index_code='000300.SH')

    mybt.run_backtest()

class MYBT(StockBacktest):
    def __init__(self, data: pd.DataFrame, initial_capital: float = 100000, log_file: str = 'backtest_log.txt',
                 start_time: str = None, end_time: str = None, stock_list: list = None, index_code: str = '000300.SH'):
        super().__init__(data, initial_capital, log_file, start_time, end_time, stock_list)
        
    def strategy(self,stock):
        """
        重写策略
        """
        # 示例策略：持仓不足100股时买入
        if self.stocks_position[stock]['available'] < 100:
            self.buy(stock, self.open_price, 100)

        # 止盈
        if self.stocks_position[stock]['available'] >= 100 and self.open_price >= self.stocks_position[stock]['cost_price'] * 1.15:
            print('yes')
            self.sell(stock, self.open_price, self.stocks_position[stock]['available'])
        
        # 补仓
        if self.stocks_position[stock]['available'] >= 100 and self.open_price <= self.stocks_position[stock]['cost_price'] * 0.85:
            print('no')

            self.buy(stock, self.open_price, 100)

        # 结束日期卖出所有持仓
        if self.current_date == self.end_time:
            available_shares = self.stocks_position[stock]['available']
            if available_shares > 0:
                self.sell(stock, self.close_price, available_shares)

if __name__ == '__main__':
    start()
    

