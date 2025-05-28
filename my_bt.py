import pandas as pd
from datetime import datetime, timedelta
import decimal
from pysql import PySQL


class StockBacktest:
    def __init__(self, data: pd.DataFrame, initial_capital: float = 100000, log_file: str = 'backtest_log.txt',
                 start_time: str = None, end_time: str = None, stock_list: list = None, index_code: str = '000300.SH'):
        """
        初始化回测类
        :param data: 包含股票数据的DataFrame，应该有stock_code, trade_date, open, high, low, close等列
        :param initial_capital: 初始资金
        :param log_file: 日志文件路径
        :param start_time: 回测开始时间，格式：'YYYY-MM-DD'
        :param end_time: 回测结束时间，格式：'YYYY-MM-DD'
        :param stock_list: 股票代码列表
        :param index_code: 对比指数代码，默认为沪深300
        """
        # 数据预处理
        self.data = data.copy()
        self.data['trade_date'] = pd.to_datetime(self.data['trade_date'])
        
        # 初始化资金和统计信息
        self.initial_capital = initial_capital
        self.cash = decimal.Decimal(initial_capital)
        self.balance = decimal.Decimal(initial_capital)
        self.result = {}
        # {"":{'daily_profit_rate': 0, 'total_profit_rate': 0, 
        #  'total_value': 0, 'cash': 0, 'market_cap': 0, 'index_daily_profit_rate': 0, 'index_total_profit_rate': 0}}

        # 设置回测时间范围
        self.start_time = pd.to_datetime(start_time) if start_time else self.data['trade_date'].min()
        self.end_time = pd.to_datetime(end_time) if end_time else self.data['trade_date'].max()
        self.current_date = self.start_time
        
        # 过滤数据在时间范围内的部分
        self.data = self.data[(self.data['trade_date'] >= self.start_time) & 
                             (self.data['trade_date'] <= self.end_time)].reset_index(drop=True)
        
        # 设置股票列表和初始化持仓
        self.stock_list = stock_list or []
        self.stocks_position = {stock: {'available': 0, 'unavailable': 0, 'cost_price': 0.0, 'sell_amount': 0} 
                               for stock in self.stock_list}
        
        # 获取指数数据
        self.index_code = index_code
        self.index_data = self._get_index_data()
        if not self.index_data.empty:
            self.initial_index_price = float(self.index_data.iloc[0]['open'])
        
        # 初始化日志
        self.log_file_name = log_file
        self._init_log()
        
        # 启动回测
        # self.run_backtest()

    def _init_log(self):
        """初始化日志文件"""
        self.log = open(self.log_file_name, 'w', encoding='utf-8')
        self.log.write(f"回测日志 - 初始资本: {self.initial_capital}\n")
        self.log.write("===========================================\n")

    def log_message(self, message: str):
        """记录日志消息"""
        log_entry = f"[{datetime.strftime(self.current_date, '%Y-%m-%d')}] {message}"
        self.log.write(log_entry + "\n")
        # print(log_entry)
    
    def buy(self, stock: str, price: float, amount: int):
        """买入操作"""
        cost = price * amount
        if cost > self.cash:
            self.log_message(f"资金不足，无法买入 {stock} {amount} 股 @ {price:.2f}")
            return False
            
        self.cash -= decimal.Decimal(cost)
        self.stocks_position[stock]['unavailable'] = amount
        
        # 计算成本价
        if self.stocks_position[stock]['cost_price'] == 0:
            self.stocks_position[stock]['cost_price'] = float(price)
        else:
            current_position = self.stocks_position[stock]['available']
            current_cost = self.stocks_position[stock]['cost_price'] * current_position
            new_cost = float(price) * amount
            total_position = current_position + amount
            self.stocks_position[stock]['cost_price'] = (current_cost + new_cost) / total_position

        self.log_message(f"买入 {stock} {amount} 股 @ {price:.2f}，总费用 {cost:.2f}，剩余资金 {self.cash:.2f}")
        return True

    def sell(self, stock: str, price: float, amount: int):
        """卖出操作"""
        if self.stocks_position[stock]['available'] < amount:
            self.log_message(f"持仓不足，无法卖出 {stock} {amount} 股 @ {price:.2f}")
            return False
            
        self.stocks_position[stock]['sell_amount'] += amount
        self.stocks_position[stock]['available'] -= amount

        revenue = float(price * amount)
        profit = revenue - self.stocks_position[stock]['cost_price'] * amount
        self.cash += decimal.Decimal(revenue)
        
        self.log_message(f"卖出 {stock} {amount} 股 @ {price:.2f}，获利 {profit:.2f}，剩余资金 {self.cash:.2f}")
        return True

    def _get_index_data(self):
        """获取指数数据"""
        try:
            user_sql = PySQL(
                host='localhost',
                user='afei',
                password='sf123456',
                database='stock',
                port=3306
            )
            user_sql.connect()
            
            # 构建查询条件
            where_clause = f'index_code = %s AND trade_date >= %s AND trade_date <= %s'
            params = [self.index_code, self.start_time.strftime('%Y-%m-%d'), self.end_time.strftime('%Y-%m-%d')]
            
            # 查询指数数据，确保包含所有需要的列
            index_data = user_sql.select('index_daily_k',
                                       columns=['trade_date', 'open', 'close', 'high', 'low', 'change_value', 'pct_change'],
                                       where=where_clause,
                                       params=params)
            
            # 转换为DataFrame
            df = pd.DataFrame(index_data)
            if not df.empty:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                # 确保数值列为float类型
                numeric_columns = ['open', 'close', 'high', 'low', 'change_value', 'pct_change']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                df.set_index('trade_date', inplace=True)
                df.sort_index(inplace=True)  # 确保按日期排序
            return df
            
        except Exception as e:
            print(f"获取指数数据失败: {e}")
            return pd.DataFrame()

    def calculate_returns(self, current_data):
        """计算当日收益和持仓情况"""
        market_cap = 0
        total_profit = 0
        
        if current_data.empty:
            return 0
            
        for stock in self.stock_list:
            stock_data = current_data[current_data['stock_code'] == stock]
            if stock_data.empty:
                continue
                
            position = self.stocks_position[stock]['available'] + self.stocks_position[stock]['unavailable']
            if position == 0:
                continue
                
            close = stock_data['close'].values[0]
            change_value = stock_data['change_value'].values[0]
            open_price = stock_data['open'].values[0]
            pct_change = stock_data['pct_change'].values[0]
            market_cap += float(close * position)
            cost_price = self.stocks_position[stock]['cost_price']
            pct_profit = (float(close)/self.stocks_position[stock]['cost_price'] - 1) * 100
            
            # 计算当日盈亏
            if self.stocks_position[stock]['unavailable'] == 0:  # 无交易
                stock_profit = float(change_value) * self.stocks_position[stock]['available']
            else:  # 有交易
                if self.current_date == self.start_time:
                    stock_profit = float(close - open_price) * self.stocks_position[stock]['unavailable']
                else:
                    position_profit = float(change_value) * self.stocks_position[stock]['available']
                    sell_profit = float(change_value) * self.stocks_position[stock]['sell_amount']
                    buy_profit = float(close - open_price) * self.stocks_position[stock]['unavailable']
                    stock_profit = position_profit + sell_profit + buy_profit
            
            total_profit += stock_profit
            
            # 记录单个股票的持仓信息
            self.log_message(f"持仓 {stock}: {position} 股，当日盈亏 {stock_profit:.2f}, 成本价 {cost_price}, 当日收盘价格 {close}, 当日涨跌幅 {pct_change:.2f}%, 持仓收益率 {pct_profit:.2f}%")
        
        # 计算总资产和收益率
        total_value = float(self.cash + decimal.Decimal(market_cap))
        returns = (total_value - self.initial_capital) / self.initial_capital * 100
        
        # 计算同期指数收益率
        try:
            if not self.index_data.empty and self.current_date in self.index_data.index:
                cost_index = self.index_data.loc[self.start_time, 'open']
                open_index = self.index_data.loc[self.current_date, 'open']
                close_index = self.index_data.loc[self.current_date, 'close']
                pct_change_index = self.index_data.loc[self.current_date, 'pct_change']
                
                # 当日指数收益率
                index_return = (close_index/open_index - 1) * 100
                
                # 持仓期指数收益率（从开始日到当前日）
                index_profit_rate = (close_index/cost_index - 1) * 100
                
                self.log_message(f"指数{self.index_code}当天收益率: {index_return:.2f}%, 当日涨跌幅{pct_change_index:.2f}%, 指数总收益率: {index_profit_rate:.2f}%")
                
                self.result[self.current_date] = {'total_profit_rate': returns, 'total_value': total_value, 'cash': self.cash, 'market_cap': market_cap, 
                                                 'index_total_profit_rate': index_profit_rate}
        except Exception as e:
            self.log_message(f"计算指数收益率时出错: {e}")
        
        # 记录总体信息
        self.log_message(f"当日总结: 总市值 {market_cap:.2f}，现金 {self.cash:.2f}，总资产 {total_value:.2f}，总盈亏 {total_profit:.2f}，总收益率 {returns:.2f}%")
        
        return returns
      
    def next(self):
        """执行下一个交易日的回测"""
        # 获取当前日期的数据
        current_data = self.data[self.data['trade_date'] == self.current_date]
        
        if not current_data.empty:
            # 执行交易策略
            self._apply_strategy(current_data)
            
            # 计算当日收益
            self.calculate_returns(current_data)
            self.log.write("\n")
        
        # 移动到下一天
        self.current_date += timedelta(days=1)
        
        # 更新可用持仓
        for stock in self.stock_list:
            if self.stocks_position[stock]['unavailable'] > 0:
                self.stocks_position[stock]['available'] += self.stocks_position[stock]['unavailable']
                self.stocks_position[stock]['unavailable'] = 0
    
    def _apply_strategy(self, current_data):
        """应用交易策略"""
        for stock in self.stock_list:
            stock_data = current_data[current_data['stock_code'] == stock]
            if stock_data.empty:
                continue
                
            self.open_price = stock_data['open'].values[0]
            self.close_price = stock_data['close'].values[0]

            self.strategy(stock)          
    
    def strategy(self,stock):
        """
        策略
        """
        # 示例策略：持仓不足100股时买入
        if self.stocks_position[stock]['available'] < 100:
            self.buy(stock, self.open_price, 100)
            
        # 结束日期卖出所有持仓
        if self.current_date == self.end_time:
            available_shares = self.stocks_position[stock]['available']
            if available_shares > 0:
                self.sell(stock, self.close_price, available_shares)

    def run_backtest(self):
        """运行回测过程"""
        while self.current_date <= self.end_time:
            self.next()
        
        self.close_log()

    def close_log(self):
        """关闭日志文件"""
        self.log.write("===========================================\n")
        self.log.write("回测结束\n")
        self.log.close()

        # 将字典转为DataFrame，并将外层键作为一列
        df = pd.DataFrame.from_dict(self.result, orient='index').reset_index()
        df.columns = ['trade_date', 'total_profit_rate', 'total_value', 'cash', 'market_cap', 'index_total_profit_rate']

        df.to_csv("output.csv", index=False, encoding='utf-8')




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
    stock_list = ['002594.XSHE','603881.XSHG']
    
    # 创建IN查询的占位符
    placeholders = ', '.join(['%s'] * len(stock_list))
    where_clause = f'trade_date > "2024-10-01" AND trade_date < "2025-05-20" AND stock_code IN ({placeholders})'
    
    stocks_data = user_sql.select('stock_daily_k',
                    columns=['stock_code','trade_date','open','high','low','close','change_value','pct_change'],
                    where=where_clause, 
                    params=stock_list)
    
    # 准备数据
    df = pd.DataFrame(stocks_data)
    df = df[['stock_code', 'trade_date', 'open', 'high', 'low', 'close', 'change_value','pct_change']]
    
    # 使用方法1：运行回测并可视化
    mybt = StockBacktest(df, initial_capital=100000, stock_list=stock_list)
    mybt.run_backtest()
    
    # 使用可视化器显示结果
    # visualizer = BacktestVisualizer(log_file='backtest_log.txt', port=8080)
    # visualizer.visualize()
    
    # 使用方法2：仅可视化已有的日志文件
    # visualizer = BacktestVisualizer(log_file='backtest_log.txt', port=8080)
    # visualizer.visualize()