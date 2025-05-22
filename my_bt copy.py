import pandas as pd
from datetime import datetime, timedelta
import decimal
from pysql import PySQL
from abc import ABC, abstractmethod


# 策略基类，所有策略都应该继承这个类
class Strategy(ABC):
    @abstractmethod
    def initialize(self, backtest):
        """初始化策略，设置参数等"""
        pass
        
    @abstractmethod
    def execute(self, backtest, current_date, current_data):
        """执行策略，根据当前数据执行交易决策"""
        pass
        
    @abstractmethod
    def get_name(self):
        """返回策略名称"""
        pass


# 简单买入持有策略：开始时买入，结束时卖出
class BuyAndHoldStrategy(Strategy):
    def __init__(self, buy_amount=100):
        self.buy_amount = buy_amount
    
    def initialize(self, backtest):
        pass
        
    def execute(self, backtest, current_date, current_data):
        for stock in backtest.stock_list:
            stock_data = current_data[current_data['stock_code'] == stock]
            if stock_data.empty:
                continue
                
            open_price = stock_data['open'].values[0]
            close_price = stock_data['close'].values[0]
            
            # 开始时买入
            if backtest.stocks_position[stock]['available'] < self.buy_amount:
                backtest.buy(open_price, self.buy_amount, stock)
                
            # 结束时卖出
            if current_date == backtest.end_time:
                available_shares = backtest.stocks_position[stock]['available']
                if available_shares > 0:
                    backtest.sell(stock, close_price, available_shares)
    
    def get_name(self):
        return f"买入持有策略(买入数量:{self.buy_amount})"


# 简单均线策略：短期均线上穿长期均线买入，下穿卖出
class SimpleMAStrategy(Strategy):
    def __init__(self, short_window=5, long_window=20, buy_amount=100):
        self.short_window = short_window
        self.long_window = long_window
        self.buy_amount = buy_amount
        self.signals = {}  # 存储每个股票的信号
        
    def initialize(self, backtest):
        # 计算每个股票的移动平均线
        for stock in backtest.stock_list:
            stock_data = backtest.data[backtest.data['stock_code'] == stock].copy()
            if len(stock_data) >= self.long_window:
                # 计算短期和长期移动平均线
                stock_data['short_ma'] = stock_data['close'].rolling(window=self.short_window).mean()
                stock_data['long_ma'] = stock_data['close'].rolling(window=self.long_window).mean()
                
                # 生成交易信号: 1=买入, -1=卖出, 0=持有
                stock_data['signal'] = 0
                stock_data['signal'][self.short_window:] = np.where(
                    stock_data['short_ma'][self.short_window:] > stock_data['long_ma'][self.short_window:], 1, 0)
                
                # 计算实际交易信号：信号变化点
                stock_data['position'] = stock_data['signal'].diff()
                
                self.signals[stock] = stock_data
    
    def execute(self, backtest, current_date, current_data):
        for stock in backtest.stock_list:
            if stock not in self.signals:
                continue
                
            stock_data = current_data[current_data['stock_code'] == stock]
            if stock_data.empty:
                continue
            
            signal_data = self.signals[stock]
            today_signal = signal_data[signal_data['trade_date'] == current_date]
            
            if today_signal.empty:
                continue
                
            # 获取今日价格
            open_price = stock_data['open'].values[0]
            close_price = stock_data['close'].values[0]
            
            # 获取今日信号
            position = today_signal['position'].values[0]
            
            # 根据信号执行交易
            if position > 0:  # 买入信号
                if backtest.stocks_position[stock]['available'] < self.buy_amount:
                    backtest.buy(open_price, self.buy_amount, stock)
            elif position < 0:  # 卖出信号
                available_shares = backtest.stocks_position[stock]['available']
                if available_shares > 0:
                    backtest.sell(stock, close_price, available_shares)
            
            # 结束时强制平仓
            if current_date == backtest.end_time:
                available_shares = backtest.stocks_position[stock]['available']
                if available_shares > 0:
                    backtest.sell(stock, close_price, available_shares)
    
    def get_name(self):
        return f"简单均线策略(短期:{self.short_window},长期:{self.long_window})"


# 添加更多自定义策略...


class StockBacktest:
    def __init__(self, data: pd.DataFrame, initial_capital: float = 100000, log_file: str = 'backtest_log.txt',
                 start_time: str = None, end_time: str = None, stock_list: list = [], strategy=None):
        """
        初始化回测类
        :param data: 包含股票数据的DataFrame，应该有stock_code, trade_date, open, high, low, close等列
        :param initial_capital: 初始资金
        :param log_file: 日志文件路径
        :param start_time: 回测开始时间，格式：'YYYY-MM-DD'
        :param end_time: 回测结束时间，格式：'YYYY-MM-DD'
        :param stock_list: 股票代码列表
        :param strategy: 交易策略，为None时使用默认策略
        """
        self.data = data
        self.initial_capital = initial_capital
        self.data['trade_date'] = pd.to_datetime(self.data['trade_date'])
        self.cash = decimal.Decimal(initial_capital)  # 将initial_capital转换为decimal.Decimal类型
        self.balance = decimal.Decimal(initial_capital)  # 将initial_capital转换为decimal.Decimal类型
        self.history = []  # 存储历史交易记录
        self.current_date = self.data['trade_date'].min()  # 回测开始时间点
        
        self.log_file_name = log_file  # 日志文件路径
        
        # 设置回测时间范围
        self.start_time = pd.to_datetime(start_time) if start_time else self.data['trade_date'].min()
        self.end_time = pd.to_datetime(end_time) if end_time else self.data['trade_date'].max()
        
        # 过滤数据在时间范围内的部分
        self.data = self.data[(self.data['trade_date'] >= self.start_time) & 
                             (self.data['trade_date'] <= self.end_time)].reset_index(drop=True)
        
        self.stock_list = stock_list
        self.stocks_position = {stock:{'available':0,'unavailable':0,'cost_price':0.0,'sell_amount':0} for stock in self.stock_list} 
        
        # 设置策略
        self.strategy = strategy if strategy else BuyAndHoldStrategy()
        self.log_file()
        
        # 初始化策略
        self.strategy.initialize(self)
        
        # 记录策略名称
        self.log_message(f"使用策略: {self.strategy.get_name()}")
        
        # 启动回测
        self.run_backtest()  # 启动回测

    def log_file(self):
        # 打开日志文件，使用 UTF-8 编码
        self.log = open(self.log_file_name, 'w', encoding='utf-8')
        self.log.write(f"回测日志 - 初始资本: {self.initial_capital}\n")
        self.log.write("===========================================\n")

    def log_message(self, message: str):
        """
        将消息写入日志文件，并记录回测数据的时间戳
        :param message: 要记录的消息
        """
        log_entry = f"[{datetime.strftime(self.current_date, '%Y-%m-%d')}] {message}"
        self.log.write(log_entry + "\n")
        print(log_entry)  # 同时打印在控制台中
    
    def buy(self, price: float, amount: int, stock):
        """
        执行买入操作
        :param price: 买入价格
        :param amount: 买入数量
        """
        cost = price * amount
        if cost > self.cash:
            self.log_message(f"资金不足，无法买入 {amount} 股 @ {price:.2f}")
            return False
        self.cash -= decimal.Decimal(cost)  # 将cost转换为decimal.Decimal类型
        self.stocks_position[stock]['unavailable'] = amount  # 不可用持仓
        if self.stocks_position[stock]['cost_price'] == 0:
            self.stocks_position[stock]['cost_price'] = float(price)
        else:
            p = self.stocks_position[stock]['cost_price']*self.stocks_position[stock]['available'] + float(price) * self.stocks_position[stock]['unavailable']
            position = self.stocks_position[stock]['available'] + self.stocks_position[stock]['unavailable']
            self.stocks_position[stock]['cost_price'] =  p / position

        trade_message = f"买入 {stock} {amount} 股 @ {price:.2f}，总费用 {cost:.2f}，剩余资金 {self.cash:.2f}"
        self.history.append(('BUY', self.current_date, stock, price, amount))
        self.log_message(trade_message)
        return True

    def sell(self, stock, price: float, amount: int):
        """
        执行卖出操作
        :param price: 卖出价格
        :param amount: 卖出数量
        """
        if self.stocks_position[stock]['available'] < amount:
            self.log_message(f"持仓不足，无法卖出 {amount} 股 @ {price:.2f}")
            return False
        
        self.stocks_position[stock]['sell_amount'] += amount
        self.stocks_position[stock]['available'] -= amount

        revenue = float(price * amount)
        profit = revenue - self.stocks_position[stock]['cost_price'] * amount
        self.cash += decimal.Decimal(revenue)  # 将revenue转换为decimal.Decimal类型
        trade_message = f"卖出 {amount} 股 @ {price:.2f}，获利 {profit:.2f}，剩余资金 {self.cash:.2f}"
        self.history.append(('SELL', self.current_date, stock, price, amount))
        self.log_message(trade_message)
        return True

    def calculate_returns(self, current_data):
        """
        计算当日收益，持有市值，剩余资金，持票数量，收益率
        """
        market_cap = 0
        total_profit = 0
        
        if not current_data.empty:
            for stock in self.stock_list:
                stock_data = current_data[current_data['stock_code'] == stock]
                if not stock_data.empty:
                    position = self.stocks_position[stock]['available'] + self.stocks_position[stock]['unavailable']
                    close = stock_data['close'].values[0]
                    change_value = stock_data['change_value'].values[0]
                    open = stock_data['open'].values[0]
                    
                    # 计算单个股票的市值和收益
                    stock_market_cap = position * close
                    if self.stocks_position[stock]['unavailable'] == 0:  # 无交易
                        stock_profit = float(change_value) * self.stocks_position[stock]['available']
                    else: # 有交易
                        sell_profit = 0
                        buy_profit = 0
                        if self.current_date == self.start_time:
                            stock_profit = float(close-open) * self.stocks_position[stock]['unavailable']
                        else:
                            position_profit = float(change_value) * self.stocks_position[stock]['available']
                            sell_profit = float(change_value) * self.stocks_position[stock]['sell_amount']
                            buy_profit = float(close-open) * self.stocks_position[stock]['unavailable']
                            stock_profit = position_profit + sell_profit + buy_profit
                    
                    
                    market_cap += stock_market_cap
                    total_profit += stock_profit
                    
                    # 记录单个股票的持仓信息
                    if position > 0:
                        trade_message = f"持仓 {stock}:  {position} 股，市值 {stock_market_cap:.2f}，当日盈亏 {stock_profit:.2f}"
                        self.log_message(trade_message)
                    
            
            # 计算总资产和收益率
            total_value = self.cash + decimal.Decimal(market_cap)
            returns = (total_value - self.initial_capital) / self.initial_capital * 100
            
            # 记录总体信息
            summary_message = f"当日总结: 总市值 {market_cap:.2f}，现金 {self.cash:.2f}，总资产 {total_value:.2f}，总盈亏 {total_profit:.2f}，收益率 {returns:.2f}%"
            self.log_message(summary_message)
            
            return returns
        else:
            print('当日无交易数据')
            return 0

    def position(self, stock, close):
        """
        持仓
        """
        position = self.stocks_position[stock]['available'] + self.stocks_position[stock]['unavailable']
        profit = (float(close) - self.stocks_position[stock]['cost_price']) * position
        trade_message = f"持仓 {stock} {position} 股，当日盈亏{profit}"
        self.log_message(trade_message)
      
    def next(self):
        """
        进行下一步回测
        每调用一次，模拟一次交易
        """
        # 获取当前日期的数据
        current_data = self.data[self.data['trade_date'] == self.current_date]        

        if not current_data.empty:
            # 执行策略
            self.strategy.execute(self, self.current_date, current_data)
            
            # 计算当日收益
            self.calculate_returns(current_data)
            self.log.write("\n")
        
        # 移动到下一天
        self.current_date += timedelta(days=1)
        
        # 更新可用持仓
        for stock, position in self.stocks_position.items():
            if self.stocks_position[stock]['unavailable'] > 0:
                self.stocks_position[stock]['available'] += self.stocks_position[stock]['unavailable']
                self.stocks_position[stock]['unavailable'] = 0

    def run_backtest(self):
        """
        自动运行回测，直到所有数据处理完成
        """
        while self.current_date <= self.end_time:
            self.next()
        
        self.log_message("回测结束")
        self.close_log()

    def get_history(self):
        """
        获取所有交易历史
        :return: 交易历史
        """
        return self.history

    def close_log(self):
        """
        关闭日志文件
        """
        self.log.write("===========================================\n")
        self.log.write("回测结束\n")
        self.log.close()


# 双均线策略示例
class DoubleMAStrategy(Strategy):
    def __init__(self, short_period=5, long_period=20, buy_amount=100):
        self.short_period = short_period
        self.long_period = long_period
        self.buy_amount = buy_amount
        # 用于存储每个股票的均线数据
        self.stock_ma_data = {}
        
    def initialize(self, backtest):
        # 为每个股票计算均线
        for stock in backtest.stock_list:
            stock_data = backtest.data[backtest.data['stock_code'] == stock].copy()
            if len(stock_data) >= self.long_period:
                # 计算短期和长期移动平均
                stock_data.sort_values('trade_date', inplace=True)
                stock_data['ma_short'] = stock_data['close'].rolling(window=self.short_period).mean()
                stock_data['ma_long'] = stock_data['close'].rolling(window=self.long_period).mean()
                self.stock_ma_data[stock] = stock_data
                
                # 记录初始化信息
                backtest.log_message(f"初始化 {stock} 的均线数据 (短期: {self.short_period}, 长期: {self.long_period})")
    
    def execute(self, backtest, current_date, current_data):
        for stock in backtest.stock_list:
            if stock not in self.stock_ma_data:
                continue
                
            # 获取当前股票数据
            stock_data = current_data[current_data['stock_code'] == stock]
            if stock_data.empty:
                continue
                
            # 获取当日均线数据
            ma_data = self.stock_ma_data[stock]
            today_ma = ma_data[ma_data['trade_date'] == current_date]
            
            if today_ma.empty or pd.isna(today_ma['ma_short'].values[0]) or pd.isna(today_ma['ma_long'].values[0]):
                continue
                
            # 获取价格数据
            close_price = float(stock_data['close'].values[0])
            open_price = float(stock_data['open'].values[0])
            
            # 获取均线数据
            ma_short = float(today_ma['ma_short'].values[0])
            ma_long = float(today_ma['ma_long'].values[0])
            
            # 获取昨日均线数据（如果有）
            yesterday = current_date - timedelta(days=1)
            yesterday_ma = ma_data[ma_data['trade_date'] == yesterday]
            
            # 交易逻辑：短期均线上穿长期均线买入，下穿卖出
            if not yesterday_ma.empty and not pd.isna(yesterday_ma['ma_short'].values[0]) and not pd.isna(yesterday_ma['ma_long'].values[0]):
                yesterday_ma_short = float(yesterday_ma['ma_short'].values[0])
                yesterday_ma_long = float(yesterday_ma['ma_long'].values[0])
                
                # 金叉：短期均线从下方穿过长期均线
                if yesterday_ma_short <= yesterday_ma_long and ma_short > ma_long:
                    # 执行买入
                    if backtest.stocks_position[stock]['available'] == 0:
                        backtest.buy(open_price, self.buy_amount, stock)
                
                # 死叉：短期均线从上方穿过长期均线
                elif yesterday_ma_short >= yesterday_ma_long and ma_short < ma_long:
                    # 执行卖出
                    available = backtest.stocks_position[stock]['available']
                    if available > 0:
                        backtest.sell(stock, close_price, available)
            
            # 结束时平仓
            if current_date == backtest.end_time:
                available = backtest.stocks_position[stock]['available']
                if available > 0:
                    backtest.sell(stock, close_price, available)
    
    def get_name(self):
        return f"双均线交叉策略 (短期:{self.short_period}, 长期:{self.long_period})"


if __name__ == '__main__':
    import numpy as np
    
    user_sql = PySQL(
            host='localhost',
            user='afei',
            password='sf123456',
            database='stock',
            port=3306
        )
    user_sql.connect()
    stock_list = ['000001.XSHE', '000002.XSHE']
    
    # 创建IN查询的占位符
    placeholders = ', '.join(['%s'] * len(stock_list))
    where_clause = f'trade_date > "2025-05-01" AND trade_date < "2025-05-20" AND stock_code IN ({placeholders})'
    
    stocks_data = user_sql.select('stock_daily_k',
                    columns=['stock_code','trade_date','open','high','low','close','change_value'],
                    where=where_clause, 
                    params=stock_list)
    
    df = pd.DataFrame(stocks_data)
    df = df[['stock_code','trade_date','open','high','low','close','change_value']]
    
    # 使用默认策略
    mybt1 = StockBacktest(df, initial_capital=100000, stock_list=stock_list, log_file='backtest_default.txt')
    
    # 使用双均线策略
    mybt2 = StockBacktest(df, initial_capital=100000, stock_list=stock_list, 
                         strategy=DoubleMAStrategy(short_period=3, long_period=7),
                         log_file='backtest_doublema.txt')