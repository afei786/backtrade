import pandas as pd
from datetime import datetime, timedelta

class StockBacktest:
    def __init__(self, data: pd.DataFrame, initial_capital: float = 100000, log_file: str = 'backtest_log.txt',
                 start_time: str = None, end_time: str = None, stock_list: list = []):
        """
        初始化回测类
        :param data: 包含股票数据的DataFrame，应该有stock_code, trade_date, open, high, low, close等列
        :param initial_capital: 初始资金
        :param log_file: 日志文件路径
        :param start_time: 回测开始时间，格式：'YYYY-MM-DD'
        :param end_time: 回测结束时间，格式：'YYYY-MM-DD'
        :param stock_list: 股票代码列表
        """
        self.data = data
        self.initial_capital = initial_capital
        self.data['trade_date'] = pd.to_datetime(self.data['trade_date'])
        self.cash = initial_capital  # 初始资金
        self.balance = initial_capital  # 当前账户余额
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
        self.stocks_position = {stock:{'available':0,'unavailable':0,'cost_price':0.0} for stock in self.stock_list} 
        self.log_file()
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
        log_entry = f"[{self.current_date}] {message}"
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
            return
        self.cash -= cost  # 扣除资金
        self.stocks_position[stock]['unavailable'] = amount  # 不可用持仓
        if self.stocks_position[stock]['cost_price'] == 0:
            self.stocks_position[stock]['cost_price'] = float(price)
        else:
            p = self.stocks_position[stock]['cost_price']*self.stocks_position[stock]['available'] + float(price) * self.stocks_position[stock]['unavailable']
            position = self.stocks_position[stock]['available'] + self.stocks_position[stock]['unavailable']
            self.stocks_position[stock]['cost_price'] =  p / position
  # 成本价

        trade_message = f"买入 {stock} {amount} 股 @ {price:.2f}，总费用 {cost:.2f}，剩余资金 {self.cash:.2f}）"
        # self.history.append(('BUY', self.data.iloc[self.current_idx]['trade_date'], price, amount))
        self.log_message(trade_message)

    def sell(self, price: float, amount: int):
        """
        执行卖出操作
        :param price: 卖出价格
        :param amount: 卖出数量
        """
        if amount > self.position:
            self.log_message(f"持仓不足，无法卖出 {amount} 股 @ {price:.2f}")
            return
        revenue = price * amount
        self.cash += revenue  # 增加资金
        self.position -= amount  # 减少持仓
        trade_message = f"卖出 {amount} 股 @ {price:.2f}，总收入 {revenue:.2f}，剩余资金 {self.cash:.2f}"
        self.history.append(('SELL', self.data.iloc[self.current_idx]['trade_date'], price, amount))
        self.log_message(trade_message)

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
                    
                    # 计算单个股票的市值和收益
                    stock_market_cap = position * close
                    if self.stocks_position[stock]['unavailable'] == 0:  # 无交易
                        stock_profit = float(change_value) * self.stocks_position[stock]['available']
                    else: # 有交易
                        sell_profit = 0
                        buy_profit = 0
                        stock_profit = float(change_value) * self.stocks_position[stock]['available']

                    
                    
                    market_cap += stock_market_cap
                    total_profit += stock_profit
                    
                    # 记录单个股票的持仓信息
                    if position > 0:
                        trade_message = f"持仓 {stock}:  {position} 股，市值 {stock_market_cap:.2f}，当日盈亏 {stock_profit:.2f}"
                        self.log_message(trade_message)
                    
            
            # 计算总资产和收益率
            total_value = self.cash + market_cap
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
            # 遍历stocks
            for stock in self.stock_list:
                stock_data = current_data[current_data['stock_code'] == stock]
                if not stock_data.empty:
                    open_price = stock_data['open'].values[0]  # 获取开盘价
                    # close = stock_data['close'].values[0]  # 获取收盘价
                    
                    if self.stocks_position[stock]['available'] < 100:
                        self.buy(open_price, 100, stock)
                    # else:
                    #     self.position(stock, close)
            
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
        else:
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



