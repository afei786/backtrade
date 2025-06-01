import pandas as pd
from datetime import datetime, timedelta
import decimal
from pysql import PySQL
from tqdm import tqdm  # 导入tqdm库
from filter_stocks import filter_stocks_by_price_range


class StockBacktest:
    def __init__(self, data: pd.DataFrame, initial_capital: float = 100000, log_file: str = 'backtest_log.txt',
                 start_time: str = None, end_time: str = None, stock_list: list = None, index_code: str = '000300.SH',
                 show_progress: bool = True):
        """
        初始化回测类
        :param data: 包含股票数据的DataFrame，应该有stock_code, trade_date, open, high, low, close等列
        :param initial_capital: 初始资金
        :param log_file: 日志文件路径
        :param start_time: 回测开始时间，格式：'YYYY-MM-DD'
        :param end_time: 回测结束时间，格式：'YYYY-MM-DD'
        :param stock_list: 股票代码列表
        :param index_code: 对比指数代码，默认为沪深300
        :param show_progress: 是否显示进度条，默认为True
        """
        # 数据预处理
        self.data = data.copy()
        self.data['trade_date'] = pd.to_datetime(self.data['trade_date'])
        self.sql = PySQL(
                host='localhost',
                user='afei',
                password='sf123456',
                database='stock',
                port=3306
            )
        self.sql.connect()

        # 初始化资金和统计信息
        self.initial_capital = initial_capital
        self.cash = decimal.Decimal(initial_capital)
        self.balance = decimal.Decimal(initial_capital)
        self.result = {}  # 每日回测结果
        self.max_stock_num = 100
        self.show_progress = show_progress  # 添加进度条显示控制参数

        # 设置回测时间范围
        self.start_time = pd.to_datetime(start_time) if start_time else self.data['trade_date'].min()
        self.end_time = pd.to_datetime(end_time) if end_time else self.data['trade_date'].max()
        self.current_date = self.start_time
        
        # 过滤数据在时间范围内的部分
        self.data = self.data[(self.data['trade_date'] >= self.start_time) & 
                             (self.data['trade_date'] <= self.end_time)].reset_index(drop=True)
        
        # 设置股票列表和初始化持仓
        self.stock_pool = stock_list  # 股票池
        self.stocks_position = {}  # 持仓
        self.zy_list = []  # 用于记录已止盈卖出的股票
        # self.stocks_position = {stock: {'available': 0, 'unavailable': 0, 'cost_price': 0.0, 'sell_amount': 0} 
        #                         for stock in self.stock_list}
        
        # 获取指数数据
        self.index_code = index_code
        self.index_data = self._get_index_data()
        if not self.index_data.empty:
            self.initial_index_price = float(self.index_data.iloc[0]['open'])  # 初始指数价格
        
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
    
    def buy(self, stock: str, price: float, amount: int, additional: bool = False):
        """买入操作"""
        if stock in self.zy_list:  # 如果股票在止盈列表中，则不再买入
            return   
        cost = price * amount
        if cost > self.cash:
            self.log_message(f"资金不足，无法买入 {stock} {amount} 股 @ {price:.2f}")
            return False
            
        self.cash -= decimal.Decimal(cost)
        if stock not in self.stocks_position:
            self.stocks_position[stock] = {'available': 0, 'unavailable': 0, 'cost_price': 0.0, 'sell_amount': 0}
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
        if not additional:
            self.log_message(f"买入 {stock} {amount} 股 @ {price:.2f}，总费用 {cost:.2f}，剩余资金 {self.cash:.2f}")
            self.trade_log[stock] = {
                "Operation": "买入",
                "name": stock,
                "amount": amount,
                "buy_price": price,
                "value": f"{amount * price:.2f}",
                "profit": f"{(self.close_price-price) * amount:.2f}" # 计算买入后收盘价的盈利
            }

        else:
            self.log_message(f"补仓 {stock} {amount} 股 @ {price:.2f}，总费用 {cost:.2f}，剩余资金 {self.cash:.2f}")
            self.trade_log[stock] = {
                "Operation": "补仓",
                "amount": amount,
                "buy_price": price,
                "value": f"{amount * price:.2f}",
                "profit": f"{(self.close_price-self.stocks_position[stock]['cost_price']) * amount:.2f}"  # 计算补仓后收盘价的盈利
            }

        return True

    def sell(self, stock: str, price: float, amount: int):
        """卖出操作"""
        if self.stocks_position[stock]['available'] == 0 or amount > self.stocks_position[stock]['available']:
            self.log_message(f"持仓不足，无法卖出 {stock} {amount} 股 @ {price:.2f}")
            return 

        if amount == -1:
            revenue = float(price * self.stocks_position[stock]['available'])  # 卖出金额
            profit = revenue - self.stocks_position[stock]['cost_price'] * self.stocks_position[stock]['available']  # 盈利
            self.cash += decimal.Decimal(revenue)  # 更新现金
            self.log_message(f"清仓 {stock} @ {self.stocks_position[stock]['cost_price']:.2f} @ {self.stocks_position[stock]['available']}，获利 {profit:.2f}，剩余资金 {self.cash:.2f}")
            self.trade_log[stock] = {
                "Operation": "清仓",
                "amount": self.stocks_position[stock]['available'],
                "sell_price": price,
                "revenue": f"{revenue:.2f}",
                "profit": f"{profit:.2f}",
            }
            # 直接删除持仓信息
            del self.stocks_position[stock]
            self.zy_list.append(stock)
        else:
            revenue = float(price * amount)  # 卖出金额
            profit = revenue - self.stocks_position[stock]['cost_price'] * amount  # 卖出盈利
            self.cash += decimal.Decimal(revenue)  # 更新现金
            self.stocks_position[stock]['available'] -= amount
            self.log_message(f"卖出 {stock} {amount} 股 @ {price:.2f}，获利 {profit:.2f}，剩余资金 {self.cash:.2f}")


    def _get_index_data(self):
        """获取指数数据"""
        try:           
            # 构建查询条件
            where_clause = f'index_code = %s AND trade_date >= %s AND trade_date <= %s'
            params = [self.index_code, self.start_time.strftime('%Y-%m-%d'), self.end_time.strftime('%Y-%m-%d')]
            
            # 查询指数数据，确保包含所有需要的列
            index_data = self.sql.select('index_daily_k',
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
      

    def next(self):
        """执行下一个交易日的回测"""
        # 获取当前日期的数据
        self.trade_log = {}
        current_data = self.data[self.data['trade_date'] == self.current_date]
        
        if not current_data.empty:
            # 检查持仓
            if len(self.stocks_position) > 0:
                self.log_message(f'盘前整理')
                # 使用列表副本遍历，避免在遍历过程中修改字典结构
                stocks_position_keys = list(self.stocks_position.keys())
                for stock in stocks_position_keys:
                    
                    stock_data = current_data[current_data['stock_code'] == stock]
                    if stock_data.empty:
                        continue
                        
                    self.open_price = float(stock_data['open'].values[0])
                    self.close_price = float(stock_data['close'].values[0])
                    self.check_position(stock)
                self.log_message(f'盘前整理完成')

            # 执行交易策略
            self._apply_strategy(current_data)
            
            # 更新可用持仓
            self.log_message("当日可用持仓:")
            market_cap = 0  # 持仓市值
            total_profit = 0  # 总盈利
            stock_num = 0  # 持仓股票数量

            for stock in list(self.stocks_position.keys()):  # 使用列表副本遍历，避免在遍历过程中修改字典结构
                stock_data = current_data[current_data['stock_code'] == stock]
                if stock_data.empty:
                    continue
                    
                self.close_price = float(stock_data['close'].values[0])

                if self.stocks_position[stock]['unavailable'] > 0:  # 更新持仓 t+1
                    self.stocks_position[stock]['available'] += self.stocks_position[stock]['unavailable']
                    self.stocks_position[stock]['unavailable'] = 0

                elif self.stocks_position[stock]['available'] == 0 and self.stocks_position[stock]['unavailable'] == 0:  # 删除已清仓股票
                    del self.stocks_position[stock]
                    continue
                stock_num += 1
                self.stocks_position[stock]['close_price'] = self.close_price
                available_amount = self.stocks_position[stock]['available']
                profit_rate = (self.close_price / self.stocks_position[stock]['cost_price'] - 1) * 100
                cost_price = self.stocks_position[stock]['cost_price']
                close_price = self.stocks_position[stock]['close_price']
                profit = (self.close_price - self.stocks_position[stock]['cost_price']) * available_amount  # 计算盈利



                market_cap += float(self.close_price * self.stocks_position[stock]['available'])

                self.log_message(f"{stock} 持仓: {available_amount}, 成本价：{cost_price:.2f}, 收盘价：{close_price:.2f}, 当日盈亏：{profit:.2f}, 收益率: {profit_rate:.2f}%")
            
            total_profit = market_cap + float(self.cash) - float(self.initial_capital)
            total_profit_rate = total_profit/self.initial_capital*100  # 总收益率
            total_assets = market_cap + float(self.cash)  # 总资产
            self.log_message(f"当日持仓总市值: {market_cap:.2f}，现金: {self.cash:.2f}，总资产: {total_assets:.2f}，总盈利: {total_profit:.2f}, 总收益率: {total_profit_rate:.2f}% ")
            self.log_message(f"当日持仓股票数量: {stock_num}")
            self.log_message(f"止盈股票数量: {len(self.zy_list)}")

            # 记录指数收益率
            index_total_profit_rate = (self.index_data.loc[self.current_date, 'close'] / self.initial_index_price - 1) * 100 if not self.index_data.empty else 0
            if self.current_date == self.start_time:  
                self.log_message(f"当日指数收益率: {(self.index_data.loc[self.current_date, 'close']/self.initial_index_price-1)*100:.2f}%, 累计收益率{index_total_profit_rate:.2f}%\n")
            else:
                self.log_message(f"当日指数收益率: {self.index_data.loc[self.current_date, 'pct_change']:.2f}%, 累计收益率{index_total_profit_rate:.2f}%\n")
            # 记录每日回测结果
            self.result[self.current_date.strftime('%Y-%m-%d')] = {
                'total_profit_rate': f"{total_profit_rate:.2f}%",
                'total_assets': f"{total_assets:.2f}",
                'cash': f"{float(self.cash):.2f}",
                'market_cap': market_cap,
                'index_total_profit_rate': f"{index_total_profit_rate:.2f}%",
                "trade_log":self.trade_log,
            }
            # 计算指数基准

        # 移动到下一天
        self.current_date += timedelta(days=1)

    def _apply_strategy(self, current_data):
        """应用交易策略"""  
            
        for stock in self.stock_pool:
            if self.cash < 5000:
                self.log_message("资金不足5000，暂停交易，等待资金恢复")
                return
            if len(self.stocks_position) > self.max_stock_num:
                self.log_message(f"股票数量超过{self.max_stock_num}，暂停交易，等待股票数量减少")
                return
            
            stock_data = current_data[current_data['stock_code'] == stock]
            if stock_data.empty:
                continue
                
            self.open_price = float(stock_data['open'].values[0])
            self.close_price = float(stock_data['close'].values[0])

            self.strategy(stock)          
    
    def check_position(self, stock):
        """检查持仓情况"""
        if self.open_price/self.stocks_position[stock]['cost_price'] > 1.15:  # 盈利15%卖出
            self.sell(stock, self.open_price, -1)
        
        elif self.open_price/self.stocks_position[stock]['cost_price'] < 0.85 :  # 亏损20%补仓
            self.buy(stock, self.open_price, 100, additional=True)
        
    def strategy(self,stock):
        """
        盘中买入策略
        """
        if stock not in self.stocks_position and stock not in self.zy_list:
            self.stocks_position[stock] = {'available': 0, 'unavailable': 0, 'cost_price': 0.0, 'close_price': 0.0}  #, 'sell_amount': 0}
            self.buy(stock, self.open_price, 100)  # 建仓
        
    def run_backtest(self):
        """运行回测过程"""
        # 计算总天数
        total_days = (self.end_time - self.current_date).days + 1
        
        if self.show_progress:
            # 使用tqdm创建进度条，添加更多信息
            with tqdm(total=total_days, desc="回测进度", unit="天") as pbar:
                while self.current_date <= self.end_time:
                    # 更新进度条描述，显示当前日期
                    pbar.set_description(f"回测日期: {self.current_date.strftime('%Y-%m-%d')}")
                    
                    self.next()
                    
                    # 更新进度条
                    pbar.update(1)
                    
                    # 添加进度条后缀，显示处理进度
                    processed_days = pbar.n
                    pbar.set_postfix(已处理=f"{processed_days}/{total_days}天", 
                                    完成率=f"{processed_days/total_days:.1%}")
        else:
            # 不显示进度条
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
        df.columns = ['trade_date', 'total_profit_rate', 'total_value', 'cash', 'market_cap', 'index_total_profit_rate', 'trade_log']

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
    start_date = '2025-01-01'
    end_date = '2025-06-01'
    result = filter_stocks_by_price_range(start_date, min_price=5, max_price=10)
    stock_list = [item['stock_code'] for item in result]

    # 随机打乱股票列表
    import random
    random.seed(415643)  # 设置随机种子以确保可重复性
    random.shuffle(stock_list)
    # stock_list = stock_list[:1000]
    
    # stock_list = ['002594.XSHE']
    
    
    # 创建IN查询的占位符
    placeholders = ', '.join(['%s'] * len(stock_list))
    where_clause = f'trade_date > "{start_date}" AND trade_date < "{end_date}" AND stock_code IN ({placeholders})'
    
    stocks_data = user_sql.select('stock_daily_k',
                    columns=['stock_code','trade_date','open','high','low','close','change_value','pct_change'],
                    where=where_clause, 
                    params=stock_list)
    
    # 准备数据
    df = pd.DataFrame(stocks_data)
    df = df[['stock_code', 'trade_date', 'open', 'high', 'low', 'close', 'change_value','pct_change']]
    
    # 使用方法1：运行回测并显示进度条（默认）
    mybt = StockBacktest(df, initial_capital=100000, stock_list=stock_list, show_progress=True)
    mybt.run_backtest()

    from backtest_report_generator import main
    main()
    