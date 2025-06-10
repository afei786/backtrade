import pandas as pd
from datetime import datetime, timedelta
import decimal
from pysql import PySQL
from tqdm import tqdm  # 导入tqdm库
from filter_stocks import filter_stocks_by_price_range
import random
import yaml


class StockBacktest:
    def __init__(self, config_file="settings.yaml"):
        """
        初始化回测类
        """
        # 读取配置文件
        with open(config_file, 'r', encoding='utf-8') as file:
            self.settings = yaml.safe_load(file)
        
        # sql配置
        self.sql = PySQL(
                host=self.settings['sql']['host'],
                user=self.settings['sql']['user'],
                password=self.settings['sql']['password'],
                database=self.settings['sql']['database'],
                port=self.settings['sql']['port']
            )
        self.sql.connect()

        # 获取股票列表
        self.stock_list = filter_stocks_by_price_range(self.sql, min_price=self.settings['min_price'], max_price=self.settings['max_price'],
                                                  min_market_cap=self.settings['min_market_cap'], max_market_cap=self.settings['max_market_cap'],
                                                  region=self.settings['region'], not_market_type=self.settings['not_market_type'],
                                                  start_date=self.settings['start_time'], end_date=self.settings['end_time'])
        
        # 获取股票数据
        self.data = self.get_stock_data()  # 获取股票数据

        # 转换数据格式
        self.data['trade_date'] = pd.to_datetime(self.data['trade_date'])

        # 初始化资金和统计信息
        self.cash = self.settings["initial_capital"]
        self.initial_capital = self.settings["initial_capital"]
        self.result = {}  # 每日回测结果
        self.show_progress = self.settings["show_progress"]  # 添加进度条显示控制参数

        self.profit = 0.0  # 总盈利
        self.profit_rate = 0.0  # 总收益率

        # 记录持仓
        self.position_log = {}  # 'stock_code': {'is_position': True, 'position': 100, 'cost_price': 10.0,'price': 0.0, 'profit': 0.0}

        # 设置回测时间范围
        self.start_time = pd.to_datetime(self.settings['start_time']) if self.settings['start_time'] else self.data['trade_date'].min()
        self.end_time = pd.to_datetime(self.settings['end_time']) if self.settings['end_time'] else self.data['trade_date'].max()
        self.current_date = self.start_time
        
        # 过滤数据在时间范围内的部分
        self.data = self.data[(self.data['trade_date'] >= self.start_time) & 
                             (self.data['trade_date'] <= self.end_time)].reset_index(drop=True)
        
        # # 设置股票列表和初始化持仓
        self.stock_pool = self.stock_list  # 股票池
        self.stocks_position = {}  # 持仓
        self.zy_list = {}  # 用于记录已止盈卖出的股票
        self.max_stock_num = self.settings['max_stock_num']  # 最大持仓股票数量
        self.zy_rate = self.settings['zy_rate']  # 止盈率
        self.zs_rate = self.settings['zs_rate']
        # self.stocks_position = {stock: {'available': 0, 'unavailable': 0, 'cost_price': 0.0, 'sell_amount': 0} 
        #                         for stock in self.stock_list}
        
        # # 获取指数数据
        self.index_code = self.settings['index_code']
        self.index_data = self._get_index_data()
        if not self.index_data.empty:
            self.initial_index_price = float(self.index_data.iloc[0]['open'])  # 初始指数价格
        
        # # 初始化日志
        self.log_file_name = self.settings['log_file']
        self._init_log()
        
        # 启动回测
        # self.run_backtest()

    def get_stock_data(self,):
        if not self.stock_list:
            print(f"没有找到符合条件的股票，区域: {self.settings['region']}, 起始日期: {self.settings['start_time']}, 结束日期: {self.settings['end_time']}")
            return None
        
        # 打乱股票列表
        random.seed(self.settings['random_seed'])
        random.shuffle(self.stock_list)
        
        # stock_list = ['002594.XSHE']
        
        # 创建IN查询的占位符
        placeholders = ', '.join(['%s'] * len(self.stock_list))
        where_clause = f'trade_date > "{self.settings["start_time"]}" AND trade_date < "{self.settings["end_time"]}" AND stock_code IN ({placeholders})'
        
        self.stocks_data = self.sql.select('stock_daily_k',
                        columns=['stock_code','trade_date','open','high','low','close','change_value','pct_change','ma5','ma10','ma20','ma30','ma45','ma60'],
                        where=where_clause, 
                        params=self.stock_list)
        
        # 准备数据
        df = pd.DataFrame(self.stocks_data)
        df = df[['stock_code', 'trade_date', 'open', 'high', 'low', 'close', 'change_value','pct_change','ma5','ma10','ma20','ma30','ma45','ma60']]
        return df
        

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
            self.stocks_position[stock] = {'available': 0, 'unavailable': 0, 'cost_price': 0.0, 'sell_amount': 0, 'buy_date': self.current_date}
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
                "amount": amount,
                "buy_price": price,
            }

        else:
            self.log_message(f"补仓 {stock} {amount} 股 @ {price:.2f}，总费用 {cost:.2f}，剩余资金 {self.cash:.2f}")
            self.trade_log[stock] = {
                "Operation": "补仓",
                "amount": amount,
                "buy_price": price,
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
            }
            self.position_log[stock] = {
                'is_position': False,
                'position': self.stocks_position[stock]['available'],
                'cost_price': self.stocks_position[stock]['cost_price'],
                'price': price,
                'profit': f"{profit:.2f}",
                'profit_rate': f"{(profit / (self.stocks_position[stock]['cost_price'] * self.stocks_position[stock]['available'])) * 100:.2f}",
            }
            # 直接删除持仓信息
            del self.stocks_position[stock]
            self.zy_list[stock] = {'price': self.close_price, 'time': self.current_date.strftime('%Y-%m-%d')}  # 记录止盈股票
        else:
            revenue = float(price * amount)  # 卖出金额
            profit = revenue - self.stocks_position[stock]['cost_price'] * amount  # 卖出盈利
            self.cash += decimal.Decimal(revenue)  # 更新现金
            self.stocks_position[stock]['available'] -= amount
            self.log_message(f"卖出 {stock} {amount} 股 @ {price:.2f}，获利 {profit:.2f}，剩余资金 {self.cash:.2f}")


    def backtrade(user_sql, region, zy_rate=1.2, zs_rate=0.8, ma_line='ma30', market_type=None):

        start_date = '2024-10-01'
        end_date = '2025-06-01'
        stock_list = filter_stocks_by_price_range(user_sql, min_price=12, max_price=25, min_market_cap=30, max_market_cap=180, region=region, not_market_type=market_type)
        if not stock_list:
            print(f"没有找到符合条件的股票，区域: {region}, 起始日期: {start_date}, 结束日期: {end_date}")
            return -100, -100

        # 随机打乱股票列表
        random.shuffle(stock_list)
        # stock_list = stock_list[:1000]
        
        # stock_list = ['002594.XSHE']
        
        
        # 创建IN查询的占位符
        placeholders = ', '.join(['%s'] * len(stock_list))
        where_clause = f'trade_date > "{start_date}" AND trade_date < "{end_date}" AND stock_code IN ({placeholders})'
        
        stocks_data = user_sql.select('stock_daily_k',
                        columns=['stock_code','trade_date','open','high','low','close','change_value','pct_change','ma5','ma10','ma20','ma30','ma45','ma60'],
                        where=where_clause, 
                        params=stock_list)
        
        # 准备数据
        df = pd.DataFrame(stocks_data)
        df = df[['stock_code', 'trade_date', 'open', 'high', 'low', 'close', 'change_value','pct_change','ma5','ma10','ma20','ma30','ma45','ma60']]
        
        # 使用方法1：运行回测并显示进度条（默认）
        mybt = StockBacktest(df, initial_capital=100000, stock_list=stock_list, show_progress=True, zy_rate=zy_rate, zs_rate=zs_rate, ma_line=ma_line)
        profit, profit_rate = mybt.run_backtest()
        return profit, profit_rate


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
                    self.low_price = float(stock_data['low'].values[0])
                    self.high_price = float(stock_data['high'].values[0])
                    self.check_position(stock)
                self.log_message(f'盘前整理完成')

            # 执行交易策略
            self._apply_strategy(current_data)
            
            # 更新可用持仓
            self.log_message("当日可用持仓:")
            market_cap = 0  # 持仓市值
            total_profit = 0  # 总盈利
            stock_num = 0  # 持仓股票数量
            self.day_profit = 0  # 当日盈利

            for stock in list(self.stocks_position.keys()):  # 使用列表副本遍历，避免在遍历过程中修改字典结构
                stock_data = current_data[current_data['stock_code'] == stock]
                if stock_data.empty:
                    continue
                    
                self.close_price = float(stock_data['close'].values[0])
                self.open_price = float(stock_data['open'].values[0])

                if self.stocks_position[stock]['unavailable'] > 0:  # 更新持仓 t+1
                    self.stocks_position[stock]['available'] += self.stocks_position[stock]['unavailable']
                    self.stocks_position[stock]['unavailable'] = 0

                elif self.stocks_position[stock]['available'] == 0 and self.stocks_position[stock]['unavailable'] == 0:  # 删除已清仓股票
                    # self.position_log[stock]['is_position'] = False
                    del self.stocks_position[stock]
                    continue
                stock_num += 1
                self.stocks_position[stock]['close_price'] = self.close_price
                available_amount = self.stocks_position[stock]['available']
                profit_rate = (self.close_price / self.stocks_position[stock]['cost_price'] - 1) * 100
                cost_price = self.stocks_position[stock]['cost_price']
                close_price = self.stocks_position[stock]['close_price']
                profit = (self.close_price - self.stocks_position[stock]['cost_price']) * available_amount  # 累计盈利
                day_profit = (self.close_price - self.open_price) * available_amount  # 当日盈利
                self.day_profit += day_profit



                market_cap += float(self.close_price * self.stocks_position[stock]['available'])

                self.log_message(f"{stock} 持仓: {available_amount}, 成本价：{cost_price:.2f}, 收盘价：{close_price:.2f}, 当日盈亏：{day_profit:.2f}, 累计盈亏：{profit:.2f}, 累计收益率: {profit_rate:.2f}%, 买入日期：{self.stocks_position[stock]['buy_date']}")
                self.position_log[stock] = {
                    'is_position': True,
                    'position': available_amount,
                    'cost_price': cost_price,
                    'price': close_price,
                    'profit': f"{profit:.2f}",
                    'profit_rate': f"{profit_rate:.2f}",
                }
            
            total_profit = market_cap + float(self.cash) - float(self.initial_capital)
            total_profit_rate = total_profit/self.initial_capital*100  # 总收益率
            total_assets = market_cap + float(self.cash)  # 总资产
            self.log_message(f"当日盈利：{self.day_profit:.2f}, 持仓总市值: {market_cap:.2f}，现金: {self.cash:.2f}，总资产: {total_assets:.2f}，总盈利: {total_profit:.2f}, 总收益率: {total_profit_rate:.2f}% ")
            self.profit = total_profit
            self.profit_rate = total_profit_rate
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
                self.log_message("资金不足5000，暂停交易，保留流动资金")
                return
            if len(self.stocks_position) > self.max_stock_num and self.cash < 8000:
                self.log_message(f"股票数量超过{self.max_stock_num}，暂停交易，等待股票数量减少")
                return
            
            stock_data = current_data[current_data['stock_code'] == stock]
            if stock_data.empty:
                continue
                
            self.open_price = float(stock_data['open'].values[0])
            self.close_price = float(stock_data['close'].values[0])
            self.low_price = float(stock_data['low'].values[0])
            self.high_price = float(stock_data['high'].values[0])
            self.ma = float(stock_data[self.settings['ma_line']].values[0])
            self.zy_ma_line = float(stock_data[self.settings['zy_ma_line']].values[0])

            self.strategy(stock)          


    def check_position(self, stock):
        """检查持仓情况"""
        if stock in self.stocks_position:
            if (self.current_date - self.stocks_position[stock]['buy_date']).days > 45 and self.open_price/self.stocks_position[stock]['cost_price'] > 1.1:
                self.sell(stock, self.high_price, -1)
                return
            elif (self.current_date - self.stocks_position[stock]['buy_date']).days > 60 and self.open_price/self.stocks_position[stock]['cost_price'] > 0.0:
                self.sell(stock, self.high_price, -1)
                return 

        if self.open_price == self.close_price == self.low_price == self.high_price:  # 如果开盘价等于收盘价等于最高价等于最低价，则不进行操作
            return
        if self.open_price/self.stocks_position[stock]['cost_price'] > self.zy_rate:  # 止盈
            self.sell(stock, self.open_price, -1)
        elif self.high_price/self.stocks_position[stock]['cost_price'] > self.zy_rate:  # 止盈
            self.sell(stock, self.high_price, -1)

        elif self.open_price/self.stocks_position[stock]['cost_price'] < self.zs_rate :  # 亏损20%补仓
            # if self.stocks_position[stock]['available'] < 200:  # 如果有可用持仓，则补仓
            self.buy(stock, self.open_price, 100, additional=True)
        elif self.low_price/self.stocks_position[stock]['cost_price'] < self.zs_rate:  # 止损
            self.buy(stock, self.open_price, 100, additional=True)



    def strategy(self,stock):
        """
        盘中买入策略
        """
        if stock not in self.stocks_position and stock not in self.zy_list:
            if self.settings['min_price'] <= self.open_price <= self.settings['max_price'] and self.open_price <= self.ma:
            # if self.open_price <= self.ma:
                self.stocks_position[stock] = {'available': 0, 'unavailable': 0, 'cost_price': 0.0, 'close_price': 0.0, 'buy_date': self.current_date}  #, 'sell_amount': 0}
                self.buy(stock, self.open_price, 100)  # 建仓
            elif self.settings['min_price'] <= self.low_price <= self.settings['max_price'] and self.low_price <= self.ma:
                self.stocks_position[stock] = {'available': 0, 'unavailable': 0, 'cost_price': 0.0, 'close_price': 0.0, 'buy_date': self.current_date}  #, 'sell_amount': 0}
                self.buy(stock, self.low_price+0.05, 100)  # 建仓
            
            # elif self.settings['min_price']
            # if 5<= self.low_price <= 15 and self.open_price <= self.ma:  # 如果开盘价小于30日均线，则买入
            #     self.stocks_position[stock] = {'available': 0, 'unavailable': 0, 'cost_price': 0.0, 'close_price': 0.0}  #, 'sell_amount': 0}
            #     self.buy(stock, self.low_price, 100)  # 建仓
        elif stock in self.zy_list:
            if self.zy_ma_line >= self.zy_list[stock]['price']:
                self.stocks_position[stock] = {'available': 0, 'unavailable': 0, 'cost_price': 0.0, 'close_price': 0.0}  #, 'sell_amount': 0}
                self.buy(stock, self.open_price, 100)  # 建仓
                del self.zy_list[stock]  # 删除止盈股票记录

                
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

        return self.profit, self.profit_rate


    def close_log(self):
        """关闭日志文件"""

        for stock in self.position_log:
            self.log.write(f"{stock} 是否持仓: {self.position_log[stock]['is_position']}, 持仓数：{self.position_log[stock]['position']}, 成本价：{self.position_log[stock]['cost_price']:.2f}, ")
            self.log.write(f"现价：{self.position_log[stock]['price']:.2f}, 盈亏：{float(self.position_log[stock]['profit']):.2f}, 收益率：{self.position_log[stock]['profit_rate']}%\n")
        self.log.write("===========================================\n")
        for stock in self.zy_list:
            self.log.write(f"{stock} 止盈价: {self.zy_list[stock]['price']:.2f}, 止盈时间: {self.zy_list[stock]['time']}\n")
        self.log.write("===========================================\n")
        self.log.write("回测结束\n")
        self.log.close()

        # 将字典转为DataFrame，并将外层键作为一列
        df = pd.DataFrame.from_dict(self.result, orient='index').reset_index()
        df.columns = ['trade_date', 'total_profit_rate', 'total_value', 'cash', 'market_cap', 'index_total_profit_rate', 'trade_log']

        df.to_csv("output.csv", index=False, encoding='utf-8')

        df1 = pd.DataFrame.from_dict(self.position_log, orient='index').reset_index()
        df1.columns = ['stock_code','is_position', 'position', 'cost_price', 'price', 'profit', 'profit_rate']
        df1.to_csv("position_log.csv", index=False, encoding='utf-8')


def run_all_params():
    # 从数据库获取数据
    user_sql_config = {
        'host': 'localhost',
        'user': 'afei',
        'password': 'sf123456',
        'database': 'stock',
        'port': 3306
    }
    user_sql = PySQL(**user_sql_config)
    user_sql.connect()
    region = user_sql.select('stock_info', columns=['region'])  # 获取所有非ST股票
    region = [item['region'] for item in region]
    region = list(set(region))  # 去重
    # user_sql.close()

    zy_rate = [1.1, 1.15, 1.2, 1.25, 1.3, 1.35, 1.4]  # 止盈率列表
    zs_rate = [0.9, 0.85, 0.8, 0.75, 0.7, 0.65, 0.6]  # 止损率列表
    ma_line = ['ma5', 'ma10', 'ma20', 'ma30', 'ma45', 'ma60']  # 均线列表

    result_file = 'backtest_results2.txt'
    # # finished_tasks = parse_finished_tasks(result_file)

    # 创建日志文件
    with open(result_file, 'w', encoding='utf-8') as f:
        f.write("回测开始\n")
        f.write("===========================================\n")
        f.close()

    # 组合所有参数
    tasks = []
    ma = 'ma30'  # 默认均线
    for r in region:
        profit,profit_rate = backtrade(user_sql, region=r, zy_rate=1.2, zs_rate=0.8, ma_line=ma)
        # 追加写入结果
        with open(result_file, 'a', encoding='utf-8') as f:
                
                f.write(f"板块: {r}, 止盈率: {1.2}, 止损率: {0.8}, 均线: {ma}, 盈利: {profit:.2f}, 收益率: {profit_rate:.2f}%\n")
    
    # 关闭日志文件
    with open(result_file, 'a', encoding='utf-8') as f:
        f.write("回测结束\n")
        f.write("===========================================\n")
        f.close()


if __name__ == '__main__':
    my_bt = StockBacktest()
    profit, profit_rate = my_bt.run_backtest()
    print(f"回测结果: 盈利: {profit:.2f}, 收益率: {profit_rate:.2f}%")

    from backtest_report_generator import main
    main()



