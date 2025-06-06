import pandas as pd
from datetime import datetime, timedelta
import decimal
from pysql import PySQL
from tqdm import tqdm  # 导入tqdm库
from filter_stocks import filter_stocks_by_price_range
import concurrent.futures
import os


class StockBacktest:
    def __init__(self, data: pd.DataFrame, initial_capital: float = 100000, log_file: str = 'backtest_log.txt',
                 start_time: str = None, end_time: str = None, stock_list: list = None, index_code: str = '000300.SH',
                 show_progress: bool = True, zy_rate: float = 1.2, zs_rate: float = 0.8, ma_line: str = 'ma30'):
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

        self.profit = 0.0  # 总盈利
        self.profit_rate = 0.0  # 总收益率

        # 控制变量
        self.zy_rate = zy_rate
        self.zs_rate = zs_rate
        self.ma_line = ma_line

        # 记录持仓
        self.position_log = {}  # 'stock_code': {'is_position': True, 'position': 100, 'cost_price': 10.0,'price': 0.0, 'profit': 0.0}

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
        # 关闭日志系统，不再记录持仓和买卖
        self.log = None
        # 原代码注释掉
        # self.log = open(self.log_file_name, 'w', encoding='utf-8')
        # self.log.write(f"回测日志 - 初始资本: {self.initial_capital}\n")
        # self.log.write("===========================================\n")

    def log_message(self, message: str):
        """记录日志消息"""
        # 关闭日志系统，不再记录消息
        # if self.log:
            # log_entry = f"[{datetime.strftime(self.current_date, '%Y-%m-%d')}] {message}"
            # self.log.write(log_entry + "\n")
        # print(log_entry)
        pass
    
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
                'profit': f"{profit:.2f}"
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
                    self.ma5 = float(stock_data['ma5'].values[0])
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



                market_cap += float(self.close_price * self.stocks_position[stock]['available'])

                self.log_message(f"{stock} 持仓: {available_amount}, 成本价：{cost_price:.2f}, 收盘价：{close_price:.2f}, 当日盈亏：{day_profit:.2f}, 累计盈亏：{profit:.2f}, 累计收益率: {profit_rate:.2f}%")
                self.position_log[stock] = {
                    'is_position': True,
                    'position': available_amount,
                    'cost_price': cost_price,
                    'price': close_price,
                    'profit': f"{profit:.2f}"
                }
            
            total_profit = market_cap + float(self.cash) - float(self.initial_capital)
            total_profit_rate = total_profit/self.initial_capital*100  # 总收益率
            total_assets = market_cap + float(self.cash)  # 总资产
            self.log_message(f"当日持仓总市值: {market_cap:.2f}，现金: {self.cash:.2f}，总资产: {total_assets:.2f}，总盈利: {total_profit:.2f}, 总收益率: {total_profit_rate:.2f}% ")
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
            self.low_price = float(stock_data['low'].values[0])
            self.high_price = float(stock_data['high'].values[0])
            self.ma = float(stock_data[self.ma_line].values[0])

            self.strategy(stock)          
    
    def check_position(self, stock):
        """检查持仓情况"""
        # if self.open_price/self.stocks_position[stock]['cost_price'] > self.zy_rate:  # 盈利15%卖出
        #     self.sell(stock, self.open_price, -1)
        if self.open_price < self.ma5:  # 如果开盘价小于5日均线，则卖出
            self.sell(stock, self.open_price, -1)
        
        elif self.open_price/self.stocks_position[stock]['cost_price'] < self.zs_rate :  # 亏损20%补仓
            self.buy(stock, self.open_price, 100, additional=True)
        
    def strategy(self,stock):
        """
        盘中买入策略
        """
        if stock not in self.stocks_position and stock not in self.zy_list:
            if self.open_price <= self.ma or self.low_price <= self.ma:  # 如果开盘价小于30日均线，则买入
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

        return self.profit, self.profit_rate

    def close_log(self):
        """关闭日志文件"""
        # 不再写入日志文件，但保留数据处理部分
        
        # 将字典转为DataFrame，并将外层键作为一列
        df = pd.DataFrame.from_dict(self.result, orient='index').reset_index()
        df.columns = ['trade_date', 'total_profit_rate', 'total_value', 'cash', 'market_cap', 'index_total_profit_rate', 'trade_log']

        df.to_csv("output.csv", index=False, encoding='utf-8')

        df1 = pd.DataFrame.from_dict(self.position_log, orient='index').reset_index()
        df1.columns = ['stock_code','is_position', 'position', 'cost_price', 'price', 'profit']
        df1.to_csv("position_log.csv", index=False, encoding='utf-8')

def backtrade(user_sql, region, zy_rate=1.2, zs_rate=0.8, ma_line='ma30'):

    start_date = '2025-01-01'
    end_date = '2025-06-01'
    result = filter_stocks_by_price_range(start_date, min_price=3, max_price=15, min_market_cap=30, max_market_cap=180, region=region)
    stock_list = [item['stock_code'] for item in result]

    # 随机打乱股票列表
    import random
    random.seed(666)  # 设置随机种子以确保可重复性
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
    mybt = StockBacktest(df, initial_capital=100000, stock_list=stock_list, show_progress=False, zy_rate=zy_rate, zs_rate=zs_rate, ma_line=ma_line)
    profit, profit_rate = mybt.run_backtest()
    return profit, profit_rate


def run_backtest_task(args):
    user_sql_config, r, zyr, zsr, ma = args
    # 每个进程内新建数据库连接，避免多进程共享同一个连接
    user_sql = PySQL(**user_sql_config)
    user_sql.connect()
    try:
        profit, profit_rate = backtrade(user_sql, region=r, zy_rate=zyr, zs_rate=zsr, ma_line=ma,)
        result = (r, zyr, zsr, ma, profit, profit_rate)
    except Exception as e:
        print(f"回测任务出错: {e}")
        result = (r, zyr, zsr, ma, 0, 0)  # 出错时返回零收益
    finally:
        user_sql.close()
    return result




if __name__ == '__main__':
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
    user_sql.close()

    zy_rate = [1.1, 1.15, 1.2, 1.25, 1.3, 1.35, 1.4]  # 止盈率列表
    zs_rate = [0.9, 0.85, 0.8, 0.75, 0.7, 0.65, 0.6]  # 止损率列表
    ma_line = ['ma5', 'ma10', 'ma20', 'ma30', 'ma45', 'ma60']  # 均线列表

    result_file = 'backtest_results2.txt'

    # 创建日志文件
    with open(result_file, 'w', encoding='utf-8') as f:
        f.write("回测开始\n")
        f.write("===========================================\n")

    # 组合所有参数
    tasks = []
    for r in region:
        for zyr in zy_rate:
            for zsr in zs_rate:
                for ma in ma_line:
                    tasks.append((user_sql_config, r, zyr, zsr, ma))

    print(f"剩余未完成任务数: {len(tasks)}")
    if not tasks:
        print("所有参数组合已完成，无需重复回测。")
        # 新增：即使没有新任务也写入一行提示到结果文件
        with open(result_file, 'a', encoding='utf-8') as f:
            f.write("所有参数组合已完成，无需重复回测。\n")
    else:
        # 并发执行
        # results = []
        # with concurrent.futures.ProcessPoolExecutor() as executor:
        #     # 使用list()强制等待所有任务完成
        #     future_to_task = {executor.submit(run_backtest_task, task): task for task in tasks}
        #     for future in concurrent.futures.as_completed(future_to_task):
        #         try:
        #             result = future.result()
        #             results.append(result)
        #             # 每完成一个任务就立即写入结果文件，避免全部完成后一次性写入
        #             with open(result_file, 'a', encoding='utf-8') as f:
        #                 r, zyr, zsr, ma, profit, profit_rate = result
        #                 f.write(f"板块: {r}, 止盈率: {zyr}, 止损率: {zsr}, 均线: {ma}, 盈利: {profit:.2f}, 收益率: {profit_rate:.2f}%\n")
        #         except Exception as exc:
        #             task = future_to_task[future]
        #             print(f'任务 {task} 生成了异常: {exc}')
        #             # 记录失败的任务
        #             with open(result_file, 'a', encoding='utf-8') as f:
        #                 f.write(f"任务失败: {task}, 错误: {exc}\n")

        import os
        import math
        import concurrent.futures

        # 计算可用的核心数（70% 四舍五入取整）
        total_cores = os.cpu_count() or 1  # 避免 None 情况，默认为 1
        max_workers = max(1, math.floor(total_cores * 0.7))  # 至少使用 1 个核心
        print(f"使用 {max_workers} 个进程进行并发回测")

        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:  # <-- 关键修改
            results = []
            future_to_task = {executor.submit(run_backtest_task, task): task for task in tasks}
            
            for future in concurrent.futures.as_completed(future_to_task):
                try:
                    result = future.result()
                    results.append(result)
                    with open(result_file, 'a', encoding='utf-8') as f:
                        r, zyr, zsr, ma, profit, profit_rate = result
                        f.write(f"板块: {r}, 止盈率: {zyr}, 止损率: {zsr}, 均线: {ma}, 盈利: {profit:.2f}, 收益率: {profit_rate:.2f}%\n")
                except Exception as exc:
                    task = future_to_task[future]
                    print(f'任务 {task} 生成了异常: {exc}')
                    with open(result_file, 'a', encoding='utf-8') as f:
                        f.write(f"任务失败: {task}, 错误: {exc}\n")
    
    # 关闭日志文件
    with open(result_file, 'a', encoding='utf-8') as f:
        f.write("回测结束\n")
        f.write("===========================================\n")
