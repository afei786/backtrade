import pandas as pd
from datetime import datetime, timedelta
import decimal
from pysql import PySQL
from tqdm import tqdm
import random

class StockBacktest:
    def __init__(self, data: pd.DataFrame, initial_capital: float = 100000, log_file: str = 'backtest_log.txt',
                 start_time: str = None, end_time: str = None, stock_list: list = None, index_code: str = '000300.SH',
                 show_progress: bool = True, max_stock_num: int = 10):
        """
        初始化回测类
        :param max_stock_num: 最大持股数量
        """
        # 数据预处理
        self.data = data.copy()
        self.data['trade_date'] = pd.to_datetime(self.data['trade_date'])
        
        # 初始化资金和统计信息
        self.initial_capital = initial_capital
        self.cash = decimal.Decimal(initial_capital)
        self.balance = decimal.Decimal(initial_capital)
        self.result = {}
        self.max_stock_num = max_stock_num  # 最大持仓股票数量
        self.show_progress = show_progress
        self.blacklist = {}  # 黑名单字典 {股票代码: 移出日期}

        # 设置回测时间范围
        self.start_time = pd.to_datetime(start_time) if start_time else self.data['trade_date'].min()
        self.end_time = pd.to_datetime(end_time) if end_time else self.data['trade_date'].max()
        self.current_date = self.start_time
        
        # 过滤数据在时间范围内的部分
        self.data = self.data[(self.data['trade_date'] >= self.start_time) & 
                             (self.data['trade_date'] <= self.end_time)].reset_index(drop=True)
        
        # 设置股票列表和初始化持仓
        self.stock_list = stock_list
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
    
    def _init_log(self):
        """初始化日志文件"""
        self.log = open(self.log_file_name, 'w', encoding='utf-8')
        self.log.write(f"回测日志 - 初始资本: {self.initial_capital}\n")
        self.log.write(f"最大持股数量: {self.max_stock_num}\n")
        self.log.write("===========================================\n")

    def log_message(self, message: str):
        """记录日志消息"""
        log_entry = f"[{datetime.strftime(self.current_date, '%Y-%m-%d')}] {message}"
        self.log.write(log_entry + "\n")

    def buy(self, stock: str, price: float, amount: int):
        """买入操作"""
        # 检查是否在股票列表中且不在黑名单中
        if stock not in self.stock_list or self._is_in_blacklist(stock):
            return False
            
        cost = price * amount
        if cost > self.cash:
            self.log_message(f"资金不足，无法买入 {stock} {amount} 股 @ {price:.2f}")
            return False
            
        self.cash -= decimal.Decimal(cost)
        self.stocks_position[stock]['unavailable'] += amount
        
        # 计算成本价
        if self.stocks_position[stock]['cost_price'] == 0:
            self.stocks_position[stock]['cost_price'] = float(price)
        else:
            current_position = self.stocks_position[stock]['available'] + self.stocks_position[stock]['unavailable']
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
        
        # 将止盈的股票加入黑名单
        if profit > 0:
            self.add_to_blacklist(stock)
            
        return True

    def add_to_blacklist(self, stock: str):
        """将股票加入黑名单15个交易日"""
        self.blacklist[stock] = self.current_date
        if stock in self.stock_list:
            self.stock_list.remove(stock)
            self.log_message(f"将止盈股票 {stock} 移出股票列表并加入黑名单15个交易日")

    def remove_from_blacklist(self):
        """从黑名单中移除过期的股票"""
        stocks_to_remove = []
        for stock, remove_date in self.blacklist.items():
            if (self.current_date - remove_date).days >= 15:
                stocks_to_remove.append(stock)
        
        for stock in stocks_to_remove:
            if stock not in self.stock_list:
                self.stock_list.append(stock)
            del self.blacklist[stock]
            self.log_message(f"从黑名单中移除股票 {stock}，已满15个交易日")

    def _is_in_blacklist(self, stock: str) -> bool:
        """检查股票是否在黑名单中"""
        if stock in self.blacklist:
            # 如果超过15个交易日则自动移出
            if (self.current_date - self.blacklist[stock]).days >= 15:
                del self.blacklist[stock]
                return False
            return True
        return False

    # ... (其他方法如_get_index_data, calculate_returns保持不变)

    def next(self):
        """执行下一个交易日的回测"""
        # 移出黑名单中过期的股票
        self.remove_from_blacklist()
        
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
        for stock in list(self.stocks_position.keys()):
            if stock in self.stocks_position:
                if self.stocks_position[stock]['unavailable'] > 0:
                    self.stocks_position[stock]['available'] += self.stocks_position[stock]['unavailable']
                    self.stocks_position[stock]['unavailable'] = 0
                    
                # 清理空仓股票
                if (self.stocks_position[stock]['available'] == 0 and 
                    self.stocks_position[stock]['unavailable'] == 0 and
                    self.stocks_position[stock]['cost_price'] == 0):
                    del self.stocks_position[stock]
    
    def _get_holding_stock_count(self) -> int:
        """获取当前持有股票数量"""
        count = 0
        for stock, pos in self.stocks_position.items():
            if pos['available'] > 0 or pos['unavailable'] > 0:
                count += 1
        return count

    def _apply_strategy(self, current_data):
        """应用交易策略"""      
        # 跳过不交易的股票
        available_stocks = [stock for stock in self.stock_list 
                            if not self._is_in_blacklist(stock) 
                            and stock in self.stocks_position
                            and self.stocks_position[stock].get('available', 0) > 0]
        
        # 计算当前持仓股票数量
        holding_count = self._get_holding_stock_count()
        
        # 处理已有持仓
        for stock in available_stocks:
            stock_data = current_data[current_data['stock_code'] == stock]
            if stock_data.empty:
                continue
                
            self.open_price = stock_data['open'].values[0]
            self.close_price = stock_data['close'].values[0]

            # 计算盈亏比例
            cost_price = self.stocks_position[stock]['cost_price']
            if cost_price > 0:
                profit_ratio = (self.open_price / cost_price - 1) * 100
                
                # 止盈策略：盈利超过15%
                if profit_ratio >= 15:
                    self.sell(stock, self.open_price, self.stocks_position[stock]['available'])
                    
                # 补仓策略：亏损20%
                elif profit_ratio <= -20:
                    self.buy(stock, self.open_price, 100)
        
        # 新增买入（当资金充足且未达到最大持仓数量时）
        if self.cash >= 5000 and holding_count < self.max_stock_num:
            # 从可用股票中随机选择一个
            buy_candidates = [stock for stock in self.stock_list 
                              if not self._is_in_blacklist(stock) 
                              and stock in current_data['stock_code'].values
                              and stock not in self.stocks_position]
            
            if buy_candidates:
                stock = random.choice(buy_candidates)
                stock_data = current_data[current_data['stock_code'] == stock]
                if not stock_data.empty:
                    open_price = stock_data['open'].values[0]
                    
                    # 计算可买入数量
                    max_buy_amount = min(
                        int(self.cash // decimal.Decimal(open_price)),
                        100  # 每次买入100股
                    )
                    
                    if max_buy_amount > 0:
                        self.buy(stock, open_price, max_buy_amount)
                        holding_count += 1
        
        # 结束日期卖出所有持仓
        if self.current_date == self.end_time:
            for stock, position in self.stocks_position.items():
                available_shares = position['available']
                if available_shares > 0:
                    self.sell(stock, self.close_price, available_shares)

    
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
        df.columns = ['trade_date', 'total_profit_rate', 'total_value', 'cash', 'market_cap', 'index_total_profit_rate']

        df.to_csv("output.csv", index=False, encoding='utf-8')
    
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
    # stock_list = ['002594.XSHE','603881.XSHG']
    stock_list = user_sql.select(
        'stock_info',
        columns=['stock_code'],
        where='market_cap > 10 AND market_cap < 100 AND is_st = 0'
    )
    print(f"获取到 {len(stock_list)} 只股票")
    stock_list = [item['stock_code'] for item in stock_list]

    # 随机打乱股票列表
    import random
    random.shuffle(stock_list)
    # stock_list = stock_list[:100]
    
    # stock_list = ['002594.XSHE','603881.XSHG']
    
    
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
    
    # 使用方法1：运行回测并显示进度条（默认）
    mybt = StockBacktest(df, initial_capital=100000, stock_list=stock_list, show_progress=True)
    mybt.run_backtest()
    