import pandas as pd
from datetime import datetime, timedelta
import decimal
from pysql import PySQL
import os
from chart_generator import ChartGenerator
from html_generator import HTMLGenerator


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
        self.history = []
        
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
        self.history.append(('BUY', self.current_date, stock, price, amount))
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
        self.history.append(('SELL', self.current_date, stock, price, amount))
        return True

    def _get_index_data(self):
        """获取指数数据"""
        try:
            user_sql = PySQL(
                host='192.168.1.122',
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
        index_return = 0
        index_profit_rate = 0
        
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
                
                self.log_message(f"指数{self.index_code}当天收益率: {index_return:.2f}%, 当日涨跌幅{pct_change_index:.2f}%, 持仓收益率: {index_profit_rate:.2f}%")
                
                # 计算超额收益率
                excess_return = returns - index_profit_rate
                self.log_message(f"策略超额收益: {excess_return:.2f}%")
        except Exception as e:
            self.log_message(f"计算指数收益率时出错: {e}")
        
        # 记录总体信息
        cumulative_profit = total_value - self.initial_capital
        self.log_message(f"当日总结: 总市值 {market_cap:.2f}，现金 {self.cash:.2f}，总资产 {total_value:.2f}，当日盈亏 {total_profit:.2f}，累计盈亏 {cumulative_profit:.2f}，总收益率 {returns:.2f}%")
        
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
        重写策略
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

    def get_history(self):
        """获取交易历史记录"""
        return self.history

    def close_log(self):
        """关闭日志文件"""
        self.log.write("===========================================\n")
        self.log.write("回测结束\n")
        self.log.close()


class BacktestVisualizer:
    def __init__(self, log_file='backtest_log.txt', port=8080):
        """
        初始化回测可视化器
        :param log_file: 日志文件路径
        :param port: 服务器端口
        """
        self.log_file = log_file
        self.port = port
        self.data = {
            'dates': [],
            'strategy_returns': [],
            'index_returns': [],
            'excess_returns': [],
            'trades': [],
            'cash': [],           # 现金
            'market_value': [],   # 市值
            'total_assets': [],   # 总资产
            'daily_profit': [],   # 新增：当日盈亏
            'cumulative_profit': [] # 新增：累计盈亏
        }
        
    def parse_log_file(self):
        """解析回测日志文件"""
        import re
        
        date_pattern = re.compile(r'\[(\d{4}-\d{2}-\d{2})\]')
        strategy_return_pattern = re.compile(r'总收益率 ([-\d.]+)%')
        # 修改指数收益率和指数信息的正则表达式
        index_day_return_pattern = re.compile(r'指数.*?当天收益率: ([-\d.]+)%')
        index_profit_pattern = re.compile(r'指数.*?持仓收益率: ([-\d.]+)%')
        excess_return_pattern = re.compile(r'策略超额收益: ([-\d.]+)%')
        trade_pattern = re.compile(r'\[(\d{4}-\d{2}-\d{2})\] (买入|卖出) ([\w.]+) (\d+) 股 @ ([\d.]+)')
        daily_summary_pattern = re.compile(
            r'当日总结: 总市值 ([\d.]+)，现金 ([\d.]+)，总资产 ([\d.]+)，当日盈亏 ([-\d.]+)，累计盈亏 ([-\d.]+)，总收益率 ([-\d.]+)%'
        )
        
        current_date = None
        
        try:
            with open(self.log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                
                # 预处理：将多行合并为一个日期的完整记录
                date_blocks = []
                current_block = []
                
                for line in lines:
                    if line.strip() == "":
                        if current_block:
                            date_blocks.append("".join(current_block))
                            current_block = []
                    elif line.startswith("====="):
                        continue
                    elif line.startswith("回测日志"):
                        continue
                    else:
                        current_block.append(line)
                
                # 添加最后一个块
                if current_block:
                    date_blocks.append("".join(current_block))
                
                # 处理每个日期块
                for block in date_blocks:
                    # 提取日期
                    date_match = date_pattern.search(block)
                    if date_match:
                        current_date = date_match.group(1)
                    else:
                        continue
                    
                    # 提取日终总结数据
                    summary_match = daily_summary_pattern.search(block)
                    if summary_match:
                        market_value = float(summary_match.group(1))
                        cash = float(summary_match.group(2))
                        total_assets = float(summary_match.group(3))
                        daily_profit = float(summary_match.group(4))
                        cumulative_profit = float(summary_match.group(5))
                        strategy_return = float(summary_match.group(6))
                        
                        # 添加到数据集
                        self.data['dates'].append(current_date)
                        self.data['strategy_returns'].append(strategy_return)
                        self.data['market_value'].append(market_value)
                        self.data['cash'].append(cash)
                        self.data['total_assets'].append(total_assets)
                        self.data['daily_profit'].append(daily_profit)
                        self.data['cumulative_profit'].append(cumulative_profit)
                        
                        # 默认值，如果没有找到指数数据
                        index_return = 0.0
                        excess_return = 0.0
                        
                        # 尝试提取指数持仓收益率
                        index_profit_match = index_profit_pattern.search(block)
                        if index_profit_match:
                            index_return = float(index_profit_match.group(1))
                            
                        # 尝试提取超额收益
                        excess_match = excess_return_pattern.search(block)
                        if excess_match:
                            excess_return = float(excess_match.group(1))
                        else:
                            # 如果没有直接的超额收益数据，计算差值
                            excess_return = strategy_return - index_return
                        
                        self.data['index_returns'].append(index_return)
                        self.data['excess_returns'].append(excess_return)
                    
                    # 提取交易记录
                    for trade_match in trade_pattern.finditer(block):
                        trade_date = trade_match.group(1)
                        action = trade_match.group(2)
                        stock = trade_match.group(3)
                        amount = int(trade_match.group(4))
                        price = float(trade_match.group(5))
                        
                        self.data['trades'].append({
                            'date': trade_date,
                            'action': action,
                            'stock': stock,
                            'amount': amount,
                            'price': price
                        })
            
            print(f"解析完成，获取了 {len(self.data['dates'])} 天的数据")
            
            # 确保数据长度一致
            min_length = min(len(self.data['dates']), len(self.data['strategy_returns']))
            self.data['dates'] = self.data['dates'][:min_length]
            self.data['strategy_returns'] = self.data['strategy_returns'][:min_length]
            self.data['index_returns'] = self.data['index_returns'][:min_length] if len(self.data['index_returns']) >= min_length else self.data['index_returns'] + [0] * (min_length - len(self.data['index_returns']))
            self.data['excess_returns'] = self.data['excess_returns'][:min_length] if len(self.data['excess_returns']) >= min_length else self.data['excess_returns'] + [0] * (min_length - len(self.data['excess_returns']))
            
            return True
        except Exception as e:
            print(f"解析日志文件时出错: {e}")
            return False
    
    def generate_html(self, output_file='backtest_report.html'):
        """生成HTML报表"""
        if not self.data['dates']:
            print("没有数据可以生成报表")
            return False
        
        print("开始生成HTML报表...")
        
        # 使用ChartGenerator处理数据并生成图表
        df = ChartGenerator.prepare_data(self.data)
        
        # 创建各种图表
        print("生成各种图表...")
        fig1 = ChartGenerator.create_cumulative_return_chart(df)
        fig2 = ChartGenerator.create_daily_return_chart(df)
        fig3 = ChartGenerator.create_asset_chart(df)
        fig4 = ChartGenerator.create_drawdown_chart(df)
        
        # 生成图表HTML
        chart_html = ChartGenerator.generate_chart_html(fig1, fig2, fig3, fig4)
        
        # 计算性能指标
        metrics = ChartGenerator.calculate_performance_metrics(df)
        
        # 使用HTMLGenerator生成完整的HTML报表
        print("生成最终HTML报表... [99%]")
        with open(output_file, 'w', encoding='utf-8') as f:
            html = HTMLGenerator.generate_complete_html(chart_html, metrics, self.data['trades'])
            f.write(html)
            
        print("HTML报表已生成: 100% 完成!")
        return True
    
    def start_server(self, html_file='backtest_report.html'):
        """启动HTTP服务器"""
        import http.server
        import socketserver
        import webbrowser
        import os
        
        if not os.path.exists(html_file):
            print(f"文件 {html_file} 不存在，请先生成HTML报表")
            return False
        
        # 创建HTTP服务器
        handler = http.server.SimpleHTTPRequestHandler
        
        try:
            with socketserver.TCPServer(("", self.port), handler) as httpd:
                print(f"服务器已启动在 http://localhost:{self.port}/")
                print(f"请在浏览器中访问 http://localhost:{self.port}/{html_file}")
                print("按Ctrl+C停止服务器")
                
                # 自动打开浏览器
                webbrowser.open(f"http://localhost:{self.port}/{html_file}")
                
                # 启动服务器
                httpd.serve_forever()
        except KeyboardInterrupt:
            print("服务器已停止")
        except Exception as e:
            print(f"启动服务器时出错: {e}")
            return False
        
        return True
    
    def visualize(self, html_file='backtest_report.html'):
        """解析日志、生成HTML并启动服务器"""
        if self.parse_log_file():
            if self.generate_html(html_file):
                print(f"HTML报表已成功生成: {os.path.abspath(html_file)}")
                self.start_server(html_file)
            else:
                print("HTML报表生成失败")
        else:
            print("日志文件解析失败")


if __name__ == '__main__':
    # 从数据库获取数据
    user_sql = PySQL(
        host='192.168.1.122',
        user='afei',
        password='sf123456',
        database='stock',
        port=3306
    )
    user_sql.connect()
    stock_list = ['002594.XSHE']
    
    # 创建IN查询的占位符
    placeholders = ', '.join(['%s'] * len(stock_list))
    where_clause = f'trade_date > "2024-09-30" AND trade_date < "2025-05-20" AND stock_code IN ({placeholders})'
    
    stocks_data = user_sql.select('stock_daily_k',
                    columns=['stock_code','trade_date','open','high','low','close','change_value','pct_change'],
                    where=where_clause, 
                    params=stock_list)
    
    # 准备数据
    df = pd.DataFrame(stocks_data)
    df = df[['stock_code', 'trade_date', 'open', 'high', 'low', 'close', 'change_value','pct_change']]
    
    # 使用方法1：运行回测并可视化
    mybt = StockBacktest(df, initial_capital=40000, stock_list=stock_list, start_time='2024-10-08', end_time='2025-05-20')
    mybt.run_backtest()
    
    # 使用可视化器显示结果
    visualizer = BacktestVisualizer(log_file='backtest_log.txt', port=8080)
    visualizer.visualize()
    
    # 使用方法2：仅可视化已有的日志文件
    # visualizer = BacktestVisualizer(log_file='backtest_log.txt', port=8080)
    # visualizer.visualize()

    # 检查日志文件是否生成
    print(f"日志文件存在: {os.path.exists('backtest_log.txt')}")
    print(f"HTML报表存在: {os.path.exists('backtest_report.html')}")