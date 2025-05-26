import pandas as pd
from datetime import datetime, timedelta
import decimal
from pysql import PySQL
import os


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
                open = self.index_data.loc[self.current_date, 'open']
                close = self.index_data.loc[self.current_date, 'close']
                pct_change = self.index_data.loc[self.current_date, 'pct_change']
                profit_rate = (close/cost_index - 1) * 100
                self.log_message(f"{cost_index, open, close, pct_change, profit_rate}")
                self.log_message(f"指数{self.index_code}当天收益率: {(close/open - 1) * 100:.2f}%, 当日涨跌幅{pct_change:.2f}%, 持仓收益率: {profit_rate:.2f}%")
                self.log_message(f"策略超额收益: {returns - profit_rate:.2f}%")
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
        index_return_pattern = re.compile(r'指数.*?同期收益率: ([-\d.]+)%')
        excess_return_pattern = re.compile(r'策略超额收益: ([-\d.]+)%')
        trade_pattern = re.compile(r'\[(\d{4}-\d{2}-\d{2})\] (买入|卖出) ([\w.]+) (\d+) 股 @ ([\d.]+)')
        daily_summary_pattern = re.compile(
            r'当日总结: 总市值 ([\d.]+)，现金 ([\d.]+)，总资产 ([\d.]+)，当日盈亏 ([\d.\-]+)，累计盈亏 ([\d.\-]+)，总收益率 ([-\d.]+)%'
        )
        
        current_date = None
        
        try:
            with open(self.log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                
                for line in lines:
                    # 提取日期
                    date_match = date_pattern.search(line)
                    if date_match:
                        current_date = date_match.group(1)
                    
                    # 提取策略收益率
                    if '总收益率' in line and current_date:
                        return_match = strategy_return_pattern.search(line)
                        if return_match:
                            self.data['dates'].append(current_date)
                            self.data['strategy_returns'].append(float(return_match.group(1)))
                        
                        # 提取每日资金情况（含盈亏）
                        summary_match = daily_summary_pattern.search(line)
                        if summary_match:
                            market_value = float(summary_match.group(1))
                            cash = float(summary_match.group(2))
                            total_assets = float(summary_match.group(3))
                            daily_profit = float(summary_match.group(4))
                            cumulative_profit = float(summary_match.group(5))
                            # 追加到对应字段
                            self.data['market_value'].append(market_value)
                            self.data['cash'].append(cash)
                            self.data['total_assets'].append(total_assets)
                            self.data['daily_profit'].append(daily_profit)
                            self.data['cumulative_profit'].append(cumulative_profit)
                    
                    # 提取指数收益率
                    if '指数' in line and '同期收益率' in line:
                        index_match = index_return_pattern.search(line)
                        if index_match:
                            self.data['index_returns'].append(float(index_match.group(1)))
                    
                    # 提取超额收益
                    if '策略超额收益' in line:
                        excess_match = excess_return_pattern.search(line)
                        if excess_match:
                            self.data['excess_returns'].append(float(excess_match.group(1)))
                    
                    # 提取交易记录
                    trade_match = trade_pattern.search(line)
                    if trade_match:
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
            return True
        except Exception as e:
            print(f"解析日志文件时出错: {e}")
            return False
    
    def generate_html(self, output_file='backtest_report.html'):
        """生成HTML报表"""
        import pandas as pd
        import plotly.graph_objects as go
        import numpy as np
        from plotly.subplots import make_subplots
        
        if not self.data['dates']:
            print("没有数据可以生成报表")
            return False
        
        # 将数据转换为DataFrame
        df = pd.DataFrame({
            'date': pd.to_datetime(self.data['dates']),
            'strategy_returns': self.data['strategy_returns'],
            'index_returns': self.data['index_returns'] if len(self.data['index_returns']) == len(self.data['dates']) else [0] * len(self.data['dates']),
            'excess_returns': self.data['excess_returns'] if len(self.data['excess_returns']) == len(self.data['dates']) else [0] * len(self.data['dates'])
        })
        
        # 如果有资金数据，添加到DataFrame
        if len(self.data['cash']) == len(self.data['dates']):
            df['cash'] = self.data['cash']
            df['market_value'] = self.data['market_value']
            df['total_assets'] = self.data['total_assets']
        
        df = df.sort_values('date')
        
        # 1. 创建累积收益率对比图和超额收益图
        fig1 = make_subplots(
            rows=2, cols=1, 
            shared_xaxes=True,
            subplot_titles=('累积收益率对比', '超额收益率'),
            vertical_spacing=0.12,
            row_heights=[0.7, 0.3]
        )
        
        # 计算累积收益率
        df['cumulative_strategy'] = (1 + df['strategy_returns'] / 100).cumprod() - 1
        df['cumulative_index'] = (1 + df['index_returns'] / 100).cumprod() - 1
        
        # 添加累积收益率曲线
        fig1.add_trace(
            go.Scatter(
                x=df['date'], 
                y=df['cumulative_strategy'] * 100,
                name='策略收益',
                line=dict(color='rgb(0, 100, 80)', width=2)
            ),
            row=1, col=1
        )
        
        fig1.add_trace(
            go.Scatter(
                x=df['date'], 
                y=df['cumulative_index'] * 100,
                name='指数收益',
                line=dict(color='rgb(205, 12, 24)', width=2)
            ),
            row=1, col=1
        )
        
        # 添加超额收益率
        fig1.add_trace(
            go.Bar(
                x=df['date'], 
                y=df['excess_returns'],
                name='每日超额收益',
                marker_color='rgba(0, 0, 255, 0.5)'
            ),
            row=2, col=1
        )
        
        # 更新布局
        fig1.update_layout(
            title='回测结果分析 - 累积收益对比',
            template='plotly_white',
            hovermode='x unified',
            height=500,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        fig1.update_yaxes(title_text='收益率(%)', row=1, col=1)
        fig1.update_yaxes(title_text='超额收益(%)', row=2, col=1)
        
        # 2. 创建每日收益率图表
        fig2 = go.Figure()
        
        fig2.add_trace(
            go.Scatter(
                x=df['date'], 
                y=df['strategy_returns'],
                name='策略日收益率',
                line=dict(color='rgb(0, 100, 80)', width=2)
            )
        )
        
        fig2.add_trace(
            go.Scatter(
                x=df['date'], 
                y=df['index_returns'],
                name='指数日收益率',
                line=dict(color='rgb(205, 12, 24)', width=2)
            )
        )
        
        # 添加零线
        fig2.add_shape(
            type="line",
            x0=df['date'].min(),
            y0=0,
            x1=df['date'].max(),
            y1=0,
            line=dict(color="black", width=1, dash="dash")
        )
        
        fig2.update_layout(
            title='每日收益率',
            template='plotly_white',
            hovermode='x unified',
            height=400,
            yaxis_title='日收益率(%)',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        # 3. 资金曲线图表
        fig3 = None
        if 'total_assets' in df.columns:
            fig3 = make_subplots(
                rows=1, cols=1,
                specs=[[{"secondary_y": True}]]
            )
            
            # 添加总资产曲线
            fig3.add_trace(
                go.Scatter(
                    x=df['date'], 
                    y=df['total_assets'],
                    name='总资产',
                    line=dict(color='rgb(31, 119, 180)', width=2)
                )
            )
            
            # 添加现金曲线
            fig3.add_trace(
                go.Scatter(
                    x=df['date'], 
                    y=df['cash'],
                    name='现金',
                    line=dict(color='rgb(148, 103, 189)', width=2, dash='dot'),
                    fill='tozeroy',
                    fillcolor='rgba(148, 103, 189, 0.1)'
                )
            )
            
            # 添加市值曲线
            fig3.add_trace(
                go.Scatter(
                    x=df['date'], 
                    y=df['market_value'],
                    name='持仓市值',
                    line=dict(color='rgb(44, 160, 44)', width=2, dash='dot'),
                    fill='tozeroy',
                    fillcolor='rgba(44, 160, 44, 0.1)'
                )
            )
            
            # 在右轴添加仓位比例
            df['position_ratio'] = df['market_value'] / df['total_assets'] * 100
            
            fig3.add_trace(
                go.Scatter(
                    x=df['date'], 
                    y=df['position_ratio'],
                    name='仓位比例(%)',
                    line=dict(color='rgb(214, 39, 40)', width=2)
                ),
                secondary_y=True
            )
            
            fig3.update_layout(
                title='资金曲线与仓位分析',
                template='plotly_white',
                hovermode='x unified',
                height=400,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            fig3.update_yaxes(title_text='金额', secondary_y=False)
            fig3.update_yaxes(title_text='仓位比例(%)', secondary_y=True)
        
        # 4. 回撤分析图表
        fig4 = go.Figure()
        
        # 计算历史新高点
        df['strategy_peak'] = df['cumulative_strategy'].cummax()
        df['index_peak'] = df['cumulative_index'].cummax()
        
        # 计算回撤
        df['strategy_drawdown'] = (df['cumulative_strategy'] - df['strategy_peak']) / df['strategy_peak'] * 100
        df['index_drawdown'] = (df['cumulative_index'] - df['index_peak']) / df['index_peak'] * 100
        
        # 添加策略回撤曲线
        fig4.add_trace(
            go.Scatter(
                x=df['date'], 
                y=df['strategy_drawdown'],
                name='策略回撤',
                line=dict(color='rgb(214, 39, 40)', width=2),
                fill='tozeroy',
                fillcolor='rgba(214, 39, 40, 0.1)'
            )
        )
        
        # 添加指数回撤曲线
        fig4.add_trace(
            go.Scatter(
                x=df['date'], 
                y=df['index_drawdown'],
                name='指数回撤',
                line=dict(color='rgb(31, 119, 180)', width=2),
                fill='tozeroy',
                fillcolor='rgba(31, 119, 180, 0.1)'
            )
        )
        
        fig4.update_layout(
            title='回撤分析',
            template='plotly_white',
            hovermode='x unified',
            height=400,
            yaxis_title='回撤比例(%)',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        # 5. 计算更多指标
        # 年化收益率
        days = (df['date'].max() - df['date'].min()).days
        if days > 0:
            annual_strategy_return = ((1 + df['cumulative_strategy'].iloc[-1]) ** (365 / days) - 1) * 100
            annual_index_return = ((1 + df['cumulative_index'].iloc[-1]) ** (365 / days) - 1) * 100
        else:
            annual_strategy_return = df['cumulative_strategy'].iloc[-1] * 100
            annual_index_return = df['cumulative_index'].iloc[-1] * 100
        
        # 最大回撤
        max_strategy_drawdown = df['strategy_drawdown'].min()
        max_index_drawdown = df['index_drawdown'].min()
        
        # 夏普比率 (假设无风险利率为3%)
        risk_free_rate = 3
        daily_risk_free = (1 + risk_free_rate/100) ** (1/365) - 1
        
        # 计算超额日收益率
        df['excess_daily_return'] = df['strategy_returns'] / 100 - daily_risk_free
        
        # 计算夏普比率
        if len(df) > 1:
            excess_returns_mean = df['excess_daily_return'].mean() * 365
            excess_returns_std = df['excess_daily_return'].std() * np.sqrt(365)
            if excess_returns_std != 0:
                sharpe_ratio = excess_returns_mean / excess_returns_std
            else:
                sharpe_ratio = 0
        else:
            sharpe_ratio = 0
        
        # 盈亏比
        winning_days = df[df['strategy_returns'] > 0]
        losing_days = df[df['strategy_returns'] < 0]
        
        if len(winning_days) > 0 and len(losing_days) > 0:
            avg_win = winning_days['strategy_returns'].mean()
            avg_loss = abs(losing_days['strategy_returns'].mean())
            win_loss_ratio = avg_win / avg_loss if avg_loss != 0 else float('inf')
            win_rate = len(winning_days) / len(df) * 100
        else:
            win_loss_ratio = 0
            win_rate = 0 if len(df) == 0 else (len(winning_days) / len(df) * 100)
        
        # 生成HTML报表
        with open(output_file, 'w', encoding='utf-8') as f:
            # CSS样式
            html_header = '''
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>回测结果分析</title>
                <style>
                    body {
                        font-family: Arial, sans-serif;
                        margin: 0;
                        padding: 20px;
                        background-color: #f5f5f5;
                    }
                    .container {
                        max-width: 1200px;
                        margin: 0 auto;
                        background-color: white;
                        padding: 20px;
                        border-radius: 5px;
                        box-shadow: 0 0 10px rgba(0,0,0,0.1);
                    }
                    h1, h2 {
                        color: #333;
                    }
                    table {
                        width: 100%;
                        border-collapse: collapse;
                        margin: 20px 0;
                    }
                    th, td {
                        padding: 10px;
                        border: 1px solid #ddd;
                        text-align: left;
                    }
                    th {
                        background-color: #f2f2f2;
                    }
                    tr:nth-child(even) {
                        background-color: #f9f9f9;
                    }
                    .buy {
                        color: #009900;
                    }
                    .sell {
                        color: #cc0000;
                    }
                    .metrics {
                        display: flex;
                        flex-wrap: wrap;
                        margin-bottom: 20px;
                    }
                    .metric-box {
                        background-color: #f2f2f2;
                        border-radius: 5px;
                        padding: 15px;
                        margin: 10px;
                        flex: 1;
                        min-width: 200px;
                        box-shadow: 0 0 5px rgba(0,0,0,0.05);
                    }
                    .metric-title {
                        font-size: 14px;
                        color: #666;
                        margin-bottom: 5px;
                    }
                    .metric-value {
                        font-size: 24px;
                        font-weight: bold;
                        color: #333;
                    }
                    .positive {
                        color: #009900;
                    }
                    .negative {
                        color: #cc0000;
                    }
                    .chart-container {
                        margin-bottom: 30px;
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>回测结果分析</h1>
            '''
            
            # 计算关键指标
            final_strategy_return = df['cumulative_strategy'].iloc[-1] * 100
            final_index_return = df['cumulative_index'].iloc[-1] * 100
            final_excess_return = final_strategy_return - final_index_return
            
            # 添加关键指标
            metrics_html = f'''
            <div class="metrics">
                <div class="metric-box">
                    <div class="metric-title">策略总收益</div>
                    <div class="metric-value {'positive' if final_strategy_return >= 0 else 'negative'}">{final_strategy_return:.2f}%</div>
                </div>
                <div class="metric-box">
                    <div class="metric-title">指数总收益</div>
                    <div class="metric-value {'positive' if final_index_return >= 0 else 'negative'}">{final_index_return:.2f}%</div>
                </div>
                <div class="metric-box">
                    <div class="metric-title">超额收益</div>
                    <div class="metric-value {'positive' if final_excess_return >= 0 else 'negative'}">{final_excess_return:.2f}%</div>
                </div>
                <div class="metric-box">
                    <div class="metric-title">年化收益率</div>
                    <div class="metric-value {'positive' if annual_strategy_return >= 0 else 'negative'}">{annual_strategy_return:.2f}%</div>
                </div>
            </div>
            <div class="metrics">
                <div class="metric-box">
                    <div class="metric-title">最大回撤</div>
                    <div class="metric-value negative">{max_strategy_drawdown:.2f}%</div>
                </div>
                <div class="metric-box">
                    <div class="metric-title">夏普比率</div>
                    <div class="metric-value {'positive' if sharpe_ratio > 0 else 'negative'}">{sharpe_ratio:.2f}</div>
                </div>
                <div class="metric-box">
                    <div class="metric-title">胜率</div>
                    <div class="metric-value {'positive' if win_rate > 50 else 'negative'}">{win_rate:.2f}%</div>
                </div>
                <div class="metric-box">
                    <div class="metric-title">盈亏比</div>
                    <div class="metric-value {'positive' if win_loss_ratio > 1 else 'negative'}">{win_loss_ratio:.2f}</div>
                </div>
            </div>
            '''
            
            # 将plotly图表转换为HTML
            import plotly.io as pio
            
            chart_html = '''
            <div class="chart-container">
                <h2>累积收益与超额收益</h2>
            '''
            chart_html += pio.to_html(fig1, include_plotlyjs='cdn', full_html=False)
            chart_html += '''
            </div>
            
            <div class="chart-container">
                <h2>每日收益率</h2>
            '''
            chart_html += pio.to_html(fig2, include_plotlyjs=False, full_html=False)
            chart_html += '''
            </div>
            '''
            
            # 添加资金曲线图(如果有数据)
            if fig3:
                chart_html += '''
                <div class="chart-container">
                    <h2>资金曲线与仓位分析</h2>
                '''
                chart_html += pio.to_html(fig3, include_plotlyjs=False, full_html=False)
                chart_html += '''
                </div>
                '''
            
            # 添加回撤分析图
            chart_html += '''
            <div class="chart-container">
                <h2>回撤分析</h2>
            '''
            chart_html += pio.to_html(fig4, include_plotlyjs=False, full_html=False)
            chart_html += '''
            </div>
            '''
            
            # 交易记录表格
            trades_df = pd.DataFrame(self.data['trades'])
            if not trades_df.empty:
                trades_df = trades_df.sort_values('date', ascending=False)
                trades_html = '''
                <h2>交易记录</h2>
                <table>
                    <tr>
                        <th>日期</th>
                        <th>操作</th>
                        <th>股票</th>
                        <th>数量</th>
                        <th>价格</th>
                        <th>金额</th>
                    </tr>
                '''
                
                for _, row in trades_df.iterrows():
                    action_class = 'buy' if row['action'] == '买入' else 'sell'
                    amount = row['amount'] * row['price']
                    trades_html += f'''
                    <tr>
                        <td>{row['date']}</td>
                        <td class="{action_class}">{row['action']}</td>
                        <td>{row['stock']}</td>
                        <td>{row['amount']}</td>
                        <td>{row['price']:.2f}</td>
                        <td>{amount:.2f}</td>
                    </tr>
                    '''
                
                trades_html += '</table>'
            else:
                trades_html = '<h2>没有交易记录</h2>'
            
            # 完整HTML
            html = html_header + metrics_html + chart_html + trades_html + '''
                </div>
            </body>
            </html>
            '''
            
            f.write(html)
            
        print(f"HTML报表已生成: {output_file}")
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