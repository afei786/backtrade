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
                
                self.log_message(f"指数{self.index_code}当天收益率: {index_return:.2f}%, 当日涨跌幅{pct_change_index:.2f}%, 指数总收益率: {index_profit_rate:.2f}%")
                
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
        
        # 定义更精确的正则表达式模式
        date_pattern = re.compile(r'\[(\d{4}-\d{2}-\d{2})\]')
        # 使用更灵活的正则表达式匹配不同格式的日志条目
        strategy_return_pattern = re.compile(r'总收益率\s*([-\d.]+)%')
        index_day_return_pattern = re.compile(r'指数.*?当天收益率:\s*([-\d.]+)%')
        # 修改指数收益率模式，同时匹配"持仓收益率"和"指数总收益率"
        index_profit_pattern = re.compile(r'指数.*?(持仓收益率|指数总收益率):\s*([-\d.]+)%')
        excess_return_pattern = re.compile(r'策略超额收益:\s*([-\d.]+)%')
        trade_pattern = re.compile(r'\[(\d{4}-\d{2}-\d{2})\]\s+(买入|卖出)\s+([\w.]+)\s+(\d+)\s+股\s+@\s+([\d.]+)')
        
        # 使用更灵活的正则表达式匹配日终总结
        # 允许数字之间有空格，允许标点符号前后有空格
        daily_summary_pattern = re.compile(
            r'当日总结:.*?总市值\s*([\d.]+).*?现金\s*([\d.]+).*?总资产\s*([\d.]+).*?当日盈亏\s*([-\d.]+).*?累计盈亏\s*([-\d.]+).*?总收益率\s*([-\d.]+)%'
        )
        
        try:
            with open(self.log_file, 'r', encoding='utf-8') as f:
                content = f.read()
                print(f"成功读取日志文件，大小为 {len(content)} 字节")
                
                # 预处理：删除所有空行和回测开始/结束的分隔符行
                content = re.sub(r'\n\s*\n', '\n', content)
                content = re.sub(r'=+\n', '', content)
                content = re.sub(r'回测日志.*\n', '', content)
                content = re.sub(r'回测结束\n', '', content)
                
                # 查找所有日期标记
                all_dates = date_pattern.findall(content)
                unique_dates = sorted(set(all_dates))
                print(f"找到 {len(unique_dates)} 个唯一日期")
                
                # 使用日期模式分割日志内容为日期块
                # 创建一个字典，用于存储每个日期的所有相关行
                date_blocks_dict = {}
                
                # 初始化字典
                for date in unique_dates:
                    date_blocks_dict[date] = []
                
                # 按行处理内容，将每一行分配给相应的日期
                current_date = None
                for line in content.split('\n'):
                    date_match = date_pattern.search(line)
                    if date_match:
                        current_date = date_match.group(1)
                    
                    if current_date and current_date in date_blocks_dict:
                        date_blocks_dict[current_date].append(line)
                
                # 将字典转换为日期块列表
                date_blocks = []
                for date in unique_dates:
                    if date in date_blocks_dict and date_blocks_dict[date]:
                        date_block = '\n'.join(date_blocks_dict[date])
                        date_blocks.append(date_block)
                
                print(f"处理了 {len(date_blocks)} 个日期块")
                
                # 用于验证的数据收集
                validation_data = []
                
                # 计数器，用于跟踪成功解析的日期块数量
                successful_blocks = 0
                failed_blocks = 0
                
                # 处理每个日期块
                for block in date_blocks:
                    # 提取日期
                    date_match = date_pattern.search(block)
                    if not date_match:
                        print(f"警告: 无法从日期块中提取日期: {block[:100]}...")
                        continue
                        
                    current_date = date_match.group(1)
                    
                    # 提取日终总结数据
                    summary_match = daily_summary_pattern.search(block)
                    if summary_match:
                        try:
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
                            
                            # 尝试提取指数收益率（可能是"持仓收益率"或"指数总收益率"）
                            index_profit_match = index_profit_pattern.search(block)
                            if index_profit_match:
                                index_return = float(index_profit_match.group(2))  # 注意：组索引从1变为2
                                print(f"日期 {current_date} 的指数收益率类型: {index_profit_match.group(1)}, 值: {index_return}")
                            else:
                                print(f"警告: 日期 {current_date} 没有找到指数收益率数据")
                                # 调试: 检查是否包含指数相关信息
                                for line in block.split('\n'):
                                    if '指数' in line and '收益率' in line:
                                        print(f"包含指数信息的行: {line}")
                                
                            # 尝试提取超额收益
                            excess_match = excess_return_pattern.search(block)
                            if excess_match:
                                excess_return = float(excess_match.group(1))
                            else:
                                # 如果没有直接的超额收益数据，计算差值
                                excess_return = strategy_return - index_return
                                print(f"日期 {current_date} 的超额收益率是计算得出的: {excess_return}")
                            
                            self.data['index_returns'].append(index_return)
                            self.data['excess_returns'].append(excess_return)
                            
                            # 收集验证数据
                            validation_data.append({
                                'date': current_date,
                                'strategy_return': strategy_return,
                                'index_return': index_return,
                                'excess_return': excess_return,
                                'total_assets': total_assets,
                                'daily_profit': daily_profit
                            })
                            
                            successful_blocks += 1
                        except (ValueError, IndexError) as e:
                            print(f"解析日期 {current_date} 的数据时出错: {e}")
                            print(f"匹配内容: {summary_match.group(0)}")
                            failed_blocks += 1
                            continue
                    else:
                        print(f"警告: 日期 {current_date} 没有找到日终总结数据")
                        # 调试: 打印该日期块的内容，以便检查格式
                        if '当日总结' in block:
                            print(f"日期 {current_date} 包含'当日总结'但无法匹配正则表达式")
                            # 提取包含"当日总结"的行
                            for line in block.split('\n'):
                                if '当日总结' in line:
                                    print(f"当日总结行: {line}")
                        failed_blocks += 1
                    
                    # 提取交易记录
                    trade_count = 0
                    for trade_match in trade_pattern.finditer(block):
                        try:
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
                            trade_count += 1
                        except (ValueError, IndexError) as e:
                            print(f"解析交易记录时出错: {e}")
                            continue
                    
                    if trade_count > 0:
                        print(f"日期 {current_date} 解析到 {trade_count} 条交易记录")
            
            if not self.data['dates']:
                print("警告: 没有从日志文件中提取到任何日期数据")
                return False
                
            print(f"解析完成，获取了 {len(self.data['dates'])} 天的数据")
            print(f"成功解析的日期块: {successful_blocks}, 失败的日期块: {failed_blocks}")
            
            # 确保所有数据列的长度一致
            min_length = min(len(self.data['dates']), len(self.data['strategy_returns']))
            
            # 截断所有数据列到相同长度
            self.data['dates'] = self.data['dates'][:min_length]
            self.data['strategy_returns'] = self.data['strategy_returns'][:min_length]
            self.data['market_value'] = self.data['market_value'][:min_length]
            self.data['cash'] = self.data['cash'][:min_length]
            self.data['total_assets'] = self.data['total_assets'][:min_length]
            self.data['daily_profit'] = self.data['daily_profit'][:min_length]
            self.data['cumulative_profit'] = self.data['cumulative_profit'][:min_length]
            
            # 处理可能长度不一致的数据列
            if len(self.data['index_returns']) >= min_length:
                self.data['index_returns'] = self.data['index_returns'][:min_length]
            else:
                self.data['index_returns'] = self.data['index_returns'] + [0] * (min_length - len(self.data['index_returns']))
                
            if len(self.data['excess_returns']) >= min_length:
                self.data['excess_returns'] = self.data['excess_returns'][:min_length]
            else:
                self.data['excess_returns'] = self.data['excess_returns'] + [0] * (min_length - len(self.data['excess_returns']))
            
            # 将日期按照时间顺序排序
            sorted_indices = sorted(range(len(self.data['dates'])), key=lambda i: self.data['dates'][i])
            
            # 按照排序后的索引重排所有数据
            for key in self.data:
                if key == 'trades':
                    continue  # 交易记录不需要重排
                self.data[key] = [self.data[key][i] for i in sorted_indices]
            
            # 验证解析出的数据与日志文件中的数据一致
            self._validate_parsed_data(validation_data)
            
            return True
        except Exception as e:
            print(f"解析日志文件时出错: {e}")
            import traceback
            traceback.print_exc()
            return False
            
    def _validate_parsed_data(self, validation_data):
        """验证解析出的数据与日志文件中的数据一致"""
        if not validation_data:
            print("警告: 没有验证数据可用")
            return
            
        # 将验证数据按日期排序
        validation_data.sort(key=lambda x: x['date'])
        
        # 检查数据长度是否一致
        if len(validation_data) != len(self.data['dates']):
            print(f"警告: 验证数据长度 ({len(validation_data)}) 与解析数据长度 ({len(self.data['dates'])}) 不一致")
            # 找出哪些日期在验证数据中但不在解析数据中
            validation_dates = [item['date'] for item in validation_data]
            parsed_dates = self.data['dates']
            missing_dates = [date for date in validation_dates if date not in parsed_dates]
            extra_dates = [date for date in parsed_dates if date not in validation_dates]
            if missing_dates:
                print(f"缺少的日期: {missing_dates}")
            if extra_dates:
                print(f"多余的日期: {extra_dates}")
            
        # 检查每一天的数据是否一致
        for i, item in enumerate(validation_data):
            if i >= len(self.data['dates']):
                break
                
            date = item['date']
            if date != self.data['dates'][i]:
                print(f"警告: 日期不匹配 - 验证数据: {date}, 解析数据: {self.data['dates'][i]}")
                continue
                
            # 检查关键数据是否一致
            if abs(item['strategy_return'] - self.data['strategy_returns'][i]) > 0.01:
                print(f"警告: 日期 {date} 的策略收益率不匹配 - 验证数据: {item['strategy_return']}, 解析数据: {self.data['strategy_returns'][i]}")
                
            if abs(item['index_return'] - self.data['index_returns'][i]) > 0.01:
                print(f"警告: 日期 {date} 的指数收益率不匹配 - 验证数据: {item['index_return']}, 解析数据: {self.data['index_returns'][i]}")
                
            if abs(item['excess_return'] - self.data['excess_returns'][i]) > 0.01:
                print(f"警告: 日期 {date} 的超额收益率不匹配 - 验证数据: {item['excess_return']}, 解析数据: {self.data['excess_returns'][i]}")
        
        print("数据验证完成")
    
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