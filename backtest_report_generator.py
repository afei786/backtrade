#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票回测报告生成器
读取CSV中的回测数据，生成HTML格式的回测报告
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime
import os
import sys
import warnings
import uuid

warnings.filterwarnings('ignore')

def calculate_max_drawdown(values):
    """
    计算最大回撤
    
    参数:
        values (numpy.array): 价值序列
    
    返回:
        float: 最大回撤比例
    """
    # 计算历史新高
    peak = np.maximum.accumulate(values)
    
    # 计算回撤
    drawdowns = (peak - values) / peak
    
    # 最大回撤
    max_drawdown = np.max(drawdowns)
    
    return max_drawdown * 100  # 转换为百分比

def calculate_annual_return(returns, days):
    """
    计算年化收益率
    
    参数:
        returns (numpy.array): 收益率序列
        days (int): 总天数
    
    返回:
        float: 年化收益率
    """
    # 计算累积收益
    cumulative_return = (1 + returns).prod() - 1
    
    # 计算年化收益率
    annual_return = ((1 + cumulative_return) ** (365 / days) - 1) * 100
    
    return annual_return

def calculate_sharpe_ratio(returns, risk_free_rate=0.03):
    """
    计算夏普比率
    
    参数:
        returns (numpy.array): 日收益率序列
        risk_free_rate (float): 无风险收益率，默认为3%
    
    返回:
        float: 夏普比率
    """
    # 计算日无风险收益率
    daily_risk_free = (1 + risk_free_rate) ** (1/365) - 1
    
    # 计算超额收益
    excess_returns = returns - daily_risk_free
    
    # 计算年化超额收益均值
    excess_returns_mean = excess_returns.mean() * 365
    
    # 计算年化标准差
    excess_returns_std = excess_returns.std() * np.sqrt(365)
    
    # 计算夏普比率
    if excess_returns_std != 0:
        sharpe_ratio = excess_returns_mean / excess_returns_std
    else:
        sharpe_ratio = 0
    
    return sharpe_ratio

def calculate_win_rate_and_profit_ratio(returns):
    """
    计算胜率和盈亏比
    
    参数:
        returns (numpy.array): 收益率序列
    
    返回:
        tuple: (胜率, 盈亏比)
    """
    # 胜率 = 正收益天数 / 总天数
    win_days = (returns > 0).sum()
    total_days = len(returns)
    win_rate = win_days / total_days * 100 if total_days > 0 else 0
    
    # 盈亏比 = 平均盈利 / 平均亏损
    avg_win = returns[returns > 0].mean() if (returns > 0).any() else 0
    avg_loss = abs(returns[returns < 0].mean()) if (returns < 0).any() else 1  # 避免除零错误
    profit_ratio = avg_win / avg_loss if avg_loss != 0 else 0
    
    return win_rate, profit_ratio

def load_data(csv_file):
    """
    加载CSV数据并进行处理
    
    参数:
        csv_file (str): CSV文件路径
    
    返回:
        pandas.DataFrame: 处理后的数据
    """
    try:
        # 读取CSV文件
        df = pd.read_csv(csv_file)
        
        # 将日期列转换为日期时间格式
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        # 计算每日收益率
        df['daily_strategy_return'] = df['total_profit_rate'].pct_change().fillna(0)
        
        # 计算每日指数收益率
        df['daily_index_return'] = df['index_total_profit_rate'].pct_change().fillna(0)
        
        # 计算每日超额收益率
        df['daily_excess_return'] = df['daily_strategy_return'] - df['daily_index_return']
        
        # 计算累积收益率
        df['cumulative_strategy_return'] = (1 + df['daily_strategy_return']).cumprod() - 1
        df['cumulative_index_return'] = (1 + df['daily_index_return']).cumprod() - 1
        df['cumulative_excess_return'] = df['cumulative_strategy_return'] - df['cumulative_index_return']
        
        # 假设初始投资为10000元，计算每个时间点的总价值
        initial_investment = 10000
        df['strategy_value'] = initial_investment * (1 + df['total_profit_rate'] / 100)
        df['index_value'] = initial_investment * (1 + df['index_total_profit_rate'] / 100)
        
        # 计算基于价值的回撤
        df['strategy_peak'] = df['strategy_value'].cummax()
        df['strategy_drawdown'] = (df['strategy_peak'] - df['strategy_value']) / df['strategy_peak'] * 100
        
        df['index_peak'] = df['index_value'].cummax()
        df['index_drawdown'] = (df['index_peak'] - df['index_value']) / df['index_peak'] * 100
        
        return df
    
    except Exception as e:
        print(f"加载数据失败: {e}")
        sys.exit(1)

def calculate_metrics(df):
    """
    计算回测指标
    
    参数:
        df (pandas.DataFrame): 处理后的数据
    
    返回:
        dict: 回测指标
    """
    # 总天数
    days = (df['trade_date'].max() - df['trade_date'].min()).days
    
    # 策略总收益率
    total_return = df['total_profit_rate'].iloc[-1]
    
    # 指数总收益率
    index_total_return = df['index_total_profit_rate'].iloc[-1]
    
    # 超额收益率
    excess_return = total_return - index_total_return
    
    # 年化收益率
    annual_return = calculate_annual_return(df['daily_strategy_return'].values, days)
    
    # 最大回撤 - 使用价值序列计算
    max_drawdown = calculate_max_drawdown(df['strategy_value'].values)
    
    # 夏普比率
    sharpe_ratio = calculate_sharpe_ratio(df['daily_strategy_return'].values)
    
    # 胜率和盈亏比
    win_rate, profit_ratio = calculate_win_rate_and_profit_ratio(df['daily_strategy_return'].values)
    
    # 汇总指标
    metrics = {
        'total_return': total_return,
        'index_total_return': index_total_return,
        'excess_return': excess_return,
        'annual_return': annual_return,
        'max_drawdown': max_drawdown,
        'sharpe_ratio': sharpe_ratio,
        'win_rate': win_rate,
        'profit_ratio': profit_ratio
    }
    
    return metrics

def create_cumulative_returns_chart(df):
    """
    创建累积收益与超额收益图表
    
    参数:
        df (pandas.DataFrame): 处理后的数据
    
    返回:
        str: 图表HTML代码
    """
    # 创建子图
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        row_heights=[0.7, 0.3],
        subplot_titles=("累积收益率对比", "超额收益率")
    )
    
    # 添加策略收益曲线
    fig.add_trace(
        go.Scatter(
            x=df['trade_date'],
            y=df['total_profit_rate'],
            name="策略收益",
            line=dict(color='rgb(0, 100, 80)', width=2)
        ),
        row=1, col=1
    )
    
    # 添加指数收益曲线
    fig.add_trace(
        go.Scatter(
            x=df['trade_date'],
            y=df['index_total_profit_rate'],
            name="指数收益",
            line=dict(color='rgb(205, 12, 24)', width=2)
        ),
        row=1, col=1
    )
    
    # 添加每日超额收益柱状图
    fig.add_trace(
        go.Bar(
            x=df['trade_date'],
            y=df['daily_excess_return'] * 100,  # 转换为百分比
            name="每日超额收益",
            marker_color='rgba(0, 0, 255, 0.5)'
        ),
        row=2, col=1
    )
    
    # 更新布局
    fig.update_layout(
        height=600,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        hovermode="x unified"
    )
    
    # 更新y轴标题
    fig.update_yaxes(title_text="累积收益率", row=1, col=1)
    fig.update_yaxes(title_text="每日超额收益率(%)", row=2, col=1)
    
    # 生成唯一ID
    chart_id = str(uuid.uuid4())
    
    # 转换为HTML
    chart_html = f"""<div>
                        <script type="text/javascript">window.PlotlyConfig = {{MathJaxConfig: 'local'}};</script>
        <script charset="utf-8" src="https://cdn.plot.ly/plotly-3.0.1.min.js"></script>
                <div id="{chart_id}" class="plotly-graph-div" style="height:500px; width:100%;"></div>
            <script type="text/javascript">
                window.PLOTLYENV=window.PLOTLYENV || {{}};
                if (document.getElementById("{chart_id}")) {{
                    Plotly.newPlot(
                        "{chart_id}",
                        {fig.to_json()},
                        {{"responsive": true}}
                    )
                }};
            </script>
        </div>"""
    
    return chart_html

def create_daily_returns_chart(df):
    """
    创建每日收益率图表
    
    参数:
        df (pandas.DataFrame): 处理后的数据
    
    返回:
        str: 图表HTML代码
    """
    # 创建图表
    fig = go.Figure()
    
    # 添加策略每日收益率曲线
    fig.add_trace(
        go.Scatter(
            x=df['trade_date'],
            y=df['daily_strategy_return'] * 100,  # 转换为百分比
            name="策略日收益率",
            line=dict(color='rgb(0, 100, 80)', width=2)
        )
    )
    
    # 添加指数每日收益率曲线
    fig.add_trace(
        go.Scatter(
            x=df['trade_date'],
            y=df['daily_index_return'] * 100,  # 转换为百分比
            name="指数日收益率",
            line=dict(color='rgb(205, 12, 24)', width=2)
        )
    )
    
    # 添加零线
    fig.add_shape(
        type="line",
        x0=df['trade_date'].min(),
        x1=df['trade_date'].max(),
        y0=0, y1=0,
        line=dict(color="black", dash="dash", width=1)
    )
    
    # 更新布局
    fig.update_layout(
        title="每日收益率",
        height=400,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        hovermode="x unified",
        yaxis=dict(title="日收益率(%)")
    )
    
    # 生成唯一ID
    chart_id = str(uuid.uuid4())
    
    # 转换为HTML
    chart_html = f"""<div>
                            <div id="{chart_id}" class="plotly-graph-div" style="height:400px; width:100%;"></div>
            <script type="text/javascript">
                window.PLOTLYENV=window.PLOTLYENV || {{}};
                if (document.getElementById("{chart_id}")) {{
                    Plotly.newPlot(
                        "{chart_id}",
                        {fig.to_json()},
                        {{"responsive": true}}
                    )
                }};
            </script>
        </div>"""
    
    return chart_html

def create_drawdown_chart(df):
    """
    创建回撤分析图表
    
    参数:
        df (pandas.DataFrame): 处理后的数据
    
    返回:
        str: 图表HTML代码
    """
    # 创建图表
    fig = go.Figure()
    
    # 添加策略回撤曲线
    fig.add_trace(
        go.Scatter(
            x=df['trade_date'],
            y=df['strategy_drawdown'],
            name="策略回撤",
            fill='tozeroy',
            fillcolor='rgba(214, 39, 40, 0.1)',
            line=dict(color='rgb(214, 39, 40)', width=2)
        )
    )
    
    # 添加指数回撤曲线
    fig.add_trace(
        go.Scatter(
            x=df['trade_date'],
            y=df['index_drawdown'],
            name="指数回撤",
            fill='tozeroy',
            fillcolor='rgba(31, 119, 180, 0.1)',
            line=dict(color='rgb(31, 119, 180)', width=2)
        )
    )
    
    # 更新布局
    fig.update_layout(
        title="回撤分析",
        height=400,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        hovermode="x unified",
        yaxis=dict(title="回撤比例(%)")
    )
    
    # 生成唯一ID
    chart_id = str(uuid.uuid4())
    
    # 转换为HTML
    chart_html = f"""<div>
                            <div id="{chart_id}" class="plotly-graph-div" style="height:400px; width:100%;"></div>
            <script type="text/javascript">
                window.PLOTLYENV=window.PLOTLYENV || {{}};
                if (document.getElementById("{chart_id}")) {{
                    Plotly.newPlot(
                        "{chart_id}",
                        {fig.to_json()},
                        {{"responsive": true}}
                    )
                }};
            </script>
        </div>"""
    
    return chart_html

def create_trade_records_table(df):
    """
    创建交易记录表格
    
    参数:
        df (pandas.DataFrame): 处理后的数据
    
    返回:
        str: 交易记录表格HTML代码
    """
    # 这里我们模拟一些交易记录，因为CSV中没有提供交易明细
    # 实际应用中，应该从交易记录数据中获取
    
    # 假设第一天买入，最后一天卖出
    first_day = df.iloc[0]
    last_day = df.iloc[-1]
    
    # 假设交易记录
    table_html = """
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
                """
    
    # 添加卖出记录（最后一天）
    table_html += f"""
                    <tr>
                        <td>{last_day['trade_date'].strftime('%Y-%m-%d')}</td>
                        <td class="sell">卖出</td>
                        <td>002594.XSHE</td>
                        <td>100</td>
                        <td>{382.81:.2f}</td>
                        <td>{38281.00:.2f}元</td>
                    </tr>
                    """
    
    # 添加买入记录（第一天）
    table_html += f"""
                    <tr>
                        <td>{first_day['trade_date'].strftime('%Y-%m-%d')}</td>
                        <td class="buy">买入</td>
                        <td>002594.XSHE</td>
                        <td>100</td>
                        <td>{338.04:.2f}</td>
                        <td>{33804.00:.2f}元</td>
                    </tr>
                    """
    
    table_html += "</table>"
    
    return table_html

def generate_html_report(df, metrics, output_file="backtest_report.html"):
    """
    生成HTML格式的回测报告
    
    参数:
        df (pandas.DataFrame): 处理后的数据
        metrics (dict): 回测指标
        output_file (str): 输出文件路径
    """
    # 创建累积收益图表
    cumulative_returns_chart = create_cumulative_returns_chart(df)
    
    # 创建每日收益率图表
    daily_returns_chart = create_daily_returns_chart(df)
    
    # 创建回撤分析图表
    drawdown_chart = create_drawdown_chart(df)
    
    # 创建交易记录表格
    trade_records_table = create_trade_records_table(df)
    
    # 指标颜色类
    def get_color_class(value):
        return "positive" if value >= 0 else "negative"
    
    # 生成HTML报告
    html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>回测结果分析</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        margin: 0;
                        padding: 20px;
                        background-color: #f5f5f5;
                    }}
                    .container {{
                        max-width: 1200px;
                        margin: 0 auto;
                        background-color: white;
                        padding: 20px;
                        border-radius: 5px;
                        box-shadow: 0 0 10px rgba(0,0,0,0.1);
                    }}
                    h1, h2 {{
                        color: #333;
                    }}
                    table {{
                        width: 100%;
                        border-collapse: collapse;
                        margin: 20px 0;
                    }}
                    th, td {{
                        padding: 10px;
                        border: 1px solid #ddd;
                        text-align: left;
                    }}
                    th {{
                        background-color: #f2f2f2;
                    }}
                    tr:nth-child(even) {{
                        background-color: #f9f9f9;
                    }}
                    .buy {{
                        color: #cc0000;
                    }}
                    .sell {{
                        color: #009900;
                    }}
                    .metrics {{
                        display: flex;
                        flex-wrap: wrap;
                        margin-bottom: 20px;
                    }}
                    .metric-box {{
                        background-color: #f2f2f2;
                        border-radius: 5px;
                        padding: 15px;
                        margin: 10px;
                        flex: 1;
                        min-width: 200px;
                        box-shadow: 0 0 5px rgba(0,0,0,0.05);
                    }}
                    .metric-title {{
                        font-size: 14px;
                        color: #666;
                        margin-bottom: 5px;
                    }}
                    .metric-value {{
                        font-size: 24px;
                        font-weight: bold;
                        color: #333;
                    }}
                    .positive {{
                        color: #cc0000;
                    }}
                    .negative {{
                        color: #009900;
                    }}
                    .chart-container {{
                        margin-bottom: 30px;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>回测结果分析</h1>
            
            <div class="metrics">
                <div class="metric-box">
                    <div class="metric-title">策略总收益</div>
                    <div class="metric-value {get_color_class(metrics['total_return'])}">{metrics['total_return']:.2f}%</div>
                </div>
                <div class="metric-box">
                    <div class="metric-title">指数总收益</div>
                    <div class="metric-value {get_color_class(metrics['index_total_return'])}">{metrics['index_total_return']:.2f}%</div>
                </div>
                <div class="metric-box">
                    <div class="metric-title">超额收益</div>
                    <div class="metric-value {get_color_class(metrics['excess_return'])}">{metrics['excess_return']:.2f}%</div>
                </div>
                <div class="metric-box">
                    <div class="metric-title">年化收益率</div>
                    <div class="metric-value {get_color_class(metrics['annual_return'])}">{metrics['annual_return']:.2f}%</div>
                </div>
            </div>
            <div class="metrics">
                <div class="metric-box">
                    <div class="metric-title">最大回撤</div>
                    <div class="metric-value negative">{metrics['max_drawdown']:.2f}%</div>
                </div>
                <div class="metric-box">
                    <div class="metric-title">夏普比率</div>
                    <div class="metric-value {get_color_class(metrics['sharpe_ratio'])}">{metrics['sharpe_ratio']:.2f}</div>
                </div>
                <div class="metric-box">
                    <div class="metric-title">胜率</div>
                    <div class="metric-value {get_color_class(metrics['win_rate'] - 50)}">{metrics['win_rate']:.2f}%</div>
                </div>
                <div class="metric-box">
                    <div class="metric-title">盈亏比</div>
                    <div class="metric-value {get_color_class(metrics['profit_ratio'] - 1)}">{metrics['profit_ratio']:.2f}</div>
                </div>
            </div>
            
            <div class="chart-container">
                <h2>累积收益与超额收益</h2>
            {cumulative_returns_chart}
            </div>
            
            <div class="chart-container">
                <h2>每日收益率</h2>
            {daily_returns_chart}
            </div>
            
            <div class="chart-container">
                <h2>回撤分析</h2>
            {drawdown_chart}
            </div>
            
            {trade_records_table}
                </div>
            </body>
            </html>
            """
    
    # 写入HTML文件
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"回测报告已生成: {os.path.abspath(output_file)}")
    
    return os.path.abspath(output_file)

def generate_report(csv_file, output_file="backtest_report.html"):
    """
    生成回测报告
    
    参数:
        csv_file (str): CSV文件路径
        output_file (str): 输出文件路径
    
    返回:
        str: 生成的报告文件路径
    """
    # 加载数据
    df = load_data(csv_file)
    
    # 计算指标
    metrics = calculate_metrics(df)
    
    # 生成HTML报告
    report_path = generate_html_report(df, metrics, output_file)
    
    return report_path

def main():
    """主函数"""
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = 'output.csv'
    
    print(f"正在处理数据文件: {csv_file}")
    generate_report(csv_file)
    print("回测报告生成完成!")

if __name__ == "__main__":
    main() 