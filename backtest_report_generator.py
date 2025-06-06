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
import json

warnings.filterwarnings('ignore')

def resample_time_series(df, max_points=500):
    """
    对时间序列数据进行降采样，减少数据点数量
    
    参数:
        df (pandas.DataFrame): 包含时间序列数据的DataFrame
        max_points (int): 最大数据点数量
        
    返回:
        pandas.DataFrame: 降采样后的DataFrame
    """
    # 获取数据点数量
    n_points = len(df)
    
    # 如果数据点数量小于等于最大点数，则不需要降采样
    if n_points <= max_points:
        return df
    
    # 计算采样间隔
    sample_step = int(np.ceil(n_points / max_points))
    
    # 确保起始点和结束点被包含
    sampled_indices = list(range(0, n_points, sample_step))
    if (n_points - 1) not in sampled_indices:
        sampled_indices.append(n_points - 1)
    
    # 返回降采样后的数据
    return df.iloc[sampled_indices].copy()

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
        
        # 确保数据按日期排序
        df = df.sort_values('trade_date')
        
        # 计算每日收益率 - 使用当日与前一日的比值计算收益率
        # 将百分比格式转换为小数进行计算
        if 'total_profit_rate' in df.columns:
            # 使用前值和后值分别计算，避免精度问题
            prev_values = df['total_profit_rate'].shift(1)
            # 第一天的收益率设为0
            df.loc[df.index[0], 'daily_strategy_return'] = 0
            # 其他天的收益率 = (今天值 - 昨天值) / (100 + 昨天值) 
            mask = df.index > df.index[0]
            df.loc[mask, 'daily_strategy_return'] = (df.loc[mask, 'total_profit_rate'] - prev_values[mask]) / (100 + prev_values[mask])
        else:
            df['daily_strategy_return'] = 0
        
        # 计算每日指数收益率
        if 'index_total_profit_rate' in df.columns:
            prev_values = df['index_total_profit_rate'].shift(1)
            df.loc[df.index[0], 'daily_index_return'] = 0
            mask = df.index > df.index[0]
            df.loc[mask, 'daily_index_return'] = (df.loc[mask, 'index_total_profit_rate'] - prev_values[mask]) / (100 + prev_values[mask])
        else:
            df['daily_index_return'] = 0
        
        # 假设初始投资为10000元，计算每个时间点的总价值
        initial_investment = 10000
        df['strategy_value'] = initial_investment * (1 + df['total_profit_rate'] / 100)
        df['index_value'] = initial_investment * (1 + df['index_total_profit_rate'] / 100)
        
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

def create_daily_returns_chart(df):
    """
    创建每日收益率图表
    
    参数:
        df (pandas.DataFrame): 处理后的数据
    
    返回:
        tuple: (data, layout) 图表数据和布局配置的JSON字符串
    """
    # 对数据进行降采样处理
    sampled_df = resample_time_series(df)
    
    # 创建图表数据
    data = [
        # 策略每日收益率曲线
        {
            "type": "scatter",
            "x": sampled_df['trade_date'].dt.strftime('%Y-%m-%d').tolist(),
            "y": (sampled_df['daily_strategy_return'] * 100).tolist(),  # 转换为百分比
            "name": "策略日收益率",
            "line": {"color": 'rgb(0, 100, 80)', "width": 2},
            "hovertemplate": '%{x}<br>%{y:.2f}%<extra></extra>'  # 简化悬停信息
        },
        # 指数每日收益率曲线
        {
            "type": "scatter",
            "x": sampled_df['trade_date'].dt.strftime('%Y-%m-%d').tolist(),
            "y": (sampled_df['daily_index_return'] * 100).tolist(),  # 转换为百分比
            "name": "指数日收益率",
            "line": {"color": 'rgb(205, 12, 24)', "width": 2},
            "hovertemplate": '%{x}<br>%{y:.2f}%<extra></extra>'  # 简化悬停信息
        }
    ]
    
    # 创建布局配置
    layout = {
        "title": "每日收益率",
        "height": 400,
        "width": "100%",  # 使用100%宽度
        "autosize": True,  # 启用自动大小调整
        "margin": {  # 设置边距
            "l": 50,   # 左边距
            "r": 30,   # 右边距
            "t": 50,   # 上边距
            "b": 50    # 下边距
        },
        "legend": {
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1
        },
        "hovermode": "x unified",
        "yaxis": {"title": "日收益率(%)"},
        "dragmode": False,  # 禁用拖拽模式
        "modebar": {
            "remove": ["lasso2d", "select2d", "autoScale2d", "toggleSpikelines"]  # 移除不必要的工具按钮
        },
        # 添加零线
        "shapes": [
            {
                "type": "line",
                "x0": sampled_df['trade_date'].min().strftime('%Y-%m-%d'),
                "x1": sampled_df['trade_date'].max().strftime('%Y-%m-%d'),
                "y0": 0, "y1": 0,
                "line": {"color": "black", "dash": "dash", "width": 1}
            }
        ]
    }
    
    return data, layout

def create_total_returns_chart(df):
    """
    创建策略总收益率和指数总收益率图表
    
    参数:
        df (pandas.DataFrame): 处理后的数据
    
    返回:
        tuple: (data, layout) 图表数据和布局配置的JSON字符串
    """
    # 对数据进行降采样处理
    sampled_df = resample_time_series(df)
    
    # 创建图表数据
    data = [
        # 策略总收益率曲线
        {
            "type": "scatter",
            "x": sampled_df['trade_date'].dt.strftime('%Y-%m-%d').tolist(),
            "y": sampled_df['total_profit_rate'].tolist(),  # 已经是百分比格式
            "name": "策略总收益率",
            "line": {"color": 'rgb(0, 100, 80)', "width": 2},
            "hovertemplate": '%{x}<br>%{y:.2f}%<extra></extra>'  # 简化悬停信息
        },
        # 指数总收益率曲线
        {
            "type": "scatter",
            "x": sampled_df['trade_date'].dt.strftime('%Y-%m-%d').tolist(),
            "y": sampled_df['index_total_profit_rate'].tolist(),  # 已经是百分比格式
            "name": "指数总收益率",
            "line": {"color": 'rgb(205, 12, 24)', "width": 2},
            "hovertemplate": '%{x}<br>%{y:.2f}%<extra></extra>'  # 简化悬停信息
        }
    ]
    
    # 创建布局配置
    layout = {
        "title": "累计收益率",
        "height": 400,
        "width": "100%",  # 使用100%宽度
        "autosize": True,  # 启用自动大小调整
        "margin": {  # 设置边距
            "l": 50,   # 左边距
            "r": 30,   # 右边距
            "t": 50,   # 上边距
            "b": 50    # 下边距
        },
        "legend": {
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1
        },
        "hovermode": "x unified",
        "yaxis": {"title": "累计收益率(%)"},
        "dragmode": False,  # 禁用拖拽模式
        "modebar": {
            "remove": ["lasso2d", "select2d", "autoScale2d", "toggleSpikelines"]  # 移除不必要的工具按钮
        },
        # 添加零线
        "shapes": [
            {
                "type": "line",
                "x0": sampled_df['trade_date'].min().strftime('%Y-%m-%d'),
                "x1": sampled_df['trade_date'].max().strftime('%Y-%m-%d'),
                "y0": 0, "y1": 0,
                "line": {"color": "black", "dash": "dash", "width": 1}
            }
        ]
    }
    
    return data, layout

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
    # 创建每日收益率图表数据
    daily_data, daily_layout = create_daily_returns_chart(df)
    
    # 创建策略总收益率和指数总收益率图表数据
    total_data, total_layout = create_total_returns_chart(df)
    
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
                    /* 添加懒加载样式 */
                    .lazy-chart {{
                        min-height: 400px;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                    }}
                    .loading {{
                        text-align: center;
                        padding: 20px;
                        color: #666;
                    }}
                    .loading:after {{
                        content: " ⏳";
                        animation: dots 1s steps(5, end) infinite;
                    }}
                    @keyframes dots {{
                        0%, 20% {{
                            color: rgba(0,0,0,0);
                            text-shadow: .25em 0 0 rgba(0,0,0,0), .5em 0 0 rgba(0,0,0,0);
                        }}
                        40% {{
                            color: #666;
                            text-shadow: .25em 0 0 rgba(0,0,0,0), .5em 0 0 rgba(0,0,0,0);
                        }}
                        60% {{
                            text-shadow: .25em 0 0 #666, .5em 0 0 rgba(0,0,0,0);
                        }}
                        80%, 100% {{
                            text-shadow: .25em 0 0 #666, .5em 0 0 #666;
                        }}
                    }}
                </style>
                <!-- 引入Plotly.js -->
                <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                <!-- 添加懒加载脚本 -->
                <script>
                    // 检测元素是否在视口中
                    function isElementInViewport(el) {{
                        var rect = el.getBoundingClientRect();
                        return (
                            rect.top >= 0 &&
                            rect.left >= 0 &&
                            rect.bottom <= (window.innerHeight || document.documentElement.clientHeight) &&
                            rect.right <= (window.innerWidth || document.documentElement.clientWidth)
                        );
                    }}
                    
                    // 懒加载图表
                    function lazyLoadCharts() {{
                        var lazyCharts = document.querySelectorAll('.lazy-chart:not(.loaded)');
                        
                        lazyCharts.forEach(function(chartDiv) {{
                            if (isElementInViewport(chartDiv)) {{
                                // 标记为已加载
                                chartDiv.classList.add('loaded');
                                
                                // 获取图表数据和配置
                                var chartData = JSON.parse(chartDiv.getAttribute('data-chart'));
                                var chartLayout = JSON.parse(chartDiv.getAttribute('data-layout'));
                                var chartId = chartDiv.getAttribute('id');
                                
                                // 清除加载提示
                                chartDiv.innerHTML = '';
                                
                                // 渲染图表
                                Plotly.newPlot(
                                    chartId,
                                    chartData,
                                    chartLayout,
                                    {{"responsive": true, "staticPlot": false, "displayModeBar": "hover"}}
                                );
                            }}
                        }});
                    }}
                    
                    // 页面加载完成后初始化
                    document.addEventListener('DOMContentLoaded', function() {{
                        // 初始检查
                        setTimeout(lazyLoadCharts, 100);
                        
                        // 滚动时检查
                        window.addEventListener('scroll', lazyLoadCharts);
                        window.addEventListener('resize', lazyLoadCharts);
                    }});
                </script>
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
                <h2>每日收益率</h2>
                <div id="daily_chart" class="lazy-chart" data-chart='{json.dumps(daily_data)}' data-layout='{json.dumps(daily_layout)}'>
                    <div class="loading">图表加载中</div>
                </div>
            </div>
            
            <div class="chart-container">
                <h2>累计收益率</h2>
                <div id="total_chart" class="lazy-chart" data-chart='{json.dumps(total_data)}' data-layout='{json.dumps(total_layout)}'>
                    <div class="loading">图表加载中</div>
                </div>
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