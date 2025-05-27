import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

class ChartGenerator:
    """
    用于生成回测报表中的图表
    """
    
    @staticmethod
    def create_cumulative_return_chart(df):
        """
        创建累积收益率对比图和超额收益图
        """
        print("生成累积收益率图表... [40%]")
        fig = make_subplots(
            rows=2, cols=1, 
            shared_xaxes=True,
            subplot_titles=('累积收益率对比', '超额收益率'),
            vertical_spacing=0.12,
            row_heights=[0.7, 0.3]
        )
        
        # 添加累积收益率曲线 - 直接使用日志中记录的收益率
        fig.add_trace(
            go.Scatter(
                x=df['date'], 
                y=df['strategy_returns'],  # 直接使用原始收益率数据
                name='策略收益',
                line=dict(color='rgb(214, 39, 40)', width=2)  # 红色
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=df['date'], 
                y=df['index_returns'],  # 直接使用原始指数收益率数据
                name='指数收益',
                line=dict(color='rgb(44, 160, 44)', width=2)  # 绿色
            ),
            row=1, col=1
        )
        
        # 添加超额收益率
        fig.add_trace(
            go.Bar(
                x=df['date'], 
                y=df['excess_returns'],
                name='超额收益',
                marker_color='rgba(214, 39, 40, 0.5)'  # 红色
            ),
            row=2, col=1
        )
        
        # 更新布局
        fig.update_layout(
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
        
        fig.update_yaxes(title_text='收益率(%)', row=1, col=1)
        fig.update_yaxes(title_text='超额收益(%)', row=2, col=1)
        
        return fig
    
    @staticmethod
    def create_daily_return_chart(df):
        """
        创建每日收益率图表
        """
        print("生成每日收益率图表... [60%]")
        fig = go.Figure()
        
        fig.add_trace(
            go.Scatter(
                x=df['date'], 
                y=df['daily_return_pct'],
                name='策略日收益率',
                line=dict(color='rgb(214, 39, 40)', width=2)  # 红色
            )
        )
        
        # 添加指数日收益率，如果有数据的话
        if 'daily_index_return' in df.columns and not all(pd.isna(df['daily_index_return'])):
            fig.add_trace(
                go.Scatter(
                    x=df['date'], 
                    y=df['daily_index_return'],
                    name='指数日收益率',
                    line=dict(color='rgb(44, 160, 44)', width=2)  # 绿色
                )
            )
        
        # 添加零线
        fig.add_shape(
            type="line",
            x0=df['date'].min(),
            y0=0,
            x1=df['date'].max(),
            y1=0,
            line=dict(color="black", width=1, dash="dash")
        )
        
        fig.update_layout(
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
        
        return fig
    
    @staticmethod
    def create_asset_chart(df):
        """
        创建资金曲线图表
        """
        print("生成资金曲线图表... [80%]")
        if 'total_assets' not in df.columns:
            return None
            
        fig = make_subplots(
            rows=1, cols=1,
            specs=[[{"secondary_y": True}]]
        )
        
        # 添加总资产曲线
        fig.add_trace(
            go.Scatter(
                x=df['date'], 
                y=df['total_assets'],
                name='总资产',
                line=dict(color='rgb(31, 119, 180)', width=2)
            )
        )
        
        # 添加现金曲线
        fig.add_trace(
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
        fig.add_trace(
            go.Scatter(
                x=df['date'], 
                y=df['market_value'],
                name='持仓市值',
                line=dict(color='rgb(44, 160, 44)', width=2, dash='dot'),  # 绿色
                fill='tozeroy',
                fillcolor='rgba(44, 160, 44, 0.1)'
            )
        )
        
        # 在右轴添加仓位比例
        if 'position_ratio' in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df['date'], 
                    y=df['position_ratio'],
                    name='仓位比例(%)',
                    line=dict(color='rgb(214, 39, 40)', width=2)  # 红色
                ),
                secondary_y=True
            )
        
        fig.update_layout(
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
        
        fig.update_yaxes(title_text='金额', secondary_y=False)
        fig.update_yaxes(title_text='仓位比例(%)', secondary_y=True)
        
        return fig
    
    @staticmethod
    def create_drawdown_chart(df):
        """
        创建回撤分析图表
        """
        print("生成回撤分析图表... [90%]")
        fig = go.Figure()
        
        # 添加策略回撤曲线
        if 'strategy_drawdown' in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df['date'], 
                    y=df['strategy_drawdown'],
                    name='策略回撤',
                    line=dict(color='rgb(214, 39, 40)', width=2),  # 红色
                    fill='tozeroy',
                    fillcolor='rgba(214, 39, 40, 0.1)'
                )
            )
        
        # 添加指数回撤曲线
        if 'index_drawdown' in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df['date'], 
                    y=df['index_drawdown'],
                    name='指数回撤',
                    line=dict(color='rgb(44, 160, 44)', width=2),  # 绿色
                    fill='tozeroy',
                    fillcolor='rgba(44, 160, 44, 0.1)'
                )
            )
        
        fig.update_layout(
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
        
        return fig
    
    @staticmethod
    def prepare_data(data):
        """
        准备用于图表的数据
        """
        print("处理数据中... [20%]")
        # 将数据转换为DataFrame
        df = pd.DataFrame({
            'date': pd.to_datetime(data['dates']),
            'strategy_returns': data['strategy_returns'],
            'index_returns': data['index_returns'],
            'excess_returns': data['excess_returns']
        })
        
        # 如果有资金数据，添加到DataFrame
        if len(data['cash']) == len(data['dates']):
            df['cash'] = data['cash']
            df['market_value'] = data['market_value']
            df['total_assets'] = data['total_assets']
            df['daily_profit'] = data['daily_profit']
            df['cumulative_profit'] = data['cumulative_profit']
        
        # 确保数据按日期排序
        df = df.sort_values('date')
        
        # 直接使用日志中提取的收益率数据，不再进行计算
        # 这确保图表显示的收益率与日志文件中的收益率完全一致
        df['cumulative_strategy'] = df['strategy_returns'] / 100
        df['cumulative_index'] = df['index_returns'] / 100
        
        # 计算每日收益率变化
        if 'daily_profit' in df.columns and 'total_assets' in df.columns:
            # 使用当日盈亏除以前一日总资产计算日收益率
            # 对于第一天，使用当日盈亏除以（总资产-当日盈亏）
            first_day_assets = df.iloc[0]['total_assets'] - df.iloc[0]['daily_profit']
            previous_assets = pd.Series([first_day_assets] + df['total_assets'].tolist()[:-1]).values
            df['daily_return_pct'] = (df['daily_profit'] / previous_assets) * 100
            df['daily_return_pct'] = df['daily_return_pct'].fillna(0)
        else:
            # 如果没有足够的资金数据，则使用收益率差分
            df['daily_return_pct'] = df['strategy_returns'].diff().fillna(df['strategy_returns'].iloc[0])
        
        # 添加指数日收益率，如果有数据的话
        if not all(v == 0 for v in df['index_returns']):
            df['daily_index_return'] = df['index_returns'].diff().fillna(df['index_returns'].iloc[0])
        
        # 计算仓位比例
        if 'market_value' in df.columns and 'total_assets' in df.columns:
            df['position_ratio'] = (df['market_value'] / df['total_assets']) * 100
        
        # 计算历史新高点和回撤
        if 'total_assets' in df.columns:
            # 使用总资产计算回撤
            df['strategy_cummax'] = df['total_assets'].cummax()
            df['strategy_drawdown'] = (df['total_assets'] / df['strategy_cummax'] - 1) * 100
        else:
            # 使用日志中的收益率计算回撤
            # 注意：这里使用(1 + 收益率/100)来计算累积收益
            df['strategy_cumulative_return'] = (1 + df['strategy_returns']/100)
            df['strategy_cummax'] = df['strategy_cumulative_return'].cummax()
            df['strategy_drawdown'] = (df['strategy_cumulative_return'] / df['strategy_cummax'] - 1) * 100
        
        # 如果有指数数据，计算指数回撤
        if not all(v == 0 for v in df['index_returns']):
            # 使用(1 + 收益率/100)来计算累积收益
            df['index_cumulative_return'] = (1 + df['index_returns']/100)
            df['index_cummax'] = df['index_cumulative_return'].cummax()
            df['index_drawdown'] = (df['index_cumulative_return'] / df['index_cummax'] - 1) * 100
        
        return df
    
    @staticmethod
    def generate_chart_html(fig1, fig2, fig3, fig4):
        """
        生成包含所有图表的HTML代码
        """
        chart_html = f'''
        <div class="chart-container">
            <h2>累积收益与超额收益</h2>
            <div id="fig1" style="width:100%; height:500px;"></div>
        </div>
        
        <div class="chart-container">
            <h2>每日收益率</h2>
            <div id="fig2" style="width:100%; height:400px;"></div>
        </div>
        '''
        
        # 添加资金曲线图(如果有数据)
        if fig3 is not None:
            chart_html += '''
            <div class="chart-container">
                <h2>资金曲线与仓位分析</h2>
                <div id="fig3" style="width:100%; height:400px;"></div>
            </div>
            '''
        
        # 添加回撤分析图
        chart_html += '''
        <div class="chart-container">
            <h2>回撤分析</h2>
            <div id="fig4" style="width:100%; height:400px;"></div>
        </div>
        
        <script>
            // 将图表数据直接嵌入到HTML中
            var fig1_data = {0};
            var fig1_layout = {1};
            var fig2_data = {2};
            var fig2_layout = {3};
            var fig4_data = {4};
            var fig4_layout = {5};
            
            // 创建图表
            Plotly.newPlot('fig1', fig1_data, fig1_layout);
            Plotly.newPlot('fig2', fig2_data, fig2_layout);
            Plotly.newPlot('fig4', fig4_data, fig4_layout);
        '''
        
        # 格式化图表数据
        chart_html = chart_html.format(
            fig1.to_json().split('"data":')[1].split(',"layout"')[0],
            fig1.layout.to_json(),
            fig2.to_json().split('"data":')[1].split(',"layout"')[0],
            fig2.layout.to_json(),
            fig4.to_json().split('"data":')[1].split(',"layout"')[0],
            fig4.layout.to_json()
        )
        
        # 如果有fig3，添加它的渲染代码
        if fig3 is not None:
            fig3_script = '''
            var fig3_data = {0};
            var fig3_layout = {1};
            Plotly.newPlot('fig3', fig3_data, fig3_layout);
            '''
            
            fig3_script = fig3_script.format(
                fig3.to_json().split('"data":')[1].split(',"layout"')[0],
                fig3.layout.to_json()
            )
            
            chart_html += fig3_script
        
        chart_html += '''
        </script>
        '''
        
        return chart_html
    
    @staticmethod
    def calculate_performance_metrics(df):
        """
        计算性能指标
        """
        print("计算性能指标... [95%]")
        metrics = {}
        
        # 年化收益率
        days = (df['date'].max() - df['date'].min()).days
        if days > 0:
            metrics['annual_strategy_return'] = ((1 + df['strategy_returns'].iloc[-1]/100) ** (365 / days) - 1) * 100
            metrics['annual_index_return'] = ((1 + df['index_returns'].iloc[-1]/100) ** (365 / days) - 1) * 100
        else:
            metrics['annual_strategy_return'] = df['strategy_returns'].iloc[-1]
            metrics['annual_index_return'] = df['index_returns'].iloc[-1]
        
        # 最大回撤
        metrics['max_strategy_drawdown'] = df['strategy_drawdown'].min() if not df['strategy_drawdown'].empty else 0
        metrics['max_index_drawdown'] = df['index_drawdown'].min() if 'index_drawdown' in df.columns and not df['index_drawdown'].empty else 0
        
        # 夏普比率 (假设无风险利率为3%)
        risk_free_rate = 3
        daily_risk_free = (1 + risk_free_rate/100) ** (1/365) - 1
        
        # 计算超额日收益率
        if 'daily_return_pct' in df.columns:
            df['excess_daily_return'] = df['daily_return_pct'] / 100 - daily_risk_free
            
            # 计算夏普比率
            if len(df) > 1:
                excess_returns_mean = df['excess_daily_return'].mean() * 365
                excess_returns_std = df['excess_daily_return'].std() * np.sqrt(365)
                if excess_returns_std != 0:
                    metrics['sharpe_ratio'] = excess_returns_mean / excess_returns_std
                else:
                    metrics['sharpe_ratio'] = 0
            else:
                metrics['sharpe_ratio'] = 0
        else:
            metrics['sharpe_ratio'] = 0
        
        # 盈亏比
        if 'daily_return_pct' in df.columns:
            winning_days = df[df['daily_return_pct'] > 0]
            losing_days = df[df['daily_return_pct'] < 0]
            
            if len(winning_days) > 0 and len(losing_days) > 0:
                avg_win = winning_days['daily_return_pct'].mean()
                avg_loss = abs(losing_days['daily_return_pct'].mean())
                metrics['win_loss_ratio'] = avg_win / avg_loss if avg_loss != 0 else float('inf')
                metrics['win_rate'] = len(winning_days) / len(df) * 100
            else:
                metrics['win_loss_ratio'] = 0
                metrics['win_rate'] = 0 if len(df) == 0 else (len(winning_days) / len(df) * 100)
        else:
            metrics['win_loss_ratio'] = 0
            metrics['win_rate'] = 0
        
        # 最终收益率
        metrics['final_strategy_return'] = df['strategy_returns'].iloc[-1]
        metrics['final_index_return'] = df['index_returns'].iloc[-1]
        metrics['final_excess_return'] = metrics['final_strategy_return'] - metrics['final_index_return']
        
        return metrics 


