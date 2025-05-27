import pandas as pd

class HTMLGenerator:
    """
    用于生成回测报表的HTML代码
    """
    
    @staticmethod
    def generate_html_header():
        """
        生成HTML报表的头部
        """
        html_header = '''
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>回测结果分析</title>
            <script src="https://cdn.plot.ly/plotly-2.26.0.min.js"></script>
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
                    color: #cc0000;
                }
                .sell {
                    color: #009900;
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
                    color: #cc0000;
                }
                .negative {
                    color: #009900;
                }
                .chart-container {
                    margin-bottom: 30px;
                }
                .progress-container {
                    width: 100%;
                    background-color: #f1f1f1;
                    border-radius: 5px;
                    margin-bottom: 20px;
                }
                .progress-bar {
                    height: 20px;
                    background-color: #4CAF50;
                    border-radius: 5px;
                    width: 0%;
                    text-align: center;
                    line-height: 20px;
                    color: white;
                }
            </style>
            <script>
                function animateProgressBar() {
                    var elem = document.getElementById("progressBar");
                    var width = 0;
                    var id = setInterval(frame, 30);
                    function frame() {
                        if (width >= 100) {
                            clearInterval(id);
                            document.getElementById("loadingMessage").style.display = "none";
                            document.getElementById("mainContent").style.display = "block";
                        } else {
                            width++;
                            elem.style.width = width + "%";
                            elem.innerHTML = width + "%";
                        }
                    }
                }
                window.onload = function() {
                    animateProgressBar();
                };
            </script>
        </head>
        <body>
            <div class="container">
                <h1>回测结果分析</h1>
                <div id="loadingMessage">
                    <p>正在加载数据和图表，请稍候...</p>
                    <div class="progress-container">
                        <div id="progressBar" class="progress-bar">0%</div>
                    </div>
                </div>
                <div id="mainContent" style="display:none;">
        '''
        return html_header
    
    @staticmethod
    def generate_metrics_html(metrics):
        """
        生成性能指标HTML
        """
        metrics_html = f'''
        <div class="metrics">
            <div class="metric-box">
                <div class="metric-title">策略总收益</div>
                <div class="metric-value {'positive' if metrics['final_strategy_return'] >= 0 else 'negative'}">{metrics['final_strategy_return']:.2f}%</div>
            </div>
            <div class="metric-box">
                <div class="metric-title">指数总收益</div>
                <div class="metric-value {'positive' if metrics['final_index_return'] >= 0 else 'negative'}">{metrics['final_index_return']:.2f}%</div>
            </div>
            <div class="metric-box">
                <div class="metric-title">超额收益</div>
                <div class="metric-value {'positive' if metrics['final_excess_return'] >= 0 else 'negative'}">{metrics['final_excess_return']:.2f}%</div>
            </div>
            <div class="metric-box">
                <div class="metric-title">年化收益率</div>
                <div class="metric-value {'positive' if metrics['annual_strategy_return'] >= 0 else 'negative'}">{metrics['annual_strategy_return']:.2f}%</div>
            </div>
        </div>
        <div class="metrics">
            <div class="metric-box">
                <div class="metric-title">最大回撤</div>
                <div class="metric-value negative">{metrics['max_strategy_drawdown']:.2f}%</div>
            </div>
            <div class="metric-box">
                <div class="metric-title">夏普比率</div>
                <div class="metric-value {'positive' if metrics['sharpe_ratio'] > 0 else 'negative'}">{metrics['sharpe_ratio']:.2f}</div>
            </div>
            <div class="metric-box">
                <div class="metric-title">胜率</div>
                <div class="metric-value {'positive' if metrics['win_rate'] > 50 else 'negative'}">{metrics['win_rate']:.2f}%</div>
            </div>
            <div class="metric-box">
                <div class="metric-title">盈亏比</div>
                <div class="metric-value {'positive' if metrics['win_loss_ratio'] > 1 else 'negative'}">{metrics['win_loss_ratio']:.2f}</div>
            </div>
        </div>
        '''
        return metrics_html
    
    @staticmethod
    def generate_trades_html(trades):
        """
        生成交易记录HTML
        """
        trades_df = pd.DataFrame(trades)
        if trades_df.empty:
            return '<h2>没有交易记录</h2>'
            
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
        return trades_html
    
    @staticmethod
    def generate_html_footer():
        """
        生成HTML报表的尾部
        """
        html_footer = '''
                </div>
            </div>
        </body>
        </html>
        '''
        return html_footer
    
    @staticmethod
    def generate_complete_html(chart_html, metrics, trades):
        """
        生成完整的HTML报表
        """
        html_header = HTMLGenerator.generate_html_header()
        metrics_html = HTMLGenerator.generate_metrics_html(metrics)
        trades_html = HTMLGenerator.generate_trades_html(trades)
        html_footer = HTMLGenerator.generate_html_footer()
        
        return html_header + metrics_html + chart_html + trades_html + html_footer 

