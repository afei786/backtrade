import jqdatasdk
from jqdatasdk import *
# 使用你的聚宽账号和密码登录
jqdatasdk.auth('13625559037', 'Jm123456')

#查询当日剩余可调用数据条数
count=get_query_count()
print(count)

# 获取所有A股股票基本信息
all_stocks = get_all_securities(['stock'])

# 只保留主板（60和00开头）的股票代码
# main_board_stocks = [code for code in all_stocks.index if code.startswith('60') or code.startswith('00')]


print(f"主板股票数量: {len(all_stocks)}")
print(all_stocks[:20])  # 只打印前20个做示例

print(type(all_stocks))

