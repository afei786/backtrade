import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Any, Optional, Union

class PySQL:
    def __init__(self, host: str, user: str, password: str, database: str, port: int = 3306):
        """
        初始化数据库连接
        
        参数:
            host: 数据库主机地址
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名称
            port: 数据库端口默认3306
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None
        self.cursor = None
        
    def connect(self) -> None:
        """建立数据库连接"""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port
            )
            if self.connection.is_connected():
                self.cursor = self.connection.cursor(dictionary=True)
                print("数据库连接成功")
        except Error as e:
            print(f"数据库连接失败: {e}")
            raise
            
    def close(self) -> None:
        """关闭数据库连接"""
        if self.connection and self.connection.is_connected():
            if self.cursor:
                self.cursor.close()
            self.connection.close()
            print("数据库连接已关闭")
            
    def execute(self, sql: str, params: Optional[Union[tuple, dict]] = None) -> int:
        """
        执行SQL语句
        
        参数:
            sql: SQL语句
            params: 参数，可以是元组或字典
            
        返回:
            影响的行数
        """
        try:
            if not self.connection or not self.connection.is_connected():
                self.connect()
                
            self.cursor.execute(sql, params)
            self.connection.commit()
            return self.cursor.rowcount
        except Error as e:
            self.connection.rollback()
            print(f"执行SQL失败: {e}")
            raise
            
    def create_table(self, table_name: str, columns: Dict[str, str], primary_key: Optional[str] = None) -> None:
        """
        创建表
        
        参数:
            table_name: 表名
            columns: 列定义字典，格式为 {'列名': '数据类型 约束'}
            primary_key: 主键列名
        """
        column_defs = []
        for col_name, col_def in columns.items():
            column_defs.append(f"`{col_name}` {col_def}")
            
        sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ("
        sql += ", ".join(column_defs)
        
        if primary_key:
            sql += f", PRIMARY KEY (`{primary_key}`)"
            
        sql += ")"
        
        self.execute(sql)
        print(f"表 {table_name} 创建成功")
        
    def insert(self, table_name: str, data: Dict[str, Any]) -> int:
        """
        插入数据
        
        参数:
            table_name: 表名
            data: 要插入的数据字典
            
        返回:
            插入的行数
        """
        columns = ", ".join([f"`{k}`" for k in data.keys()])
        placeholders = ", ".join(["%s"] * len(data))
        sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
        
        affected_rows = self.execute(sql, tuple(data.values()))
        print(f"成功插入 {affected_rows} 行数据到表 {table_name}")
        return affected_rows
        
    def batch_insert(self, table_name: str, data_list: List[Dict[str, Any]]) -> int:
        """
        批量插入数据
        
        参数:
            table_name: 表名
            data_list: 要插入的数据字典列表
            
        返回:
            插入的总行数
        """
        if not data_list:
            return 0
            
        # 先获取所有列名，固定顺序
        columns = list(data_list[0].keys())
        columns_str = ", ".join([f"`{k}`" for k in columns])
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        
        # 按照固定的列顺序提取值
        values = [[data[column] for column in columns] for data in data_list]
        
        try:
            if not self.connection or not self.connection.is_connected():
                self.connect()
                
            self.cursor.executemany(sql, values)
            self.connection.commit()
            affected_rows = self.cursor.rowcount
            print(f"成功批量插入 {affected_rows} 行数据到表 {table_name}")
            return affected_rows
        except Error as e:
            self.connection.rollback()
            print(f"批量插入失败: {e}")
            raise
            
    def select(self, table_name: str, columns: Optional[List[str]] = None, 
               where: Optional[str] = None, params: Optional[Union[tuple, dict]] = None,
               order_by: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        查询数据
        
        参数:
            table_name: 表名
            columns: 要查询的列名列表，None表示查询所有列
            where: WHERE条件语句
            params: WHERE条件参数
            order_by: 排序条件
            limit: 限制返回的行数
            
        返回:
            查询结果列表，每个元素是一个字典表示一行数据
        """
        if columns:
            columns_str = ", ".join([f"`{col}`" for col in columns])
        else:
            columns_str = "*"
            
        sql = f"SELECT {columns_str} FROM `{table_name}`"
        
        if where:
            sql += f" WHERE {where}"
            
        if order_by:
            sql += f" ORDER BY {order_by}"
            
        if limit:
            sql += f" LIMIT {limit}"
            
        try:
            if not self.connection or not self.connection.is_connected():
                self.connect()
                
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            # print(f"成功查询到 {len(results)} 行数据")
            return results
        except Error as e:
            print(f"查询失败: {e}")
            raise
    
    def update(self, table_name: str, data: Dict[str, Any], 
           where: str, params: Optional[Union[tuple, dict]] = None) -> int:
        """
        更新数据
        
        参数:
            table_name: 表名
            data: 要更新的数据字典
            where: WHERE条件语句
            params: WHERE条件参数
            
        返回:
            影响的行数
        """
        set_clause = ", ".join([f"`{k}` = %s" for k in data.keys()])
        sql = f"UPDATE `{table_name}` SET {set_clause} WHERE {where}"
        
        # 处理参数
        if params is None:
            all_params = tuple(data.values())
        elif isinstance(params, dict):
            # 字典参数 - 转换为元组并保持顺序
            all_params = tuple(data.values()) + tuple(params.values())
        else:
            # 元组参数 - 直接合并
            all_params = tuple(data.values()) + (params if isinstance(params, tuple) else (params,))
        
        affected_rows = self.execute(sql, all_params)
        print(f"成功更新 {affected_rows} 行数据")
        return affected_rows
  
    def delete(self, table_name: str, where: str, params: Optional[Union[tuple, dict]] = None) -> int:
        """
        删除数据
        
        参数:
            table_name: 表名
            where: WHERE条件语句
            params: WHERE条件参数
            
        返回:
            影响的行数
        """
        sql = f"DELETE FROM `{table_name}` WHERE {where}"
        affected_rows = self.execute(sql, params)
        print(f"成功删除 {affected_rows} 行数据")
        return affected_rows
        
    def drop_table(self, table_name: str) -> None:
        """
        删除表
        
        参数:
            table_name: 表名
        """
        sql = f"DROP TABLE IF EXISTS `{table_name}`"
        self.execute(sql)
        print(f"表 {table_name} 已删除")
        
    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在
        
        参数:
            table_name: 表名
            
        返回:
            表是否存在
        """
        sql = "SHOW TABLES LIKE %s"
        self.cursor.execute(sql, (table_name,))
        result = self.cursor.fetchone()
        if result:
            print(f"表 {table_name} 存在")
        else:
            print(f"表 {table_name} 不存在")
        return result is not None
        
    def sql_append(self, table_name: str, 
                append_data: Dict[str, Any], 
                where: str, 
                params: Optional[Union[tuple, dict]] = None) -> int:
        """
        Appends values to existing fields in a database table.
        
        Args:
            table_name: Name of the table to update
            append_data: Dictionary of field names and values to append
            where: WHERE clause for the update
            params: Optional parameters for the WHERE clause
            
        Returns:
            Number of affected rows
        """
        if not append_data:
            raise ValueError("append_data cannot be empty")
            
        set_clause = ", ".join([
            f"`{k}` = CASE WHEN `{k}` IS NULL OR `{k}` = '' THEN %s ELSE CONCAT(`{k}`, %s) END"
            for k in append_data.keys()
        ])
        
        # Generate two parameters for each append_data field: original value and comma-prefixed value
        append_params = []
        for v in append_data.values():
            append_params.extend([v, f",{v}"])  # Note the comma
        
        all_params = tuple(append_params) + (params if params else())
        
        sql = f"UPDATE `{table_name}` SET {set_clause} WHERE {where}"
        return self.execute(sql, all_params)
    
    def sql_remove(self, table_name: str, 
               remove_data: Dict[str, Any], 
               where: str, 
               params: Optional[Union[tuple, dict]] = None) -> int:
        """
        Removes specified values from fields in a database table.
        
        Args:
            table_name: Name of the table to update
            remove_data: Dictionary of field names and values to remove
            where: WHERE clause for the update
            params: Optional parameters for the WHERE clause
            
        Returns:
            Number of affected rows
        
        Example:
            sql_remove("users", {"tags": "premium"}, "id = %s", (1,))
            This would remove "premium" from the "tags" field for user with id=1
        """
        if not remove_data:
            raise ValueError("remove_data cannot be empty")
            
        set_clauses = []
        remove_params = []
        
        for field, value in remove_data.items():
            # Handle both comma-separated values and direct matches
            set_clauses.append(
                f"`{field}` = CASE "
                f"WHEN `{field}` = %s THEN '' "  # Exact match
                f"WHEN `{field}` LIKE %s THEN TRIM(BOTH ',' FROM REPLACE(CONCAT(',', `{field}`, ','), CONCAT(',', %s, ','), ',')) "  # Remove from CSV
                f"WHEN `{field}` LIKE %s THEN TRIM(BOTH ',' FROM REPLACE(CONCAT(',', `{field}`, ','), CONCAT(',', %s, ','), ',')) "  # Remove first in CSV
                f"WHEN `{field}` LIKE %s THEN TRIM(BOTH ',' FROM REPLACE(CONCAT(',', `{field}`, ','), CONCAT(',', %s, ','), ',')) "  # Remove last in CSV
                f"ELSE `{field}` END"
            )
            
            # Add parameters for all cases
            remove_params.extend([
                value,  # Exact match
                f"%,{value},%", value,  # Middle of CSV
                f"{value},%", value,  # Start of CSV
                f"%,{value}", value   # End of CSV
            ])
        
        all_params = tuple(remove_params) + (params if params else ())
        set_clause = ", ".join(set_clauses)
        
        sql = f"UPDATE `{table_name}` SET {set_clause} WHERE {where}"
        return self.execute(sql, all_params)
    
    def batch_update(self, table_name, data: List[Dict[str, Any]], key_fields: List[str] = None) -> int:
        """
        批量根据主键（可多个字段）更新多字段数据到指定表中
        data: 每个元素为{'id': ..., 'trade_date': ..., 'ma5': ..., ...}
        key_fields: 主键字段名列表，如 ['id', 'trade_date']
        """
        if not data:
            return 0
        if key_fields is None:
            key_fields = ['stock_code']  # 默认主键

        # 所有字段
        all_keys = set()
        for row in data:
            all_keys.update(row.keys())
        for k in key_fields:
            all_keys.discard(k)
        fields = list(all_keys)
        if not fields:
            return 0

        set_clause = ", ".join([f"`{field}` = %s" for field in fields])
        where_clause = " AND ".join([f"`{k}` = %s" for k in key_fields])
        sql = f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause}"

        values = []
        for row in data:
            if not all(k in row for k in key_fields):
                continue
            row_values = [row.get(field) for field in fields] + [row[k] for k in key_fields]
            values.append(tuple(row_values))

        try:
            if not self.connection or not self.connection.is_connected():
                self.connect()
            self.cursor.executemany(sql, values)
            self.connection.commit()
            affected_rows = self.cursor.rowcount
            print(f"成功批量更新 {affected_rows} 行数据到表 {table_name}")
            return affected_rows
        except Error as e:
            self.connection.rollback()
            print(f"批量更新数据失败: {e}")
            raise

        
    def __enter__(self):
        """支持上下文管理协议"""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持上下文管理协议"""
        self.close()


    def commit(self):
        """提交事务"""
        self.connection.commit()

if __name__ =="__main__":
    # pysql = PySQL(host="192.168.1.149", user="user", password="123456", database="crrc_alstom")
    # pysql = PySQL(host='127.0.0.1', user='afei', password='sf123456', database='stock')
    # pysql.connect()
    user_sql = PySQL(
        host='localhost',
        user='afei',
        password='sf123456',
        database='stock',
        port=3306
    )
    user_sql.connect()
    region = user_sql.select('stock_info', columns=['region'])  # 获取所有非ST股票
    region = [item['region'] for item in region]
    region = list(set(region))  # 去重
    print(len(region))
    # 创建表
    # columns = {
    #     'id': 'INT AUTO_INCREMENT',
    #     'name': 'VARCHAR(100) NOT NULL',
    #     'age': 'INT NOT NULL',
    #     'email': 'VARCHAR(100)'
    # }
    # pysql.create_table('users1', columns, primary_key='id')
    # 增
    # data = {
    #     'id': 123,
    #     'name': 'John Doe',
    #     'age': 30,
    #     'email': '@163.com'
    # }
    # pysql.insert('users1', data)
    # 查
    # results = pysql.select('users1')
    # for row in results:
    #     print(row)
    # 改
    # update_data = {
    #     'name': 'Jane Doe',
    #     'age': 25
    # }
    # pysql.update('users1', update_data, 'id = %s', (123,))
    # 删除数据
    # pysql.delete('users1', 'id = %s', (123,))

    # 查
    # t2 = time.time()
    # # results = pysql.select('device_list',)
    # results = pysql.select("orders", where="id = %s", params=("GD0000092",))
    # print(results[0])
    # pysql.delete('project_list', f"name = test")


    # print("查询用时：", time.time() - t2)
    # for row in results:
    #     print(row)
    
    # 按照类型查询
    # res = pysql.select('orders')


    # res = pysql.select('_list')
    # for row in res:
    #     print(row)
    # pysql.table_exists('users1')
    # 插入数据
    # data = [{
    #     'id': 1,
    #     'trade_date': '2023-01-05',
    #     'ma5': 11.0,
    #     'ma10': 10.0,
    #     'ma20': 10.0,
    #     },
    #     {'id': 1,
    #     'trade_date': '2023-01-07',
    #     'ma5': 10.0,
    #     'ma10': 12.0,
    #     'ma20': 10.0,
    #     }]
    # # 假设你的主键是 id 和 trade_date
    # pysql.batch_update('stock_ma', data, key_fields=['id', 'trade_date'])

    # pysql.close()