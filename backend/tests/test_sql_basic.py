"""
快速测试 SQL 功能
"""
import os
from app.utils.sql_executor import get_db_schema_info, execute_sql_safely

# 获取数据库路径
db_path = os.path.join(os.path.dirname(__file__), "data", "analytics.db")

print("=" * 80)
print("测试1: 读取数据库结构")
print("=" * 80)

db_info = get_db_schema_info(db_path)

if db_info.get("error"):
    print(f"✗ 错误: {db_info['error']}")
else:
    print(f"✓ 成功读取数据库")
    print(f"  表数量: {db_info['table_count']}")
    for table in db_info["tables"]:
        print(f"  - {table['name']}: {table['row_count']} 行")

print("\n" + "=" * 80)
print("测试2: 执行简单 SQL 查询")
print("=" * 80)

# 测试查询
test_sql = "SELECT * FROM sales_data LIMIT 5"
print(f"SQL: {test_sql}\n")

result = execute_sql_safely(test_sql, db_path)

if result["success"]:
    print(f"✓ 查询成功")
    print(f"  返回行数: {result['row_count']}")
    print(f"  列: {', '.join(result['columns'])}")
    print("\n前3行数据:")
    for i, row in enumerate(result["data"][:3], 1):
        print(f"  {i}. {row}")
else:
    print(f"✗ 查询失败: {result['error']}")

print("\n" + "=" * 80)
print("测试3: SQL 安全性验证")
print("=" * 80)

from app.utils.sql_executor import validate_sql_safety

# 测试危险 SQL
dangerous_sqls = [
    "DROP TABLE sales_data",
    "DELETE FROM sales_data WHERE id=1",
    "SELECT * FROM sales_data; DROP TABLE sales_data",
]

for sql in dangerous_sqls:
    is_safe, error = validate_sql_safety(sql)
    status = "✓ 已拦截" if not is_safe else "✗ 未拦截"
    print(f"{status}: {sql[:50]}... -> {error}")

# 测试安全 SQL
safe_sql = "SELECT COUNT(*) FROM sales_data WHERE region='华东'"
is_safe, error = validate_sql_safety(safe_sql)
status = "✓ 允许" if is_safe else "✗ 错误拦截"
print(f"{status}: {safe_sql}")

print("\n" + "=" * 80)
print("基本功能测试完成")
print("=" * 80)
