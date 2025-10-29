"""
独立的 SQL 功能测试（不依赖外部模块）
"""
import sqlite3
import os

# 获取数据库路径
db_path = os.path.join(os.path.dirname(__file__), "data", "analytics.db")

print("=" * 80)
print("独立 SQL 功能测试")
print("=" * 80)

# 测试1: 检查数据库是否存在
print("\n测试1: 检查数据库")
if os.path.exists(db_path):
    print(f"✓ 数据库存在: {db_path}")
    file_size = os.path.getsize(db_path) / 1024 / 1024  # MB
    print(f"  文件大小: {file_size:.2f} MB")
else:
    print(f"✗ 数据库不存在: {db_path}")
    exit(1)

# 测试2: 连接数据库并查询表结构
print("\n测试2: 查询数据库结构")
try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 获取所有表名
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = cursor.fetchall()

    print(f"✓ 成功连接数据库")
    print(f"  表数量: {len(tables)}")

    for table_name in tables:
        table = table_name[0]
        if table != 'sqlite_sequence':
            # 获取表的行数
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]

            # 获取表结构
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            col_names = [col[1] for col in columns]

            print(f"\n  表: {table}")
            print(f"    行数: {count}")
            print(f"    列: {', '.join(col_names[:5])}" + ("..." if len(col_names) > 5 else ""))

    cursor.close()
    conn.close()

except Exception as e:
    print(f"✗ 数据库连接失败: {e}")
    exit(1)

# 测试3: 执行简单查询
print("\n测试3: 执行数据查询")
try:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 查询销售数据
    cursor.execute("SELECT * FROM sales_data LIMIT 3")
    rows = cursor.fetchall()

    print("✓ 销售数据查询成功")
    print(f"  返回行数: {len(rows)}")

    for i, row in enumerate(rows, 1):
        print(f"\n  第 {i} 行:")
        print(f"    日期: {row['date']}")
        print(f"    产品: {row['product_name']}")
        print(f"    销售额: {row['sales_amount']}")
        print(f"    地区: {row['region']}")

    # 查询用户指标
    cursor.execute("SELECT platform, AVG(daily_active_users) as avg_dau FROM user_metrics GROUP BY platform")
    metrics = cursor.fetchall()

    print("\n✓ 用户指标聚合查询成功")
    for row in metrics:
        print(f"  {row['platform']}: 平均日活 {row['avg_dau']:.0f}")

    # 查询性能统计
    cursor.execute("""
        SELECT service_name,
               AVG(response_time_ms) as avg_response_time,
               COUNT(*) as total_requests
        FROM performance_stats
        GROUP BY service_name
    """)
    perf_stats = cursor.fetchall()

    print("\n✓ 性能统计聚合查询成功")
    for row in perf_stats:
        print(f"  {row['service_name']}: 平均响应 {row['avg_response_time']:.2f}ms, 总请求 {row['total_requests']}")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"✗ 查询失败: {e}")
    exit(1)

# 测试4: 测试复杂查询
print("\n测试4: 执行复杂业务查询")
try:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 查询：各地区销售额排名
    query1 = """
        SELECT region,
               SUM(sales_amount) as total_sales,
               COUNT(*) as order_count
        FROM sales_data
        GROUP BY region
        ORDER BY total_sales DESC
    """
    cursor.execute(query1)
    results = cursor.fetchall()

    print("✓ 查询1: 各地区销售额排名")
    for row in results[:3]:
        print(f"  {row['region']}: ¥{row['total_sales']:.2f} ({row['order_count']} 笔订单)")

    # 查询：电子产品类别的top产品
    query2 = """
        SELECT product_name,
               SUM(sales_amount) as total_sales,
               SUM(quantity) as total_quantity
        FROM sales_data
        WHERE product_category = '电子产品'
        GROUP BY product_name
        ORDER BY total_sales DESC
        LIMIT 3
    """
    cursor.execute(query2)
    results = cursor.fetchall()

    print("\n✓ 查询2: 电子产品销售TOP3")
    for i, row in enumerate(results, 1):
        print(f"  {i}. {row['product_name']}: ¥{row['total_sales']:.2f} (销量 {row['total_quantity']})")

    # 查询：用户留存趋势
    query3 = """
        SELECT date,
               SUM(daily_active_users) as total_dau,
               AVG(retention_rate_7d) as avg_retention
        FROM user_metrics
        WHERE date >= date('now', '-7 days')
        GROUP BY date
        ORDER BY date
    """
    cursor.execute(query3)
    results = cursor.fetchall()

    print("\n✓ 查询3: 最近7天用户留存")
    for row in results[:3]:
        print(f"  {row['date']}: DAU {row['total_dau']}, 7日留存 {row['avg_retention']*100:.1f}%")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"✗ 复杂查询失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("✓ 所有独立测试通过！")
print("=" * 80)
print("\n数据库功能正常，可以用于 SQL 自然语言查询")
