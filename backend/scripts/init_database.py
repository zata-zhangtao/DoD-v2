"""
数据库初始化脚本
创建 SQLite 数据库并填充分析类型的测试数据
"""
import sqlite3
import os
import random
from datetime import datetime, timedelta


def create_analytics_database(db_path: str = None):
    """
    创建包含分析数据的 SQLite 数据库

    Args:
        db_path: 数据库文件路径，默认为 data/analytics.db
    """
    if db_path is None:
        # 获取脚本所在目录的父目录
        script_dir = os.path.dirname(os.path.abspath(__file__))
        backend_dir = os.path.dirname(script_dir)
        data_dir = os.path.join(backend_dir, "data")

        # 确保 data 目录存在
        os.makedirs(data_dir, exist_ok=True)

        db_path = os.path.join(data_dir, "analytics.db")

    print(f"正在创建数据库: {db_path}")

    # 如果数据库已存在，先删除
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"  已删除旧数据库")

    # 创建数据库连接
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # ========== 1. 创建销售数据表 ==========
    print("\n创建销售数据表 (sales_data)...")
    cursor.execute("""
        CREATE TABLE sales_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            product_category TEXT NOT NULL,
            product_name TEXT NOT NULL,
            region TEXT NOT NULL,
            sales_amount REAL NOT NULL,
            quantity INTEGER NOT NULL,
            discount_rate REAL DEFAULT 0,
            profit REAL NOT NULL
        )
    """)

    # 生成销售数据
    categories = ['电子产品', '家居用品', '服装配饰', '食品饮料', '图书文具']
    products = {
        '电子产品': ['笔记本电脑', '智能手机', '平板电脑', '耳机', '智能手表'],
        '家居用品': ['沙发', '床垫', '台灯', '收纳箱', '餐具套装'],
        '服装配饰': ['T恤', '牛仔裤', '运动鞋', '背包', '帽子'],
        '食品饮料': ['零食礼包', '茶叶', '咖啡', '坚果', '饮料'],
        '图书文具': ['笔记本', '钢笔', '书籍', '文件夹', '便签']
    }
    regions = ['华东', '华北', '华南', '西南', '东北']

    # 生成最近180天的数据
    start_date = datetime.now() - timedelta(days=180)
    sales_records = []

    for day in range(180):
        current_date = start_date + timedelta(days=day)
        date_str = current_date.strftime("%Y-%m-%d")

        # 每天生成 5-15 条销售记录
        num_records = random.randint(5, 15)

        for _ in range(num_records):
            category = random.choice(categories)
            product = random.choice(products[category])
            region = random.choice(regions)

            # 根据产品类型设置价格范围
            if category == '电子产品':
                base_price = random.uniform(500, 8000)
            elif category == '家居用品':
                base_price = random.uniform(100, 3000)
            elif category == '服装配饰':
                base_price = random.uniform(50, 800)
            elif category == '食品饮料':
                base_price = random.uniform(20, 300)
            else:  # 图书文具
                base_price = random.uniform(10, 200)

            quantity = random.randint(1, 20)
            discount_rate = random.choice([0, 0.05, 0.1, 0.15, 0.2])
            sales_amount = base_price * quantity * (1 - discount_rate)
            profit = sales_amount * random.uniform(0.1, 0.4)  # 利润率 10-40%

            sales_records.append((
                date_str, category, product, region,
                round(sales_amount, 2), quantity,
                discount_rate, round(profit, 2)
            ))

    cursor.executemany("""
        INSERT INTO sales_data (date, product_category, product_name, region,
                               sales_amount, quantity, discount_rate, profit)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, sales_records)

    print(f"  ✓ 已插入 {len(sales_records)} 条销售记录")

    # ========== 2. 创建用户指标表 ==========
    print("\n创建用户指标表 (user_metrics)...")
    cursor.execute("""
        CREATE TABLE user_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            platform TEXT NOT NULL,
            daily_active_users INTEGER NOT NULL,
            new_users INTEGER NOT NULL,
            retention_rate_7d REAL,
            retention_rate_30d REAL,
            avg_session_duration REAL,
            conversion_rate REAL,
            churn_rate REAL
        )
    """)

    # 生成用户指标数据
    platforms = ['iOS', 'Android', 'Web']
    user_metrics_records = []

    for day in range(180):
        current_date = start_date + timedelta(days=day)
        date_str = current_date.strftime("%Y-%m-%d")

        for platform in platforms:
            # 基础用户数（随时间增长）
            base_dau = 10000 + day * 50
            if platform == 'Android':
                dau_multiplier = 1.5
            elif platform == 'iOS':
                dau_multiplier = 1.0
            else:  # Web
                dau_multiplier = 0.8

            dau = int(base_dau * dau_multiplier * random.uniform(0.9, 1.1))
            new_users = int(dau * random.uniform(0.02, 0.08))
            retention_7d = random.uniform(0.35, 0.55)
            retention_30d = random.uniform(0.15, 0.30)
            avg_session = random.uniform(5, 25)  # 分钟
            conversion = random.uniform(0.02, 0.08)
            churn = random.uniform(0.01, 0.05)

            user_metrics_records.append((
                date_str, platform, dau, new_users,
                round(retention_7d, 4), round(retention_30d, 4),
                round(avg_session, 2), round(conversion, 4),
                round(churn, 4)
            ))

    cursor.executemany("""
        INSERT INTO user_metrics (date, platform, daily_active_users, new_users,
                                 retention_rate_7d, retention_rate_30d,
                                 avg_session_duration, conversion_rate, churn_rate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, user_metrics_records)

    print(f"  ✓ 已插入 {len(user_metrics_records)} 条用户指标记录")

    # ========== 3. 创建性能统计表 ==========
    print("\n创建性能统计表 (performance_stats)...")
    cursor.execute("""
        CREATE TABLE performance_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            service_name TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            response_time_ms REAL NOT NULL,
            error_count INTEGER DEFAULT 0,
            success_count INTEGER NOT NULL,
            cpu_usage REAL,
            memory_usage REAL,
            qps REAL
        )
    """)

    # 生成性能统计数据（每小时一条）
    services = ['api-gateway', 'user-service', 'order-service', 'payment-service']
    endpoints = {
        'api-gateway': ['/health', '/api/v1/users', '/api/v1/orders', '/api/v1/products'],
        'user-service': ['/login', '/register', '/profile', '/logout'],
        'order-service': ['/create', '/list', '/detail', '/cancel'],
        'payment-service': ['/pay', '/refund', '/query', '/callback']
    }

    performance_records = []
    hours_to_generate = 180 * 24  # 180天 * 24小时

    for hour in range(hours_to_generate):
        timestamp = start_date + timedelta(hours=hour)
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        for service in services:
            for endpoint in endpoints[service]:
                # 模拟响应时间（有些端点更慢）
                if 'payment' in service or 'order' in endpoint:
                    base_response = random.uniform(100, 500)
                else:
                    base_response = random.uniform(20, 200)

                # 偶尔有性能问题
                if random.random() < 0.05:
                    response_time = base_response * random.uniform(2, 5)
                else:
                    response_time = base_response

                success_count = random.randint(100, 1000)
                error_count = int(success_count * random.uniform(0, 0.05))
                cpu_usage = random.uniform(20, 80)
                memory_usage = random.uniform(30, 85)
                qps = random.uniform(10, 200)

                performance_records.append((
                    timestamp_str, service, endpoint,
                    round(response_time, 2), error_count, success_count,
                    round(cpu_usage, 2), round(memory_usage, 2),
                    round(qps, 2)
                ))

    cursor.executemany("""
        INSERT INTO performance_stats (timestamp, service_name, endpoint,
                                      response_time_ms, error_count, success_count,
                                      cpu_usage, memory_usage, qps)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, performance_records)

    print(f"  ✓ 已插入 {len(performance_records)} 条性能统计记录")

    # 提交并关闭
    conn.commit()

    # 显示数据库摘要
    print("\n" + "=" * 60)
    print("数据库创建完成！")
    print("=" * 60)

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    print(f"\n数据库路径: {db_path}")
    print(f"包含 {len(tables)} 张表:")

    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"  - {table_name}: {count} 条记录")

    cursor.close()
    conn.close()

    return db_path


if __name__ == "__main__":
    print("=" * 60)
    print("SQLite 分析数据库初始化")
    print("=" * 60)

    db_path = create_analytics_database()

    print("\n✓ 数据库初始化完成！")
    print(f"\n可以使用以下代码查询数据库:")
    print(f"  from app.utils.sql_executor import execute_sql_safely")
    print(f"  result = execute_sql_safely('SELECT * FROM sales_data LIMIT 10', '{db_path}')")
