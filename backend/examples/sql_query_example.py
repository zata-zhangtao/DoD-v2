"""
SQL 自然语言查询示例
演示如何使用自然语言查询 SQLite 数据库
"""
import os
import sys

# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from app.graphs.code_analysis_graph import run_sql_analysis, run_multi_query_analysis


def example_single_query():
    """
    示例1：单个查询
    """
    print("\n" + "=" * 80)
    print("示例1：单个自然语言查询")
    print("=" * 80)

    # 数据库路径
    db_path = os.path.join(project_root, "data", "analytics.db")

    if not os.path.exists(db_path):
        print(f"错误：数据库文件不存在: {db_path}")
        print("请先运行 scripts/init_database.py 创建数据库")
        return

    # 自然语言查询
    natural_query = "查询过去30天销售额最高的前10个产品"

    # 运行查询
    result = run_sql_analysis(db_path, natural_query)

    # 显示结果
    print("\n" + "=" * 80)
    print("查询结果总结")
    print("=" * 80)

    if result.get("error"):
        print(f"✗ 查询失败: {result['error']}")
    else:
        print("✓ 查询成功")
        print(f"\n生成的 SQL:")
        print(result.get("generated_sql", "N/A"))

        execution_result = result.get("sql_execution_result", {})
        if execution_result.get("success"):
            print(f"\n返回行数: {execution_result.get('row_count', 0)}")


def example_multiple_queries():
    """
    示例2：多个查询（批量分析）
    """
    print("\n" + "=" * 80)
    print("示例2：多个自然语言查询（批量分析）")
    print("=" * 80)

    # 数据库路径
    db_path = os.path.join(project_root, "data", "analytics.db")

    if not os.path.exists(db_path):
        print(f"错误：数据库文件不存在: {db_path}")
        print("请先运行 scripts/init_database.py 创建数据库")
        return

    # 多个自然语言查询
    queries = [
        "查询各个地区的总销售额，按销售额降序排列",
        "统计每个产品类别的平均折扣率",
        "查询最近7天每天的新增用户数",
        "找出响应时间超过200ms的服务和端点",
    ]

    # 运行批量查询
    results = run_multi_query_analysis(db_path, queries)

    # 总结
    print("\n" + "=" * 80)
    print("批量查询总结")
    print("=" * 80)

    success_count = sum(1 for r in results if not r.get("error"))
    print(f"总查询数: {len(queries)}")
    print(f"成功: {success_count}")
    print(f"失败: {len(queries) - success_count}")

    # 显示每个查询的生成 SQL
    print("\n生成的 SQL 语句:")
    print("-" * 80)
    for i, (query, result) in enumerate(zip(queries, results), 1):
        print(f"\n{i}. {query}")
        if result.get("generated_sql"):
            print(f"   SQL: {result['generated_sql'][:100]}...")
        else:
            print(f"   SQL: 未生成")


def example_business_questions():
    """
    示例3：业务问题分析
    """
    print("\n" + "=" * 80)
    print("示例3：业务问题分析")
    print("=" * 80)

    # 数据库路径
    db_path = os.path.join(project_root, "data", "analytics.db")

    if not os.path.exists(db_path):
        print(f"错误：数据库文件不存在: {db_path}")
        print("请先运行 scripts/init_database.py 创建数据库")
        return

    # 复杂的业务问题
    business_queries = [
        # 销售分析
        "哪个地区的电子产品销售额最高？",

        # 用户留存分析
        "iOS平台的平均7日留存率是多少？",

        # 性能分析
        "payment-service 的平均响应时间是多少？",

        # 趋势分析
        "最近一周每天的总销售额趋势",
    ]

    print(f"\n将执行 {len(business_queries)} 个业务问题查询:")
    for i, q in enumerate(business_queries, 1):
        print(f"  {i}. {q}")

    print("\n开始执行...")
    results = run_multi_query_analysis(db_path, business_queries)

    # 显示关键结果
    print("\n" + "=" * 80)
    print("业务洞察摘要")
    print("=" * 80)

    for i, (query, result) in enumerate(zip(business_queries, results), 1):
        print(f"\n问题 {i}: {query}")
        print("-" * 80)

        if result.get("error"):
            print(f"✗ 查询失败: {result['error']}")
        else:
            # 显示解释
            interpretation = result.get("interpretation", "")
            if interpretation:
                # 只显示前200个字符
                print(interpretation[:200] + "..." if len(interpretation) > 200 else interpretation)
            else:
                # 如果没有解释，显示数据摘要
                exec_result = result.get("sql_execution_result", {})
                if exec_result.get("success"):
                    print(f"✓ 返回 {exec_result.get('row_count', 0)} 行数据")


def interactive_query():
    """
    示例4：交互式查询
    """
    print("\n" + "=" * 80)
    print("示例4：交互式查询模式")
    print("=" * 80)

    # 数据库路径
    db_path = os.path.join(project_root, "data", "analytics.db")

    if not os.path.exists(db_path):
        print(f"错误：数据库文件不存在: {db_path}")
        print("请先运行 scripts/init_database.py 创建数据库")
        return

    print("\n进入交互式查询模式（输入 'exit' 或 'quit' 退出）")
    print("=" * 80)

    # 显示可用的表
    from app.utils.sql_executor import get_db_schema_info

    db_info = get_db_schema_info(db_path)
    if not db_info.get("error"):
        print("\n可用的表:")
        for table in db_info["tables"]:
            cols = ", ".join([c["name"] for c in table["columns"][:5]])
            if len(table["columns"]) > 5:
                cols += ", ..."
            print(f"  - {table['name']}: {cols}")

    print("\n示例查询:")
    print("  - 查询最近7天的销售总额")
    print("  - iOS 平台的日活用户数趋势")
    print("  - 哪个服务的错误率最高")
    print()

    while True:
        try:
            # 获取用户输入
            user_query = input("请输入查询 > ").strip()

            # 检查退出命令
            if user_query.lower() in ['exit', 'quit', '退出']:
                print("退出交互式查询模式")
                break

            # 跳过空输入
            if not user_query:
                continue

            # 执行查询
            print()
            result = run_sql_analysis(db_path, user_query)

            # 简短总结
            print("\n" + "-" * 80)
            if result.get("error"):
                print(f"查询失败: {result['error']}")
            else:
                exec_result = result.get("sql_execution_result", {})
                if exec_result.get("success"):
                    print(f"✓ 查询成功，返回 {exec_result.get('row_count', 0)} 行数据")
            print("-" * 80)
            print()

        except KeyboardInterrupt:
            print("\n\n退出交互式查询模式")
            break
        except EOFError:
            print("\n\n退出交互式查询模式")
            break


def main():
    """
    主函数：运行所有示例
    """
    print("=" * 80)
    print("SQL 自然语言查询示例集")
    print("=" * 80)

    print("\n可用的示例:")
    print("  1. 单个查询示例")
    print("  2. 多个查询示例（批量）")
    print("  3. 业务问题分析")
    print("  4. 交互式查询")
    print("  5. 运行所有示例（1-3）")

    choice = input("\n请选择示例 (1-5): ").strip()

    if choice == "1":
        example_single_query()
    elif choice == "2":
        example_multiple_queries()
    elif choice == "3":
        example_business_questions()
    elif choice == "4":
        interactive_query()
    elif choice == "5":
        example_single_query()
        example_multiple_queries()
        example_business_questions()
    else:
        print("无效的选择")

    print("\n" + "=" * 80)
    print("示例执行完成")
    print("=" * 80)


if __name__ == "__main__":
    main()
