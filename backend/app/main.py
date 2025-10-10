"""
主入口文件
演示如何使用数据分析工作流
"""
import os
from backend.app.graphs.code_analysis_graph import run_analysis


def main():
    """
    主函数：运行数据分析工作流
    """
    # CSV 文件路径
    csv_path = "/code/DoD_v2/backend/data/raw/test_data.csv"

    # 检查文件是否存在
    if not os.path.exists(csv_path):
        print(f"错误: CSV 文件不存在: {csv_path}")
        return

    # 运行分析
    result = run_analysis(csv_path)

    # 展示生成的代码
    print("\n" + "=" * 60)
    print("生成的代码:")
    print("=" * 60)
    print(result.get("generated_code", "无"))

    # 展示执行结果
    print("\n" + "=" * 60)
    print("执行结果:")
    print("=" * 60)
    execution_result = result.get("execution_result", {})
    if execution_result.get("success"):
        print("✓ 执行成功")
    else:
        print(f"✗ 执行失败: {execution_result.get('error', '未知错误')}")


if __name__ == "__main__":
    main()
