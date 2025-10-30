"""
Excel 文件多轮分析示例
演示如何使用 LangGraph 工作流分析 Excel 文件
"""
import os
import sys

# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from app.graphs.code_analysis_graph import run_analysis


def example_basic_excel_analysis():
    """
    示例1：基础 Excel 文件分析

    演示如何对一个包含多个 sheet 的 Excel 文件进行自动化分析
    系统会：
    1. 自动检测文件类型（Excel）
    2. 读取所有 sheet 的结构信息
    3. 由 LLM 规划多轮分析任务
    4. 自动生成和执行分析代码
    5. 生成最终分析报告
    """
    print("\n" + "=" * 80)
    print("示例1：基础 Excel 文件分析")
    print("=" * 80)

    # Excel 文件路径
    excel_path = os.path.join(project_root, "data", "raw", "test_data.xlsx")

    if not os.path.exists(excel_path):
        print(f"错误：Excel 文件不存在: {excel_path}")
        print("请确保测试文件已创建")
        return

    print(f"\n文件路径: {excel_path}")
    print("开始分析...")

    # 运行完整的多轮分析工作流
    result = run_analysis(excel_path)

    # 显示结果摘要
    print("\n" + "=" * 80)
    print("分析结果摘要")
    print("=" * 80)

    if result.get("error"):
        print(f"✗ 分析失败: {result['error']}")
    else:
        print("✓ 分析成功完成")

        # 显示 Excel 文件信息
        excel_info = result.get("excel_info", {})
        if excel_info:
            print(f"\nExcel 文件结构:")
            print(f"  - 总 Sheet 数: {excel_info.get('total_sheets', 0)}")
            print(f"  - Sheet 名称: {', '.join(excel_info.get('sheet_names', []))}")

            for sheet_name, sheet_info in excel_info.get("sheets", {}).items():
                print(f"\n  Sheet: {sheet_name}")
                print(f"    - 行数: {sheet_info.get('rows', 0)}")
                print(f"    - 列数: {sheet_info.get('cols', 0)}")
                print(f"    - 列名: {', '.join(sheet_info.get('columns', []))}")

        # 显示完成的分析任务
        completed = result.get("completed_analyses", [])
        if completed:
            print(f"\n完成的分析任务 ({len(completed)} 个):")
            for i, task in enumerate(completed, 1):
                print(f"  {i}. {task}")

        # 显示临时报告路径
        temp_report = result.get("temp_report_path", "")
        if temp_report and os.path.exists(temp_report):
            print(f"\n临时报告保存位置: {temp_report}")


def example_excel_with_error_handling():
    """
    示例2：带错误处理的 Excel 分析

    演示工作流如何处理执行错误：
    - 自动检测代码执行失败
    - 提供错误信息和修复建议
    - 支持跳过失败任务继续分析
    """
    print("\n" + "=" * 80)
    print("示例2：带错误处理的 Excel 分析")
    print("=" * 80)

    excel_path = os.path.join(project_root, "data", "raw", "test_data.xlsx")

    if not os.path.exists(excel_path):
        print(f"错误：Excel 文件不存在: {excel_path}")
        return

    print(f"\n文件路径: {excel_path}")
    print("开始分析（包含错误处理演示）...")

    # 运行分析
    result = run_analysis(excel_path)

    # 显示分析轮次信息
    print("\n" + "=" * 80)
    print("分析轮次详情")
    print("=" * 80)

    analysis_rounds = result.get("analysis_rounds", [])
    if analysis_rounds:
        for round_info in analysis_rounds:
            round_num = round_info.get("round", 0)
            task = round_info.get("task", "未知任务")
            exec_result = round_info.get("execution_result", {})

            print(f"\n轮次 {round_num}: {task}")
            if exec_result.get("success"):
                print(f"  ✓ 执行成功")
                output_preview = exec_result.get("output", "")[:100]
                if output_preview:
                    print(f"  输出预览: {output_preview}...")
            else:
                print(f"  ✗ 执行失败")
                error = exec_result.get("error", "未知错误")
                print(f"  错误: {error[:200]}...")
    else:
        print("无分析轮次记录")


def example_manual_analysis_plan():
    """
    示例3：手动指定分析计划

    演示如何通过修改代码来指定特定的分析任务，
    而不是完全依赖 LLM 自动规划
    """
    print("\n" + "=" * 80)
    print("示例3：自定义分析任务")
    print("=" * 80)

    print("\n说明:")
    print("如果需要手动指定分析计划，可以修改代码分析节点")
    print("在 plan_analysis_node 中设置自定义的 analysis_plan 列表")
    print()
    print("示例分析计划:")
    print("  1. 数据质量检查（缺失值、重复值、异常值）")
    print("  2. 数值列统计分析（均值、中位数、分布）")
    print("  3. 类别列分析（频率统计、占比）")
    print("  4. 多 Sheet 关联分析")
    print("  5. 时间序列分析（如果有日期列）")
    print()
    print("当前版本使用 LLM 自动规划，未来可以支持用户自定义")


def example_compare_csv_and_excel():
    """
    示例4：CSV vs Excel 文件分析对比

    演示系统如何自动识别文件类型并使用相应的处理流程
    """
    print("\n" + "=" * 80)
    print("示例4：CSV vs Excel 文件分析对比")
    print("=" * 80)

    # CSV 文件
    csv_path = os.path.join(project_root, "data", "raw", "test_data.csv")
    # Excel 文件
    excel_path = os.path.join(project_root, "data", "raw", "test_data.xlsx")

    print("\n系统特性:")
    print("  - 自动检测文件类型（.csv, .xlsx, .xls, .xlsm, .xlsb）")
    print("  - CSV 文件: 单表分析")
    print("  - Excel 文件: 多 Sheet 分析，支持跨 Sheet 关联")
    print("  - 统一的代码生成和执行流程")
    print("  - 相同的多轮迭代分析框架")

    # 检查文件存在性
    print("\n可用文件:")
    if os.path.exists(csv_path):
        print(f"  ✓ CSV:  {csv_path}")
    else:
        print(f"  ✗ CSV:  {csv_path} (不存在)")

    if os.path.exists(excel_path):
        print(f"  ✓ Excel: {excel_path}")
    else:
        print(f"  ✗ Excel: {excel_path} (不存在)")

    print("\n提示:")
    print("  可以修改 run_analysis(file_path) 的参数来分析不同文件")


def show_excel_file_info():
    """
    辅助函数：显示 Excel 文件的详细信息
    不运行完整分析，只读取文件结构
    """
    print("\n" + "=" * 80)
    print("Excel 文件结构信息")
    print("=" * 80)

    from app.nodes.code_analysis_nodes import read_excel_info_node

    excel_path = os.path.join(project_root, "data", "raw", "test_data.xlsx")

    if not os.path.exists(excel_path):
        print(f"错误：Excel 文件不存在: {excel_path}")
        return

    # 只读取 Excel 信息，不运行完整工作流
    initial_state = {
        "excel_path": excel_path,
        "excel_info": {},
        "error": None,
    }

    result = read_excel_info_node(initial_state)

    if result.get("error"):
        print(f"✗ 读取失败: {result['error']}")
        return

    excel_info = result.get("excel_info", {})

    print(f"\n文件: {excel_path}")
    print(f"Sheet 数量: {excel_info.get('total_sheets', 0)}")
    print(f"文件大小: {excel_info.get('file_size_mb', 0):.2f} MB")

    # 详细显示每个 sheet
    for sheet_name, sheet_info in excel_info.get("sheets", {}).items():
        print(f"\n" + "-" * 80)
        print(f"Sheet: {sheet_name}")
        print("-" * 80)
        print(f"行数: {sheet_info.get('rows', 0)}")
        print(f"列数: {sheet_info.get('cols', 0)}")

        print(f"\n列名及类型:")
        columns = sheet_info.get("columns", [])
        dtypes = sheet_info.get("dtypes", {})
        for col in columns:
            dtype = dtypes.get(col, "unknown")
            print(f"  - {col}: {dtype}")

        # 显示数值列统计
        summary = sheet_info.get("summary", {})
        numeric_cols = summary.get("numeric_cols", [])
        if numeric_cols:
            print(f"\n数值列 ({len(numeric_cols)} 个):")
            for col in numeric_cols:
                print(f"  - {col}")

        # 显示类别列
        categorical_cols = summary.get("categorical_cols", [])
        if categorical_cols:
            print(f"\n类别列 ({len(categorical_cols)} 个):")
            for col in categorical_cols:
                print(f"  - {col}")

        # 显示日期列
        date_cols = summary.get("date_cols", [])
        if date_cols:
            print(f"\n日期列 ({len(date_cols)} 个):")
            for col in date_cols:
                print(f"  - {col}")

        # 显示缺失值
        null_counts = sheet_info.get("null_counts", {})
        cols_with_nulls = {k: v for k, v in null_counts.items() if v > 0}
        if cols_with_nulls:
            print(f"\n缺失值统计:")
            for col, count in cols_with_nulls.items():
                pct = (count / sheet_info.get('rows', 1)) * 100
                print(f"  - {col}: {count} ({pct:.1f}%)")
        else:
            print(f"\n无缺失值")

        # 显示前几行样例
        print(f"\n前 3 行样例数据:")
        head_data = sheet_info.get("head", [])
        if head_data:
            for i, row in enumerate(head_data, 1):
                print(f"  行 {i}: {row}")


def main():
    """
    主函数：运行所有示例
    """
    print("=" * 80)
    print("Excel 文件多轮分析示例集")
    print("=" * 80)

    print("\n可用的示例:")
    print("  1. 基础 Excel 文件分析（完整工作流）")
    print("  2. 带错误处理的分析")
    print("  3. 自定义分析任务说明")
    print("  4. CSV vs Excel 对比说明")
    print("  5. 查看 Excel 文件结构（不运行分析）")
    print("  6. 运行所有分析示例（1-2）")

    choice = input("\n请选择示例 (1-6): ").strip()

    if choice == "1":
        example_basic_excel_analysis()
    elif choice == "2":
        example_excel_with_error_handling()
    elif choice == "3":
        example_manual_analysis_plan()
    elif choice == "4":
        example_compare_csv_and_excel()
    elif choice == "5":
        show_excel_file_info()
    elif choice == "6":
        example_basic_excel_analysis()
        example_excel_with_error_handling()
    else:
        print("无效的选择")

    print("\n" + "=" * 80)
    print("示例执行完成")
    print("=" * 80)


if __name__ == "__main__":
    main()
