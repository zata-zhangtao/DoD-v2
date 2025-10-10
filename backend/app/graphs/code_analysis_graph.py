"""
代码分析图模块
定义 LangGraph 工作流
"""
from langgraph.graph import StateGraph, END

from backend.app.nodes.code_analysis_nodes import (
    AnalysisState,
    read_csv_info_node,
    generate_code_node,
    execute_code_node,
    summarize_node,
)


def create_analysis_graph():
    """
    创建数据分析工作流图

    工作流：
    读取CSV信息 → 生成代码 → 执行代码 → 总结结果

    Returns:
        编译后的图
    """
    # 创建状态图
    workflow = StateGraph(AnalysisState)

    # 添加节点
    workflow.add_node("read_csv_info", read_csv_info_node)
    workflow.add_node("generate_code", generate_code_node)
    workflow.add_node("execute_code", execute_code_node)
    workflow.add_node("summarize", summarize_node)

    # 设置入口点
    workflow.set_entry_point("read_csv_info")

    # 添加边（定义节点之间的流转）
    workflow.add_edge("read_csv_info", "generate_code")
    workflow.add_edge("generate_code", "execute_code")
    workflow.add_edge("execute_code", "summarize")
    workflow.add_edge("summarize", END)

    # 编译图
    return workflow.compile()


def run_analysis(csv_path: str):
    """
    运行数据分析工作流

    Args:
        csv_path: CSV 文件路径

    Returns:
        最终状态
    """
    # 创建图
    app = create_analysis_graph()

    # 初始化状态
    initial_state: AnalysisState = {
        "csv_path": csv_path,
        "csv_info": {},
        "prompt": "",
        "generated_code": "",
        "execution_result": {},
        "error": None,
        "messages": [],
    }

    print("\n" + "=" * 60)
    print("开始数据分析工作流")
    print("=" * 60)
    print(f"CSV 文件: {csv_path}")
    print()

    # 运行图
    result = app.invoke(initial_state)

    print("\n" + "=" * 60)
    print("工作流完成")
    print("=" * 60)

    return result
