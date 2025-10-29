"""
代码分析图模块
定义 LangGraph 多轮迭代工作流
支持 CSV 文件分析和 SQL 数据库查询
"""
from langgraph.graph import StateGraph, END

from app.nodes.code_analysis_nodes import (
    AnalysisState,
    read_csv_info_node,
    plan_analysis_node,
    generate_code_node,
    execute_code_node,
    handle_error_node,
    update_temp_report_node,
    decide_continue_node,
    final_summary_node,
)
from app.nodes.sql_analysis_nodes import (
    SQLAnalysisState,
    read_db_info_node,
    generate_sql_node,
    validate_sql_node,
    execute_sql_node,
    interpret_results_node,
)
from app.utils.state_manager import load_state
from app.utils.code_executor import execute_code_safely


def create_analysis_graph():
    """
    创建多轮迭代数据分析工作流图（支持错误处理）

    工作流：
    读取CSV → 规划分析 → [生成代码 → 执行代码 → (错误处理) → 更新报告 → 决策继续] (循环) → 最终总结

    Returns:
        编译后的图
    """
    # 创建状态图
    workflow = StateGraph(AnalysisState)

    # 添加节点
    workflow.add_node("read_csv_info", read_csv_info_node)
    workflow.add_node("plan_analysis", plan_analysis_node)
    workflow.add_node("generate_code", generate_code_node)
    workflow.add_node("execute_code", execute_code_node)
    workflow.add_node("handle_error", handle_error_node)
    workflow.add_node("update_temp_report", update_temp_report_node)
    workflow.add_node("decide_continue", decide_continue_node)
    workflow.add_node("final_summary", final_summary_node)

    # 设置入口点
    workflow.set_entry_point("read_csv_info")

    # 添加固定边
    workflow.add_edge("read_csv_info", "plan_analysis")
    workflow.add_edge("plan_analysis", "generate_code")
    workflow.add_edge("generate_code", "execute_code")

    # 条件边1：执行代码后判断是否有错误
    def check_execution_error(state: AnalysisState) -> str:
        """
        检查代码执行是否成功

        Returns:
            "success": 执行成功，继续更新报告
            "error": 执行失败，进入错误处理
        """
        if state.get("has_execution_error", False):
            return "error"
        else:
            return "success"

    workflow.add_conditional_edges(
        "execute_code",
        check_execution_error,
        {
            "success": "update_temp_report",  # 成功则更新报告
            "error": "handle_error"           # 失败则处理错误
        }
    )

    # 条件边2：错误处理后判断下一步
    def after_error_handling(state: AnalysisState) -> str:
        """
        错误处理后决定下一步

        Returns:
            "retry": 自动修复成功，重新执行代码
            "skip": 跳过本轮，继续决策流程
            "pause": 暂停等待手动修复
        """
        if state.get("paused_for_fix", False):
            return "pause"
        elif state.get("user_intervention_mode") == "auto_fix":
            return "retry"
        else:  # skip
            return "skip"

    workflow.add_conditional_edges(
        "handle_error",
        after_error_handling,
        {
            "retry": "execute_code",          # 重试执行
            "skip": "decide_continue",        # 跳过，继续决策
            "pause": "final_summary"          # 暂停，生成报告并结束
        }
    )

    workflow.add_edge("update_temp_report", "decide_continue")

    # 条件边3：根据 should_continue 决定是继续循环还是结束
    def should_continue_analysis(state: AnalysisState) -> str:
        """
        决定下一步：继续分析或进入最终总结

        Returns:
            "continue": 返回 generate_code 节点继续下一轮
            "finish": 进入 final_summary 节点
        """
        if state.get("should_continue", False):
            return "continue"
        else:
            return "finish"

    workflow.add_conditional_edges(
        "decide_continue",
        should_continue_analysis,
        {
            "continue": "generate_code",  # 循环回生成代码
            "finish": "final_summary"      # 进入最终总结
        }
    )

    workflow.add_edge("final_summary", END)

    # 编译图
    return workflow.compile()


def run_analysis(csv_path: str):
    """
    运行多轮迭代数据分析工作流

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
        # 多轮分析字段
        "analysis_rounds": [],
        "current_round": 0,
        "analysis_plan": [],
        "completed_analyses": [],
        "temp_report_path": "",
        "should_continue": True,
        # 错误处理字段
        "has_execution_error": False,
        "error_retry_count": 0,
        "user_intervention_mode": None,
        "paused_for_fix": False,
    }

    print("\n" + "=" * 80)
    print("开始多轮迭代数据分析工作流")
    print("=" * 80)
    print(f"CSV 文件: {csv_path}")
    print("=" * 80)
    print()

    # 运行图
    result = app.invoke(initial_state)

    print("\n" + "=" * 80)
    print("多轮分析工作流完成")
    print("=" * 80)

    return result


def resume_analysis(state_file: str, fixed_code: str = None):
    """
    从保存的状态恢复多轮分析工作流

    Args:
        state_file: 保存的状态文件路径
        fixed_code: 用户修复后的代码（可选）

    Returns:
        最终状态
    """
    print("\n" + "=" * 80)
    print("从断点恢复分析工作流")
    print("=" * 80)

    # 加载保存的状态
    print(f"正在加载状态文件: {state_file}")
    saved_state = load_state(state_file)

    print(f"✓ 状态加载成功")
    print(f"  - CSV 文件: {saved_state.get('csv_path')}")
    print(f"  - 当前轮次: {saved_state.get('current_round', 0)}")
    print(f"  - 已完成: {len(saved_state.get('completed_analyses', []))} 个任务")

    # 如果提供了修复代码，先执行它
    if fixed_code:
        print("\n正在执行修复后的代码...")
        print("-" * 80)

        csv_path = saved_state.get("csv_path", "")
        result = execute_code_safely(fixed_code, csv_path)

        if result["success"]:
            print("✓ 修复后的代码执行成功")
            print("\n执行输出：")
            print(result["output"])

            # 更新状态
            current_round = saved_state.get("current_round", 0)
            analysis_plan = saved_state.get("analysis_plan", [])
            current_task = (
                analysis_plan[current_round - 1]
                if current_round > 0 and current_round <= len(analysis_plan)
                else "未知任务"
            )

            # 记录成功的轮次
            round_record = {
                "round": current_round,
                "task": current_task,
                "code": fixed_code,
                "execution_result": result,
                "timestamp": saved_state.get("saved_at", "")
            }

            analysis_rounds = saved_state.get("analysis_rounds", [])
            analysis_rounds.append(round_record)

            completed_analyses = saved_state.get("completed_analyses", [])
            completed_analyses.append(current_task)

            saved_state["analysis_rounds"] = analysis_rounds
            saved_state["completed_analyses"] = completed_analyses
            saved_state["generated_code"] = fixed_code
            saved_state["execution_result"] = result
            saved_state["error"] = None
            saved_state["has_execution_error"] = False

        else:
            print("✗ 修复后的代码仍然执行失败")
            print(f"错误: {result['error']}")
            saved_state["error"] = result["error"]
            saved_state["has_execution_error"] = True

    # 重置暂停标志，继续分析
    saved_state["paused_for_fix"] = False
    saved_state["should_continue"] = True

    # 创建图并从当前状态继续
    app = create_analysis_graph()

    print("\n正在继续分析...")
    print("=" * 80)

    # 从 decide_continue 节点继续
    result = app.invoke(saved_state)

    print("\n" + "=" * 80)
    print("恢复的分析工作流完成")
    print("=" * 80)

    return result


# ============================================================
# SQL 数据库查询工作流
# ============================================================

def create_sql_analysis_graph():
    """
    创建 SQL 自然语言查询工作流图

    工作流：
    读取数据库信息 → 生成SQL → 验证SQL → 执行SQL → 解释结果

    Returns:
        编译后的图
    """
    # 创建状态图
    workflow = StateGraph(SQLAnalysisState)

    # 添加节点
    workflow.add_node("read_db_info", read_db_info_node)
    workflow.add_node("generate_sql", generate_sql_node)
    workflow.add_node("validate_sql", validate_sql_node)
    workflow.add_node("execute_sql", execute_sql_node)
    workflow.add_node("interpret_results", interpret_results_node)

    # 设置入口点
    workflow.set_entry_point("read_db_info")

    # 添加边
    workflow.add_edge("read_db_info", "generate_sql")
    workflow.add_edge("generate_sql", "validate_sql")

    # 条件边：验证通过则执行，否则结束
    def check_validation(state: SQLAnalysisState) -> str:
        """检查 SQL 验证是否通过"""
        if state.get("error"):
            return "end"
        validation_result = state.get("sql_validation_result", {})
        if validation_result.get("is_safe", False):
            return "execute"
        else:
            return "end"

    workflow.add_conditional_edges(
        "validate_sql",
        check_validation,
        {
            "execute": "execute_sql",
            "end": END
        }
    )

    # 条件边：执行成功则解释结果，否则结束
    def check_execution(state: SQLAnalysisState) -> str:
        """检查 SQL 执行是否成功"""
        execution_result = state.get("sql_execution_result", {})
        if execution_result.get("success", False):
            return "interpret"
        else:
            return "end"

    workflow.add_conditional_edges(
        "execute_sql",
        check_execution,
        {
            "interpret": "interpret_results",
            "end": END
        }
    )

    workflow.add_edge("interpret_results", END)

    # 编译图
    return workflow.compile()


def run_sql_analysis(db_path: str, natural_query: str):
    """
    运行自然语言 SQL 查询工作流

    Args:
        db_path: 数据库文件路径
        natural_query: 用户的自然语言查询

    Returns:
        最终状态
    """
    # 创建图
    app = create_sql_analysis_graph()

    # 初始化状态
    initial_state: SQLAnalysisState = {
        "db_path": db_path,
        "db_info": {},
        "natural_language_query": natural_query,
        "generated_sql": "",
        "sql_validation_result": {},
        "sql_execution_result": {},
        "interpretation": "",
        "error": None,
        "messages": [],
        "query_history": [],
        "current_query_index": 0,
    }

    print("\n" + "=" * 80)
    print("开始自然语言 SQL 查询工作流")
    print("=" * 80)
    print(f"数据库: {db_path}")
    print(f"查询: {natural_query}")
    print("=" * 80)
    print()

    # 运行图
    result = app.invoke(initial_state)

    print("\n" + "=" * 80)
    print("SQL 查询工作流完成")
    print("=" * 80)

    # 显示结果摘要
    if result.get("error"):
        print(f"✗ 查询失败: {result['error']}")
    else:
        print("✓ 查询成功")
        if result.get("interpretation"):
            print("\n结果解释:")
            print("-" * 80)
            print(result["interpretation"])
            print("-" * 80)

    return result


def run_multi_query_analysis(db_path: str, queries: list):
    """
    运行多轮 SQL 查询分析

    Args:
        db_path: 数据库文件路径
        queries: 自然语言查询列表

    Returns:
        所有查询的结果列表
    """
    print("\n" + "=" * 80)
    print(f"开始多轮查询分析（共 {len(queries)} 个查询）")
    print("=" * 80)

    results = []
    shared_history = []

    for i, query in enumerate(queries, 1):
        print(f"\n{'='*80}")
        print(f"查询 {i}/{len(queries)}: {query}")
        print("=" * 80)

        # 创建图
        app = create_sql_analysis_graph()

        # 初始化状态（共享历史）
        initial_state: SQLAnalysisState = {
            "db_path": db_path,
            "db_info": {},
            "natural_language_query": query,
            "generated_sql": "",
            "sql_validation_result": {},
            "sql_execution_result": {},
            "interpretation": "",
            "error": None,
            "messages": [],
            "query_history": shared_history.copy(),
            "current_query_index": i,
        }

        # 运行图
        result = app.invoke(initial_state)

        # 更新共享历史
        if result.get("query_history"):
            shared_history = result["query_history"]

        results.append(result)

        # 简短摘要
        if result.get("error"):
            print(f"\n✗ 查询 {i} 失败: {result['error']}")
        else:
            print(f"\n✓ 查询 {i} 成功")

    print("\n" + "=" * 80)
    print(f"多轮查询分析完成！成功: {sum(1 for r in results if not r.get('error'))}/{len(queries)}")
    print("=" * 80)

    return results
