"""
SQL 分析节点模块
定义 LangGraph SQL 查询工作流中的各个节点
"""
from typing import TypedDict, Optional, List, Dict
from datetime import datetime
import json

from app.core.config import get_dashscope_client, get_model_name
from app.utils.sql_executor import (
    get_db_schema_info,
    execute_sql_safely,
    validate_sql_safety,
    extract_sql_from_llm_response,
    format_query_result
)


# 定义 SQL 分析状态
class SQLAnalysisState(TypedDict):
    # 数据源信息
    db_path: str                           # 数据库文件路径
    db_info: dict                          # 数据库结构信息
    natural_language_query: str            # 用户的自然语言查询

    # SQL 生成和执行
    generated_sql: str                     # LLM 生成的 SQL 语句
    sql_validation_result: dict            # SQL 验证结果
    sql_execution_result: dict             # SQL 执行结果

    # 结果解释
    interpretation: str                    # LLM 对结果的解释

    # 错误处理
    error: Optional[str]                   # 错误信息
    messages: List[str]                    # 过程日志

    # 多轮查询支持
    query_history: List[Dict]              # 历史查询记录
    current_query_index: int               # 当前查询索引


def read_db_info_node(state: SQLAnalysisState) -> SQLAnalysisState:
    """
    节点1：读取数据库结构信息

    Args:
        state: 当前状态

    Returns:
        SQLAnalysisState: 更新后的状态
    """
    print("=" * 60)
    print("节点 1: 读取数据库结构信息")
    print("=" * 60)

    db_path = state["db_path"]

    try:
        # 获取数据库结构信息
        db_info = get_db_schema_info(db_path)

        if db_info.get("error"):
            raise Exception(db_info["error"])

        print(f"✓ 成功读取数据库: {db_path}")
        print(f"  - 表数量: {db_info['table_count']}")
        print(f"  - 表名: {[t['name'] for t in db_info['tables']]}")

        # 显示每个表的详细信息
        for table in db_info["tables"]:
            print(f"\n  表: {table['name']} ({table['row_count']} 行)")
            print(f"    字段: {', '.join([c['name'] for c in table['columns']])}")

        return {
            **state,
            "db_info": db_info,
            "error": None,
            "messages": [f"成功读取数据库，包含 {db_info['table_count']} 张表"]
        }

    except Exception as e:
        error_msg = f"读取数据库失败: {str(e)}"
        print(f"✗ {error_msg}")

        return {
            **state,
            "db_info": {},
            "error": error_msg,
            "messages": [error_msg]
        }


def generate_sql_node(state: SQLAnalysisState) -> SQLAnalysisState:
    """
    节点2：使用 LLM 将自然语言转换为 SQL

    Args:
        state: 当前状态

    Returns:
        SQLAnalysisState: 更新后的状态
    """
    print("\n" + "=" * 60)
    print("节点 2: 生成 SQL 查询语句")
    print("=" * 60)

    # 如果前一个节点出错，直接返回
    if state.get("error"):
        print(f"✗ 跳过此节点，因为前面出错: {state['error']}")
        return state

    db_info = state["db_info"]
    natural_query = state["natural_language_query"]
    query_history = state.get("query_history", [])

    print(f"用户查询: {natural_query}")

    # 构建数据库 schema 描述
    schema_description = []
    for table in db_info["tables"]:
        cols = ", ".join([f"{c['name']} ({c['type']})" for c in table["columns"]])
        schema_description.append(f"- {table['name']}: {cols}")

        # 添加样例数据
        if table["sample_data"]:
            sample = table["sample_data"][0]
            schema_description.append(f"  样例: {sample}")

    schema_text = "\n".join(schema_description)

    # 添加历史查询上下文（如果有）
    history_context = ""
    if query_history:
        recent_queries = query_history[-3:]  # 最近3条
        history_lines = []
        for i, h in enumerate(recent_queries, 1):
            history_lines.append(f"{i}. 问题: {h['query']}")
            history_lines.append(f"   SQL: {h['sql'][:100]}...")
        history_context = "\n\n之前的查询历史:\n" + "\n".join(history_lines)

    # 构建 LLM 提示词
    prompt = f"""你是一个 SQL 专家。请根据用户的自然语言查询，生成对应的 SQLite SQL 查询语句。

数据库结构：
{schema_text}
{history_context}

用户查询：{natural_query}

要求：
1. 只输出 SQL 语句，不要有任何解释说明
2. 使用标准的 SQLite 语法
3. 只生成 SELECT 查询（不允许 INSERT, UPDATE, DELETE 等）
4. 如果需要聚合或分组，使用 GROUP BY
5. 如果需要排序，使用 ORDER BY
6. 考虑添加 LIMIT 限制返回行数（如果用户没有特别要求，默认限制为 100 行）
7. 确保列名和表名正确匹配数据库结构
8. 如果查询涉及时间范围，使用合适的 DATE 函数

请生成 SQL："""

    try:
        # 调用 LLM
        client = get_dashscope_client()
        model_name = get_model_name()

        print(f"正在调用 {model_name} 生成 SQL...")

        completion = client.chat.completions.create(
            model=model_name,
            messages=[
                {'role': 'system', 'content': '你是一个 SQL 专家，擅长将自然语言转换为精确的 SQL 查询。'},
                {'role': 'user', 'content': prompt}
            ],
        )

        raw_response = completion.choices[0].message.content
        generated_sql = extract_sql_from_llm_response(raw_response)

        print(f"✓ 成功生成 SQL:")
        print("-" * 60)
        print(generated_sql)
        print("-" * 60)

        return {
            **state,
            "generated_sql": generated_sql,
            "messages": state["messages"] + ["成功生成 SQL 查询语句"]
        }

    except Exception as e:
        error_msg = f"LLM 调用失败: {str(e)}"
        print(f"✗ {error_msg}")

        return {
            **state,
            "generated_sql": "",
            "error": error_msg,
            "messages": state["messages"] + [error_msg]
        }


def validate_sql_node(state: SQLAnalysisState) -> SQLAnalysisState:
    """
    节点3：验证 SQL 语句的安全性

    Args:
        state: 当前状态

    Returns:
        SQLAnalysisState: 更新后的状态
    """
    print("\n" + "=" * 60)
    print("节点 3: 验证 SQL 安全性")
    print("=" * 60)

    # 如果前一个节点出错，直接返回
    if state.get("error"):
        print(f"✗ 跳过此节点，因为前面出错: {state['error']}")
        return state

    generated_sql = state["generated_sql"]

    if not generated_sql:
        error_msg = "没有生成的 SQL 可验证"
        print(f"✗ {error_msg}")
        return {
            **state,
            "error": error_msg,
            "messages": state["messages"] + [error_msg]
        }

    try:
        # 验证 SQL 安全性
        is_safe, error_msg = validate_sql_safety(generated_sql)

        validation_result = {
            "is_safe": is_safe,
            "error": error_msg if not is_safe else ""
        }

        if is_safe:
            print("✓ SQL 安全性验证通过")
        else:
            print(f"✗ SQL 安全性验证失败: {error_msg}")

        return {
            **state,
            "sql_validation_result": validation_result,
            "error": None if is_safe else f"SQL 安全性验证失败: {error_msg}",
            "messages": state["messages"] + [
                "SQL 安全性验证通过" if is_safe else f"SQL 验证失败: {error_msg}"
            ]
        }

    except Exception as e:
        error_msg = f"验证 SQL 时出错: {str(e)}"
        print(f"✗ {error_msg}")

        return {
            **state,
            "sql_validation_result": {"is_safe": False, "error": error_msg},
            "error": error_msg,
            "messages": state["messages"] + [error_msg]
        }


def execute_sql_node(state: SQLAnalysisState) -> SQLAnalysisState:
    """
    节点4：执行 SQL 查询

    Args:
        state: 当前状态

    Returns:
        SQLAnalysisState: 更新后的状态
    """
    print("\n" + "=" * 60)
    print("节点 4: 执行 SQL 查询")
    print("=" * 60)

    # 如果前一个节点出错，直接返回
    if state.get("error"):
        print(f"✗ 跳过此节点，因为前面出错: {state['error']}")
        return state

    generated_sql = state["generated_sql"]
    db_path = state["db_path"]

    if not generated_sql:
        error_msg = "没有生成的 SQL 可执行"
        print(f"✗ {error_msg}")
        return {
            **state,
            "error": error_msg,
            "messages": state["messages"] + [error_msg]
        }

    try:
        print("正在执行 SQL...")
        print("-" * 60)

        # 执行 SQL
        result = execute_sql_safely(generated_sql, db_path)

        if result["success"]:
            print("✓ SQL 执行成功")
            print(f"  返回 {result['row_count']} 行数据")
            print(f"  列: {', '.join(result['columns'])}")

            # 显示前几行数据
            if result["data"]:
                print("\n前 3 行数据:")
                for i, row in enumerate(result["data"][:3], 1):
                    print(f"  {i}. {row}")
        else:
            print("✗ SQL 执行失败")
            print(f"  错误: {result['error']}")

        # 记录到查询历史
        query_history = state.get("query_history", [])
        query_record = {
            "query": state["natural_language_query"],
            "sql": generated_sql,
            "result": result,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        query_history.append(query_record)

        return {
            **state,
            "sql_execution_result": result,
            "query_history": query_history,
            "error": None if result["success"] else result["error"],
            "messages": state["messages"] + [
                f"SQL 执行成功，返回 {result['row_count']} 行" if result["success"]
                else f"SQL 执行失败: {result['error']}"
            ]
        }

    except Exception as e:
        error_msg = f"执行 SQL 时出错: {str(e)}"
        print(f"✗ {error_msg}")

        return {
            **state,
            "sql_execution_result": {"success": False, "error": str(e)},
            "error": error_msg,
            "messages": state["messages"] + [error_msg]
        }


def interpret_results_node(state: SQLAnalysisState) -> SQLAnalysisState:
    """
    节点5：使用 LLM 解释查询结果

    Args:
        state: 当前状态

    Returns:
        SQLAnalysisState: 更新后的状态
    """
    print("\n" + "=" * 60)
    print("节点 5: 解释查询结果")
    print("=" * 60)

    # 如果前一个节点出错，直接返回
    if state.get("error"):
        print(f"✗ 跳过此节点，因为前面出错: {state['error']}")
        return state

    natural_query = state["natural_language_query"]
    generated_sql = state["generated_sql"]
    execution_result = state["sql_execution_result"]

    if not execution_result.get("success"):
        error_msg = "查询执行失败，无法解释结果"
        print(f"✗ {error_msg}")
        return {
            **state,
            "interpretation": "",
            "messages": state["messages"] + [error_msg]
        }

    # 准备结果数据用于解释
    result_summary = {
        "row_count": execution_result["row_count"],
        "columns": execution_result["columns"],
        "sample_data": execution_result["data"][:10]  # 只给 LLM 前 10 行
    }

    # 构建解释提示词
    prompt = f"""你是一个数据分析专家。请根据用户的问题和 SQL 查询结果，用通俗易懂的中文解释查询结果。

用户问题：{natural_query}

执行的 SQL：
{generated_sql}

查询结果：
- 返回行数: {result_summary['row_count']}
- 列名: {', '.join(result_summary['columns'])}
- 数据示例（前10行）:
{json.dumps(result_summary['sample_data'], ensure_ascii=False, indent=2)}

请提供：
1. 对查询结果的总体概述（1-2句话）
2. 关键数据洞察（2-5个要点）
3. 如果数据有明显的趋势或异常，请指出

要求：
- 使用通俗易懂的语言
- 重点关注用户问题相关的信息
- 如果数据量大，总结关键统计信息
- 简洁明了，避免冗长"""

    try:
        # 调用 LLM
        client = get_dashscope_client()
        model_name = get_model_name()

        print(f"正在调用 {model_name} 解释结果...")

        completion = client.chat.completions.create(
            model=model_name,
            messages=[
                {'role': 'system', 'content': '你是一个数据分析专家，擅长用通俗易懂的语言解释数据查询结果。'},
                {'role': 'user', 'content': prompt}
            ],
        )

        interpretation = completion.choices[0].message.content

        print("✓ 结果解释已生成")
        print("-" * 60)
        print(interpretation)
        print("-" * 60)

        return {
            **state,
            "interpretation": interpretation,
            "messages": state["messages"] + ["成功生成结果解释"]
        }

    except Exception as e:
        error_msg = f"生成解释失败: {str(e)}"
        print(f"⚠️  {error_msg}")

        # 解释失败不影响整体流程，使用默认解释
        default_interpretation = format_query_result(execution_result)

        return {
            **state,
            "interpretation": default_interpretation,
            "messages": state["messages"] + [f"使用默认格式显示结果（{error_msg}）"]
        }
