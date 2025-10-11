"""
代码分析节点模块
定义 LangGraph 工作流中的各个节点
"""
from typing import TypedDict, Optional
import pandas as pd
import os

from app.core.config import get_dashscope_client, get_model_name
from app.utils.code_executor import execute_code_safely, extract_code_from_llm_response
from app.utils.report_generator import update_temp_markdown, generate_final_report
from app.utils.state_manager import save_state, get_error_info
from app.utils.error_handler import analyze_and_fix_code, get_user_choice_prompt
from datetime import datetime
import json


# 定义状态
class AnalysisState(TypedDict):
    csv_path: str                      # CSV 文件路径
    csv_info: dict                     # CSV 基本信息
    prompt: str                        # 给 LLM 的提示词
    generated_code: str                # LLM 生成的代码
    execution_result: dict             # 代码执行结果
    error: Optional[str]               # 错误信息
    messages: list[str]                # 过程日志

    # 多轮分析相关字段
    analysis_rounds: list[dict]        # 每轮分析的详细记录
    current_round: int                 # 当前轮次（从1开始）
    analysis_plan: list[str]           # LLM规划的分析任务列表
    completed_analyses: list[str]      # 已完成的分析类型
    temp_report_path: str              # 临时报告文件路径
    should_continue: bool              # 是否继续下一轮分析

    # 错误处理相关字段
    has_execution_error: bool          # 是否有执行错误
    error_retry_count: int             # 当前轮次错误重试次数
    user_intervention_mode: Optional[str]  # 用户干预模式: 'auto_fix', 'manual_fix', 'skip'
    paused_for_fix: bool               # 是否暂停等待修复


def read_csv_info_node(state: AnalysisState) -> AnalysisState:
    """
    节点1：读取 CSV 文件信息

    Args:
        state: 当前状态

    Returns:
        AnalysisState: 更新后的状态
    """
    print("=" * 60)
    print("节点 1: 读取 CSV 文件信息")
    print("=" * 60)

    csv_path = state["csv_path"]

    try:
        # 读取 CSV 文件
        df = pd.read_csv(csv_path)

        # 获取基本信息
        csv_info = {
            "file_path": csv_path,
            "rows": len(df),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "sample_data": df.head(3).to_dict('records'),
            "summary": {
                "null_counts": df.isnull().sum().to_dict(),
                "numeric_cols": df.select_dtypes(include=['number']).columns.tolist(),
            }
        }

        print(f"✓ 成功读取 CSV 文件: {csv_path}")
        print(f"  - 行数: {csv_info['rows']}")
        print(f"  - 列数: {len(csv_info['columns'])}")
        print(f"  - 列名: {csv_info['columns']}")

        return {
            "csv_path": csv_path,
            "csv_info": csv_info,
            "prompt": "",
            "generated_code": "",
            "execution_result": {},
            "error": None,
            "messages": [f"成功读取 CSV 文件，包含 {csv_info['rows']} 行数据"],
            # 多轮分析字段初始化
            "analysis_rounds": [],
            "current_round": 0,
            "analysis_plan": [],
            "completed_analyses": [],
            "temp_report_path": "",
            "should_continue": True,
            # 错误处理字段初始化
            "has_execution_error": False,
            "error_retry_count": 0,
            "user_intervention_mode": None,
            "paused_for_fix": False,
        }

    except Exception as e:
        error_msg = f"读取 CSV 文件失败: {str(e)}"
        print(f"✗ {error_msg}")

        return {
            "csv_path": csv_path,
            "csv_info": {},
            "prompt": "",
            "generated_code": "",
            "execution_result": {},
            "error": error_msg,
            "messages": [error_msg],
            # 多轮分析字段初始化
            "analysis_rounds": [],
            "current_round": 0,
            "analysis_plan": [],
            "completed_analyses": [],
            "temp_report_path": "",
            "should_continue": False,
            # 错误处理字段初始化
            "has_execution_error": False,
            "error_retry_count": 0,
            "user_intervention_mode": None,
            "paused_for_fix": False,
        }


def plan_analysis_node(state: AnalysisState) -> AnalysisState:
    """
    节点1.5：使用 LLM 规划多轮分析任务

    Args:
        state: 当前状态

    Returns:
        AnalysisState: 更新后的状态（包含分析计划）
    """
    print("\n" + "=" * 60)
    print("节点 1.5: 规划分析任务")
    print("=" * 60)

    # 如果前一个节点出错，直接返回
    if state.get("error"):
        print(f"✗ 跳过此节点，因为前面出错: {state['error']}")
        return state

    csv_info = state["csv_info"]

    # 构建规划提示词
    planning_prompt = f"""你是一个数据分析专家。请根据以下 CSV 文件信息，规划一系列深度数据分析任务。

CSV 文件信息：
- 行数: {csv_info['rows']}
- 列名: {csv_info['columns']}
- 数据类型: {csv_info['dtypes']}
- 数值列: {csv_info.get('summary', {}).get('numeric_cols', [])}
- 前3行示例: {csv_info['sample_data']}

请规划 3-5 个循序渐进的分析任务，从基础到深入：
1. 基础统计分析（必须）
2. 数据分布和可视化分析
3. 相关性分析（如果有多个数值列）
4. 异常值检测
5. 分类变量分析（如果有分类列）
6. 其他有价值的分析

要求：
- 只输出 JSON 格式的任务列表
- 每个任务用简短的中文描述（10-20字）
- 根据数据特征选择合适的分析任务
- 返回格式: {{"tasks": ["任务1", "任务2", "任务3"]}}
"""

    try:
        # 调用 LLM 生成分析计划
        client = get_dashscope_client()
        model_name = get_model_name()

        print(f"正在调用 {model_name} 规划分析任务...")

        completion = client.chat.completions.create(
            model=model_name,
            messages=[
                {'role': 'system', 'content': '你是一个数据分析规划专家，擅长根据数据特征制定分析计划。只返回JSON格式的结果。'},
                {'role': 'user', 'content': planning_prompt}
            ],
        )

        raw_response = completion.choices[0].message.content

        # 解析 JSON 响应
        try:
            # 尝试提取 JSON（可能被包裹在代码块中）
            if "```json" in raw_response:
                json_str = raw_response.split("```json")[1].split("```")[0].strip()
            elif "```" in raw_response:
                json_str = raw_response.split("```")[1].split("```")[0].strip()
            else:
                json_str = raw_response.strip()

            plan_data = json.loads(json_str)
            analysis_plan = plan_data.get("tasks", [])

        except json.JSONDecodeError:
            # 如果 JSON 解析失败，使用默认计划
            print("⚠️  JSON 解析失败，使用默认分析计划")
            analysis_plan = [
                "基础统计分析",
                "数据分布分析",
                "相关性分析",
                "异常值检测"
            ]

        print(f"✓ 分析计划已生成，共 {len(analysis_plan)} 个任务：")
        for i, task in enumerate(analysis_plan, 1):
            print(f"  {i}. {task}")

        # 初始化临时报告
        temp_report_path = update_temp_markdown(state)
        print(f"✓ 临时报告已创建: {temp_report_path}")

        return {
            **state,
            "analysis_plan": analysis_plan,
            "temp_report_path": temp_report_path,
            "messages": state["messages"] + [f"规划了 {len(analysis_plan)} 个分析任务"]
        }

    except Exception as e:
        error_msg = f"规划分析任务失败: {str(e)}"
        print(f"✗ {error_msg}")

        return {
            **state,
            "error": error_msg,
            "should_continue": False,
            "messages": state["messages"] + [error_msg]
        }


def generate_code_node(state: AnalysisState) -> AnalysisState:
    """
    节点2：使用 LLM 生成分析代码（支持多轮）

    Args:
        state: 当前状态

    Returns:
        AnalysisState: 更新后的状态
    """
    # 轮次递增
    current_round = state.get("current_round", 0) + 1

    print("\n" + "=" * 60)
    print(f"节点 2: 生成第 {current_round} 轮分析代码")
    print("=" * 60)

    # 如果前一个节点出错，直接返回
    if state.get("error"):
        print(f"✗ 跳过此节点，因为前面出错: {state['error']}")
        return state

    csv_info = state["csv_info"]
    analysis_plan = state.get("analysis_plan", [])
    completed_analyses = state.get("completed_analyses", [])

    # 确定当前轮次的任务
    if current_round <= len(analysis_plan):
        current_task = analysis_plan[current_round - 1]
    else:
        # 超出计划，停止分析
        print("✓ 所有计划任务已完成")
        return {
            **state,
            "should_continue": False,
            "messages": state["messages"] + ["所有计划任务已完成"]
        }

    print(f"当前任务: {current_task}")

    # 构建提示词，包含历史分析信息
    previous_analysis_summary = ""
    if completed_analyses:
        previous_analysis_summary = f"\n已完成的分析：{', '.join(completed_analyses)}\n请避免重复，提供新的洞察。\n"

    prompt = f"""你是一个数据分析专家。请根据以下 CSV 文件信息，编写 Python 代码完成特定的分析任务。

CSV 文件信息：
- 文件路径变量: csv_path (已在环境中定义)
- 行数: {csv_info['rows']}
- 列名: {csv_info['columns']}
- 数据类型: {csv_info['dtypes']}
- 数值列: {csv_info.get('summary', {}).get('numeric_cols', [])}
- 前3行示例数据: {csv_info['sample_data']}
{previous_analysis_summary}
本轮分析任务：{current_task}

要求：
- 只输出可执行的 Python 代码，不要有任何解释说明
- 针对"{current_task}"这个任务编写专门的分析代码
- 代码要包含详细的 print 语句来输出分析结果
- 使用 csv_path 变量作为文件路径
- 不要使用任何可视化库（matplotlib、seaborn等）
- 确保代码可以直接执行
- 输出要清晰、有结构，便于阅读
"""

    try:
        # 调用 LLM
        client = get_dashscope_client()
        model_name = get_model_name()

        print(f"正在调用 {model_name} 生成代码...")

        completion = client.chat.completions.create(
            model=model_name,
            messages=[
                {'role': 'system', 'content': '你是一个数据分析专家，擅长编写针对性的 Python 数据分析代码。'},
                {'role': 'user', 'content': prompt}
            ],
        )

        raw_response = completion.choices[0].message.content
        generated_code = extract_code_from_llm_response(raw_response)

        print(f"✓ 成功生成代码 ({len(generated_code)} 字符)")
        print(f"Token 消耗: {completion.usage}")

        return {
            **state,
            "current_round": current_round,
            "prompt": prompt,
            "generated_code": generated_code,
            "messages": state["messages"] + [f"第{current_round}轮：生成了 {current_task} 的分析代码"]
        }

    except Exception as e:
        error_msg = f"LLM 调用失败: {str(e)}"
        print(f"✗ {error_msg}")

        return {
            **state,
            "current_round": current_round,
            "prompt": prompt,
            "generated_code": "",
            "error": error_msg,
            "should_continue": False,
            "messages": state["messages"] + [error_msg]
        }


def execute_code_node(state: AnalysisState) -> AnalysisState:
    """
    节点3：执行生成的代码（支持多轮记录）

    Args:
        state: 当前状态

    Returns:
        AnalysisState: 更新后的状态
    """
    current_round = state.get("current_round", 1)

    print("\n" + "=" * 60)
    print(f"节点 3: 执行第 {current_round} 轮代码")
    print("=" * 60)

    # 如果前一个节点出错，直接返回
    if state.get("error"):
        print(f"✗ 跳过此节点，因为前面出错: {state['error']}")
        return state

    generated_code = state["generated_code"]
    csv_path = state["csv_path"]
    analysis_plan = state.get("analysis_plan", [])

    if not generated_code:
        error_msg = "没有生成的代码可执行"
        print(f"✗ {error_msg}")
        return {
            **state,
            "error": error_msg,
            "should_continue": False,
            "messages": state["messages"] + [error_msg]
        }

    try:
        print("正在执行代码...")
        print("-" * 60)

        # 执行代码
        result = execute_code_safely(generated_code, csv_path)

        if result["success"]:
            print("✓ 代码执行成功")
            print("\n执行输出：")
            print(result["output"])
        else:
            print("✗ 代码执行失败")
            print(f"错误: {result['error']}")

        # 记录本轮分析结果
        current_task = analysis_plan[current_round - 1] if current_round <= len(analysis_plan) else "未知任务"

        round_record = {
            "round": current_round,
            "task": current_task,
            "code": generated_code,
            "execution_result": result,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # 添加到分析轮次列表
        analysis_rounds = state.get("analysis_rounds", [])
        analysis_rounds.append(round_record)

        # 更新已完成的分析列表
        completed_analyses = state.get("completed_analyses", [])
        if result["success"]:
            completed_analyses.append(current_task)

        return {
            **state,
            "execution_result": result,
            "analysis_rounds": analysis_rounds,
            "completed_analyses": completed_analyses,
            "error": None if result["success"] else result["error"],
            "has_execution_error": not result["success"],  # 标记是否有错误
            "messages": state["messages"] + [
                f"第{current_round}轮执行成功" if result["success"] else f"第{current_round}轮执行失败: {result['error']}"
            ]
        }

    except Exception as e:
        error_msg = f"执行代码时出错: {str(e)}"
        print(f"✗ {error_msg}")

        return {
            **state,
            "execution_result": {"success": False, "error": str(e)},
            "error": error_msg,
            "has_execution_error": True,
            "should_continue": False,
            "messages": state["messages"] + [error_msg]
        }


def handle_error_node(state: AnalysisState) -> AnalysisState:
    """
    节点3.5：处理代码执行错误

    根据用户选择：自动修复、手动修复暂停、或跳过本轮

    Args:
        state: 当前状态

    Returns:
        AnalysisState: 更新后的状态
    """
    print("\n" + "=" * 80)
    print("节点 3.5: 处理执行错误")
    print("=" * 80)

    # 获取错误信息
    error_info = get_error_info(state)
    current_round = state.get("current_round", 0)
    error_retry_count = state.get("error_retry_count", 0)
    MAX_RETRY = 3

    # 显示错误信息
    print(get_user_choice_prompt(error_info))

    # 这里简化处理：由于是CLI环境，我们提供一个合理的默认策略
    # 实际应用中可以通过API参数或配置文件来设置策略

    # 默认策略：先尝试自动修复，如果重试次数超过限制则跳过
    if error_retry_count < MAX_RETRY:
        user_choice = "auto_fix"
        print(f">>> 自动选择：尝试自动修复（第 {error_retry_count + 1}/{MAX_RETRY} 次重试）")
    else:
        user_choice = "skip"
        print(f">>> 自动选择：已达到最大重试次数，跳过本轮")

    # 处理用户选择
    if user_choice == "auto_fix":
        # 自动修复
        print("\n正在调用 LLM 分析并修复代码...")

        success, fixed_code, explanation = analyze_and_fix_code(
            original_code=state.get("generated_code", ""),
            error_msg=error_info.get("error", ""),
            task_description=error_info.get("current_task", ""),
            csv_info=state.get("csv_info", {})
        )

        if success:
            print(f"✓ 代码修复成功: {explanation}")
            print("\n修复后的代码:")
            print("-" * 80)
            print(fixed_code[:300] + "..." if len(fixed_code) > 300 else fixed_code)
            print("-" * 80)

            # 更新状态，准备重新执行
            return {
                **state,
                "generated_code": fixed_code,
                "error": None,
                "has_execution_error": False,
                "error_retry_count": error_retry_count + 1,
                "user_intervention_mode": "auto_fix",
                "messages": state["messages"] + [f"第{current_round}轮代码已自动修复，准备重新执行"]
            }
        else:
            print(f"✗ 自动修复失败: {explanation}")
            print("跳过本轮分析")

            return {
                **state,
                "has_execution_error": False,
                "error_retry_count": 0,
                "user_intervention_mode": "skip",
                "messages": state["messages"] + [f"第{current_round}轮自动修复失败，跳过本轮"]
            }

    elif user_choice == "manual_fix":
        # 手动修复 - 保存状态并暂停
        print("\n正在保存状态...")

        state_file = save_state(state)

        print(f"✓ 状态已保存到: {state_file}")
        print("\n请修复代码后使用以下命令恢复分析：")
        print(f"  resume_analysis('{state_file}', fixed_code='您修复后的代码')")

        return {
            **state,
            "paused_for_fix": True,
            "should_continue": False,
            "user_intervention_mode": "manual_fix",
            "messages": state["messages"] + [f"第{current_round}轮暂停，等待手动修复"]
        }

    else:  # skip
        # 跳过本轮
        print(f"\n跳过第 {current_round} 轮分析，继续下一轮")

        return {
            **state,
            "has_execution_error": False,
            "error": None,
            "error_retry_count": 0,
            "user_intervention_mode": "skip",
            "messages": state["messages"] + [f"第{current_round}轮执行失败，已跳过"]
        }


def update_temp_report_node(state: AnalysisState) -> AnalysisState:
    """
    节点4：更新临时 Markdown 报告

    Args:
        state: 当前状态

    Returns:
        AnalysisState: 更新后的状态
    """
    current_round = state.get("current_round", 0)

    print("\n" + "=" * 60)
    print(f"节点 4: 更新临时报告（第 {current_round} 轮）")
    print("=" * 60)

    try:
        # 更新临时报告
        temp_report_path = update_temp_markdown(state)

        print(f"✓ 临时报告已更新: {temp_report_path}")

        return {
            **state,
            "temp_report_path": temp_report_path,
            "messages": state["messages"] + [f"第{current_round}轮分析结果已写入临时报告"]
        }

    except Exception as e:
        error_msg = f"更新临时报告失败: {str(e)}"
        print(f"⚠️  {error_msg}")

        # 报告更新失败不影响继续分析
        return {
            **state,
            "messages": state["messages"] + [error_msg]
        }


def decide_continue_node(state: AnalysisState) -> AnalysisState:
    """
    节点5：LLM 智能判断是否继续分析

    Args:
        state: 当前状态

    Returns:
        AnalysisState: 更新后的状态（包含 should_continue 标志）
    """
    print("\n" + "=" * 60)
    print("节点 5: 判断是否继续分析")
    print("=" * 60)

    # 如果出错，停止分析
    if state.get("error"):
        print("✗ 由于错误，停止分析")
        return {
            **state,
            "should_continue": False
        }

    current_round = state.get("current_round", 0)
    analysis_plan = state.get("analysis_plan", [])
    completed_analyses = state.get("completed_analyses", [])
    analysis_rounds = state.get("analysis_rounds", [])

    # 检查是否达到最大轮次（防止无限循环）
    MAX_ROUNDS = 5
    if current_round >= MAX_ROUNDS:
        print(f"✓ 已达到最大轮次 ({MAX_ROUNDS})，停止分析")
        return {
            **state,
            "should_continue": False,
            "messages": state["messages"] + [f"已完成 {MAX_ROUNDS} 轮分析，达到上限"]
        }

    # 检查是否完成所有计划任务
    if current_round >= len(analysis_plan):
        print("✓ 所有计划任务已完成")
        return {
            **state,
            "should_continue": False,
            "messages": state["messages"] + ["所有计划任务已完成"]
        }

    # 构建决策提示词
    recent_results = []
    for round_data in analysis_rounds[-2:]:  # 最近2轮
        recent_results.append({
            "task": round_data["task"],
            "success": round_data["execution_result"].get("success"),
            "output_preview": round_data["execution_result"].get("output", "")[:200]
        })

    decision_prompt = f"""你是一个数据分析决策专家。请判断是否需要继续进行下一轮分析。

当前情况：
- 已完成轮次: {current_round}/{len(analysis_plan)}
- 计划任务: {analysis_plan}
- 已完成任务: {completed_analyses}
- 剩余任务: {analysis_plan[current_round:]}

最近分析结果：
{json.dumps(recent_results, ensure_ascii=False, indent=2)}

请判断：
1. 如果还有重要的未完成任务，且数据仍有分析价值 → 返回 "continue"
2. 如果已完成所有关键分析，或继续分析价值不大 → 返回 "stop"

只返回 JSON 格式: {{"decision": "continue"}} 或 {{"decision": "stop", "reason": "原因"}}
"""

    try:
        # 调用 LLM 做决策
        client = get_dashscope_client()
        model_name = get_model_name()

        print(f"正在调用 {model_name} 做决策...")

        completion = client.chat.completions.create(
            model=model_name,
            messages=[
                {'role': 'system', 'content': '你是一个数据分析决策专家。只返回JSON格式的决策结果。'},
                {'role': 'user', 'content': decision_prompt}
            ],
        )

        raw_response = completion.choices[0].message.content

        # 解析决策
        try:
            if "```json" in raw_response:
                json_str = raw_response.split("```json")[1].split("```")[0].strip()
            elif "```" in raw_response:
                json_str = raw_response.split("```")[1].split("```")[0].strip()
            else:
                json_str = raw_response.strip()

            decision_data = json.loads(json_str)
            decision = decision_data.get("decision", "continue")
            reason = decision_data.get("reason", "")

        except json.JSONDecodeError:
            # 解析失败，默认继续
            print("⚠️  决策解析失败，默认继续分析")
            decision = "continue"
            reason = ""

        should_continue = (decision == "continue")

        if should_continue:
            print("✓ LLM 决定：继续下一轮分析")
        else:
            print(f"✓ LLM 决定：停止分析")
            if reason:
                print(f"  原因: {reason}")

        return {
            **state,
            "should_continue": should_continue,
            "messages": state["messages"] + [
                "LLM决策：继续分析" if should_continue else f"LLM决策：停止分析 ({reason})"
            ]
        }

    except Exception as e:
        # 决策失败，默认继续（但不超过计划）
        print(f"⚠️  决策失败: {str(e)}，根据计划继续")

        should_continue = current_round < len(analysis_plan)

        return {
            **state,
            "should_continue": should_continue,
            "messages": state["messages"] + [f"决策失败，根据计划{'继续' if should_continue else '停止'}"]
        }


def final_summary_node(state: AnalysisState) -> AnalysisState:
    """
    节点6：生成最终综合报告

    Args:
        state: 当前状态

    Returns:
        AnalysisState: 更新后的状态
    """
    print("\n" + "=" * 60)
    print("节点 6: 生成最终综合报告")
    print("=" * 60)

    try:
        # 生成最终报告
        final_report_path = generate_final_report(state)

        print(f"✓ 最终报告已生成: {final_report_path}")

        total_rounds = state.get("current_round", 0)
        completed_count = len(state.get("completed_analyses", []))

        summary_msg = f"分析完成！共进行 {total_rounds} 轮分析，成功完成 {completed_count} 个任务"

        print("\n" + "=" * 60)
        print("分析总结")
        print("=" * 60)
        print(summary_msg)
        print(f"临时报告: {state.get('temp_report_path', 'N/A')}")
        print(f"最终报告: {final_report_path}")
        print("=" * 60)

        return {
            **state,
            "messages": state["messages"] + [summary_msg, f"最终报告: {final_report_path}"]
        }

    except Exception as e:
        error_msg = f"生成最终报告失败: {str(e)}"
        print(f"✗ {error_msg}")

        return {
            **state,
            "error": error_msg,
            "messages": state["messages"] + [error_msg]
        }


def read_excel_info_node(state: AnalysisState) -> AnalysisState:
    """
    节点：读取 Excel 文件信息

    Args:
        state: 当前状态

    Returns:
        AnalysisState: 更新后的状态
    """
    print("=" * 60)
    print("节点: 读取 Excel 文件信息")
    print("=" * 60)

    excel_path = state.get("excel_path", "")

    if not excel_path:
        error_msg = "未提供 Excel 文件路径"
        print(f"✗ {error_msg}")
        return {
            **state,
            "excel_info": {},
            "error": error_msg,
            "messages": state.get("messages", []) + [error_msg]
        }

    try:
        # 获取所有 sheet 名称
        excel_file = pd.ExcelFile(excel_path)
        sheet_names = excel_file.sheet_names

        # 存储每个 sheet 的详细信息
        sheets_data = {}
        total_rows = 0

        # 遍历所有 sheet 并提取详细信息
        for sheet_name in sheet_names:
            df = pd.read_excel(excel_path, sheet_name=sheet_name)

            # 获取数值列和分类列
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()

            # 基本统计信息（仅数值列）
            basic_stats = {}
            if numeric_cols:
                desc = df[numeric_cols].describe()
                basic_stats = desc.to_dict()

            # 分类列的值分布（前5个最常见的值）
            categorical_distributions = {}
            for col in categorical_cols[:5]:  # 限制最多5个分类列以避免信息过载
                value_counts = df[col].value_counts().head(5).to_dict()
                categorical_distributions[col] = value_counts

            # 构建该 sheet 的详细信息
            sheet_info = {
                "rows": len(df),
                "columns": list(df.columns),
                "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
                "sample_data": df.head(10).to_dict('records'),  # 增加到10行
                "summary": {
                    "null_counts": df.isnull().sum().to_dict(),
                    "numeric_cols": numeric_cols,
                    "categorical_cols": categorical_cols,
                    "basic_stats": basic_stats,
                    "categorical_distributions": categorical_distributions
                }
            }

            sheets_data[sheet_name] = sheet_info
            total_rows += len(df)

        # 构建完整的 Excel 信息
        excel_info = {
            "file_path": excel_path,
            "total_sheets": len(sheet_names),
            "sheet_names": sheet_names,
            "sheets": sheets_data
        }

        # 打印详细信息
        print(f"✓ 成功读取 Excel 文件: {excel_path}")
        print(f"  - Sheet 数量: {len(sheet_names)}")
        print(f"  - 总行数（所有 sheet）: {total_rows}")
        print()

        for sheet_name, sheet_data in sheets_data.items():
            print(f"  📊 Sheet: '{sheet_name}'")
            print(f"     - 行数: {sheet_data['rows']}")
            print(f"     - 列数: {len(sheet_data['columns'])}")
            print(f"     - 列名: {sheet_data['columns']}")
            print(f"     - 数值列: {sheet_data['summary']['numeric_cols']}")
            print(f"     - 分类列: {sheet_data['summary']['categorical_cols']}")
            print()

        return {
            **state,
            "excel_info": excel_info,
            "error": None,
            "messages": state.get("messages", []) + [
                f"成功读取 Excel 文件，包含 {len(sheet_names)} 个 sheet，总共 {total_rows} 行数据"
            ]
        }

    except Exception as e:
        error_msg = f"读取 Excel 文件失败: {str(e)}"
        print(f"✗ {error_msg}")

        return {
            **state,
            "excel_info": {},
            "error": error_msg,
            "messages": state.get("messages", []) + [error_msg]
        }


def summarize_node(state: AnalysisState) -> AnalysisState:
    """
    节点4：总结分析结果

    Args:
        state: 当前状态

    Returns:
        AnalysisState: 更新后的状态
    """
    print("\n" + "=" * 60)
    print("节点 4: 总结分析结果")
    print("=" * 60)

    messages = state["messages"]
    execution_result = state.get("execution_result", {})

    summary = "\n分析流程总结:\n"
    summary += "-" * 60 + "\n"

    for i, msg in enumerate(messages, 1):
        summary += f"{i}. {msg}\n"

    if execution_result.get("success"):
        summary += "\n✓ 数据分析完成！\n"
    else:
        summary += "\n✗ 分析过程中遇到错误\n"

    print(summary)

    return {
        **state,
        "messages": messages + ["分析流程已完成"]
    }
