"""
代码分析节点模块
定义 LangGraph 工作流中的各个节点
"""
from typing import TypedDict, Optional
import pandas as pd
import os

from backend.app.core.config import get_dashscope_client, get_model_name
from backend.app.utils.code_executor import execute_code_safely, extract_code_from_llm_response


# 定义状态
class AnalysisState(TypedDict):
    csv_path: str              # CSV 文件路径
    csv_info: dict             # CSV 基本信息
    prompt: str                # 给 LLM 的提示词
    generated_code: str        # LLM 生成的代码
    execution_result: dict     # 代码执行结果
    error: Optional[str]       # 错误信息
    messages: list[str]        # 过程日志


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
            "messages": [f"成功读取 CSV 文件，包含 {csv_info['rows']} 行数据"]
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
            "messages": [error_msg]
        }


def generate_code_node(state: AnalysisState) -> AnalysisState:
    """
    节点2：使用 LLM 生成分析代码

    Args:
        state: 当前状态

    Returns:
        AnalysisState: 更新后的状态
    """
    print("\n" + "=" * 60)
    print("节点 2: 使用 LLM 生成分析代码")
    print("=" * 60)

    # 如果前一个节点出错，直接返回
    if state.get("error"):
        print(f"✗ 跳过此节点，因为前面出错: {state['error']}")
        return state

    csv_info = state["csv_info"]

    # 构建提示词
    prompt = f"""你是一个数据分析专家。请根据以下 CSV 文件信息，编写 Python 代码进行数据分析。

CSV 文件信息：
- 文件路径变量: csv_path (已在环境中定义)
- 行数: {csv_info['rows']}
- 列名: {csv_info['columns']}
- 数据类型: {csv_info['dtypes']}
- 前3行示例数据: {csv_info['sample_data']}

请编写代码完成以下分析：
1. 使用 pandas 读取 CSV 文件（使用变量 csv_path）
2. 显示数据的基本统计信息
3. 对数值列进行统计分析（均值、中位数、标准差等）
4. 检查是否有缺失值
5. 如果有分类列，统计各类别的分布

要求：
- 只输出可执行的 Python 代码，不要有任何解释说明
- 代码要包含 print 语句来输出分析结果
- 使用 csv_path 变量作为文件路径
- 不要使用任何可视化库（matplotlib、seaborn等）
- 确保代码可以直接执行
"""

    try:
        # 调用 LLM
        client = get_dashscope_client()
        model_name = get_model_name()

        print(f"正在调用 {model_name} 生成代码...")

        completion = client.chat.completions.create(
            model=model_name,
            messages=[
                {'role': 'system', 'content': '你是一个数据分析专家，擅长编写 Python 数据分析代码。'},
                {'role': 'user', 'content': prompt}
            ],
        )

        raw_response = completion.choices[0].message.content
        generated_code = extract_code_from_llm_response(raw_response)

        print(f"✓ 成功生成代码 ({len(generated_code)} 字符)")
        print(f"Token 消耗: {completion.usage}")

        return {
            **state,
            "prompt": prompt,
            "generated_code": generated_code,
            "messages": state["messages"] + [f"LLM 成功生成了 {len(generated_code)} 字符的分析代码"]
        }

    except Exception as e:
        error_msg = f"LLM 调用失败: {str(e)}"
        print(f"✗ {error_msg}")

        return {
            **state,
            "prompt": prompt,
            "generated_code": "",
            "error": error_msg,
            "messages": state["messages"] + [error_msg]
        }


def execute_code_node(state: AnalysisState) -> AnalysisState:
    """
    节点3：执行生成的代码

    Args:
        state: 当前状态

    Returns:
        AnalysisState: 更新后的状态
    """
    print("\n" + "=" * 60)
    print("节点 3: 执行生成的代码")
    print("=" * 60)

    # 如果前一个节点出错，直接返回
    if state.get("error"):
        print(f"✗ 跳过此节点，因为前面出错: {state['error']}")
        return state

    generated_code = state["generated_code"]
    csv_path = state["csv_path"]

    if not generated_code:
        error_msg = "没有生成的代码可执行"
        print(f"✗ {error_msg}")
        return {
            **state,
            "error": error_msg,
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

        return {
            **state,
            "execution_result": result,
            "error": None if result["success"] else result["error"],
            "messages": state["messages"] + [
                "代码执行成功" if result["success"] else f"代码执行失败: {result['error']}"
            ]
        }

    except Exception as e:
        error_msg = f"执行代码时出错: {str(e)}"
        print(f"✗ {error_msg}")

        return {
            **state,
            "execution_result": {"success": False, "error": str(e)},
            "error": error_msg,
            "messages": state["messages"] + [error_msg]
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
