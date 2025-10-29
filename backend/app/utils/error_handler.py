"""
错误处理工具
分析代码错误并尝试自动修复
"""
from typing import Dict, Any, Tuple
from app.core.config import get_dashscope_client, get_model_name
import json


def analyze_and_fix_code(
    original_code: str,
    error_msg: str,
    task_description: str,
    csv_info: Dict[str, Any]
) -> Tuple[bool, str, str]:
    """
    使用 LLM 分析错误并尝试修复代码

    Args:
        original_code: 原始出错的代码
        error_msg: 错误信息
        task_description: 任务描述
        csv_info: CSV 文件信息

    Returns:
        Tuple[bool, str, str]: (是否修复成功, 修复后的代码, 修复说明)
    """
    # 构建修复提示词
    fix_prompt = f"""你是一个 Python 代码调试专家。以下代码执行时出现了错误，请分析并修复。

任务描述：{task_description}

CSV 数据信息：
- 列名: {csv_info.get('columns', [])}
- 数据类型: {csv_info.get('dtypes', {{}})}
- 数值列: {csv_info.get('summary', {{}}).get('numeric_cols', [])}

出错的代码：
```python
{original_code}
```

错误信息：
```
{error_msg}
```

请修复这段代码，注意：
1. 分析错误原因（如缺少导入、变量未定义、语法错误等）
2. 修复代码使其可以正常执行
3. 确保使用 csv_path 变量读取文件
4. 不要使用可视化库（matplotlib、seaborn等）
5. 如果需要导入库，代码中已经预定义了 pd（pandas）和 np（numpy）变量，可以直接使用，不需要 import
6. 只输出修复后的完整代码，不要有任何解释

返回 JSON 格式：
{{
    "fixed_code": "修复后的完整代码",
    "explanation": "简短的修复说明（1-2句话）"
}}
"""

    try:
        # 调用 LLM
        client = get_dashscope_client()
        model_name = get_model_name()

        completion = client.chat.completions.create(
            model=model_name,
            messages=[
                {'role': 'system', 'content': '你是一个 Python 代码调试专家，擅长分析和修复代码错误。只返回JSON格式的结果。'},
                {'role': 'user', 'content': fix_prompt}
            ],
        )

        raw_response = completion.choices[0].message.content

        # 解析 JSON 响应
        try:
            if "```json" in raw_response:
                json_str = raw_response.split("```json")[1].split("```")[0].strip()
            elif "```" in raw_response:
                json_str = raw_response.split("```")[1].split("```")[0].strip()
            else:
                json_str = raw_response.strip()

            fix_data = json.loads(json_str)
            fixed_code = fix_data.get("fixed_code", "")
            explanation = fix_data.get("explanation", "")

            if fixed_code:
                return True, fixed_code, explanation
            else:
                return False, "", "LLM 未返回修复后的代码"

        except json.JSONDecodeError:
            return False, "", "无法解析 LLM 响应"

    except Exception as e:
        return False, "", f"调用 LLM 失败: {str(e)}"


def get_user_choice_prompt(error_info: Dict[str, Any]) -> str:
    """
    生成用户选择提示信息

    Args:
        error_info: 错误信息字典

    Returns:
        str: 格式化的提示信息
    """
    prompt = f"""
{'='*80}
❌ 第 {error_info.get('current_round', '?')} 轮分析执行失败
{'='*80}

任务: {error_info.get('current_task', '未知任务')}

错误信息:
{error_info.get('error', '未知错误')}

出错代码:
{'-'*80}
{error_info.get('code', 'N/A')[:500]}...
{'-'*80}

请选择处理方式：
1. [自动修复] 让 LLM 分析错误并自动修复代码（最多重试 3 次）
2. [手动修复] 暂停分析，保存当前状态，等待您修复后恢复
3. [跳过本轮] 跳过当前任务，继续下一轮分析

"""
    return prompt
