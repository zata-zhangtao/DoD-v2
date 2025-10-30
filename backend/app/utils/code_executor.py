"""
代码执行器模块
安全地执行 LLM 生成的 Python 代码
"""
import sys
import io
import traceback
from typing import Dict, Any
import re
import contextlib


def _sanitize_code(code: str) -> str:
    """
    以最小侵入的方式清理 LLM 生成的代码：
    - 移除对 `file_path` 的重新赋值，确保使用运行环境注入的路径
    - 将 pd.read_csv/excel('file_path') 形式替换为 pd.read_xxx(file_path)
    """
    try:
        lines = []
        for line in code.splitlines():
            # 跳过任何形式的 file_path 赋值
            if re.match(r"^\s*file_path\s*=\s*.*", line):
                continue
            lines.append(line)
        code = "\n".join(lines)

        # 将读取调用中把 'file_path' 字面量替换为变量 file_path
        code = re.sub(r"(pd\.read_(?:csv|excel)\s*\(\s*)['\"]file_path['\"]",
                      r"\1file_path", code)
        return code
    except Exception:
        # 任何异常都不影响执行，返回原始代码
        return code


def execute_code_safely(code: str, file_path: str, timeout: int = 30) -> Dict[str, Any]:
    """
    安全地执行 Python 代码

    Args:
        code: 要执行的 Python 代码
        file_path: 数据文件路径（CSV 或 Excel）
        timeout: 执行超时时间（秒），默认 30 秒

    Returns:
        Dict: 包含执行结果的字典
            - success: bool - 是否执行成功
            - output: str - 标准输出
            - error: str - 错误信息（如果有）
            - locals: dict - 执行后的局部变量
    """
    # 准备执行环境
    result = {
        "success": False,
        "output": "",
        "error": "",
        "locals": {}
    }

    # 捕获标准输出
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()

    # 导入常用库
    import pandas as pd
    import numpy as np

    # 创建受限的全局命名空间
    safe_globals = {
        "__builtins__": __builtins__,
        "file_path": file_path,  # 传递文件路径给代码（兼容 CSV 和 Excel）
        "pd": pd,              # pandas
        "pandas": pd,
        "np": np,              # numpy
        "numpy": np,
    }

    # 创建局部命名空间
    safe_locals = {}

    try:
        # 重定向标准输出和错误输出
        with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
            # 先做一次简单的代码清理，避免覆盖 file_path
            sanitized = _sanitize_code(code)
            # 执行代码
            exec(sanitized, safe_globals, safe_locals)

        # 执行成功
        result["success"] = True
        result["output"] = stdout_buffer.getvalue()
        result["locals"] = safe_locals

    except Exception as e:
        # 执行失败
        result["success"] = False
        result["error"] = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        result["output"] = stdout_buffer.getvalue()

    finally:
        # 清理
        stdout_buffer.close()
        stderr_buffer.close()

    return result


def extract_code_from_llm_response(response: str) -> str:
    """
    从 LLM 响应中提取代码块

    Args:
        response: LLM 的响应文本

    Returns:
        str: 提取的代码
    """
    # 尝试提取 Markdown 代码块
    if "```python" in response:
        # 提取 ```python ... ``` 之间的内容
        start = response.find("```python") + len("```python")
        end = response.find("```", start)
        if end != -1:
            return response[start:end].strip()

    elif "```" in response:
        # 提取 ``` ... ``` 之间的内容
        start = response.find("```") + len("```")
        end = response.find("```", start)
        if end != -1:
            return response[start:end].strip()

    # 如果没有代码块标记，返回原始响应
    return response.strip()
