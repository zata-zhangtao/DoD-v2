"""
状态管理工具
支持分析状态的保存和加载，实现断点续传
"""
import json
from datetime import datetime
from typing import Dict, Any
import os


def save_state(state: Dict[str, Any], output_path: str = None) -> str:
    """
    保存分析状态到 JSON 文件

    Args:
        state: 分析状态字典
        output_path: 输出文件路径，如果为 None 则自动生成

    Returns:
        str: 保存的文件路径
    """
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_basename = os.path.basename(state.get("csv_path", "unknown"))
        csv_name = os.path.splitext(csv_basename)[0]
        output_path = f"analysis_state_{csv_name}_{timestamp}.json"

    # 准备可序列化的状态数据
    serializable_state = {
        "csv_path": state.get("csv_path", ""),
        "csv_info": state.get("csv_info", {}),
        "prompt": state.get("prompt", ""),
        "generated_code": state.get("generated_code", ""),
        "execution_result": state.get("execution_result", {}),
        "error": state.get("error"),
        "messages": state.get("messages", []),
        "analysis_rounds": state.get("analysis_rounds", []),
        "current_round": state.get("current_round", 0),
        "analysis_plan": state.get("analysis_plan", []),
        "completed_analyses": state.get("completed_analyses", []),
        "temp_report_path": state.get("temp_report_path", ""),
        "should_continue": state.get("should_continue", True),
        "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # 写入 JSON 文件
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(serializable_state, f, ensure_ascii=False, indent=2)

    return output_path


def load_state(state_file: str) -> Dict[str, Any]:
    """
    从 JSON 文件加载分析状态

    Args:
        state_file: 状态文件路径

    Returns:
        Dict: 分析状态字典
    """
    if not os.path.exists(state_file):
        raise FileNotFoundError(f"状态文件不存在: {state_file}")

    with open(state_file, 'r', encoding='utf-8') as f:
        state = json.load(f)

    return state


def get_error_info(state: Dict[str, Any]) -> Dict[str, str]:
    """
    提取当前状态的错误信息

    Args:
        state: 分析状态

    Returns:
        Dict: 包含错误代码和错误信息
    """
    return {
        "code": state.get("generated_code", ""),
        "error": state.get("error", ""),
        "execution_result": state.get("execution_result", {}),
        "current_round": state.get("current_round", 0),
        "current_task": (
            state.get("analysis_plan", [])[state.get("current_round", 1) - 1]
            if state.get("current_round", 0) > 0
            and state.get("current_round", 0) <= len(state.get("analysis_plan", []))
            else "未知任务"
        )
    }
