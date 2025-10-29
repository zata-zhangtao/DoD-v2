"""
Markdown 报告生成工具
支持增量更新和最终报告生成
"""
from datetime import datetime
from typing import Dict, Any
import os


def update_temp_markdown(state: Dict[str, Any], output_dir: str = ".") -> str:
    """
    增量更新临时 Markdown 报告
    每轮分析后调用，追加最新一轮的分析结果

    Args:
        state: 分析状态字典
        output_dir: 输出目录

    Returns:
        str: 临时报告文件路径
    """
    # 确定临时报告路径
    temp_report_path = state.get("temp_report_path")

    if not temp_report_path:
        # 首次创建临时报告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_basename = os.path.basename(state.get("csv_path", "unknown"))
        csv_name = os.path.splitext(csv_basename)[0]
        temp_report_path = os.path.join(output_dir, f"analysis_temp_{csv_name}_{timestamp}.md")

        # 写入报告头部
        _write_report_header(temp_report_path, state)

    # 追加当前轮次的分析结果
    current_round = state.get("current_round", 0)
    if current_round > 0:
        _append_round_result(temp_report_path, state, current_round)

    return temp_report_path


def _write_report_header(file_path: str, state: Dict[str, Any]):
    """写入报告头部（仅首次创建时调用）"""
    csv_info = state.get("csv_info", {})

    content = []
    content.append("# 数据分析报告（进行中）\n")
    content.append(f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    content.append(f"**CSV 文件**: `{state.get('csv_path', 'N/A')}`")
    content.append(f"**状态**: 🔄 分析中...\n")
    content.append("---\n")

    # 数据集信息
    content.append("## 📊 数据集信息\n")
    if csv_info:
        content.append(f"- **总行数**: {csv_info.get('rows', 0)}")
        content.append(f"- **总列数**: {len(csv_info.get('columns', []))}")

        columns = csv_info.get('columns', [])
        dtypes = csv_info.get('dtypes', {})

        if columns:
            content.append("\n### 列信息\n")
            content.append("| 列名 | 数据类型 |")
            content.append("|------|----------|")
            for col in columns:
                dtype = dtypes.get(col, 'unknown')
                content.append(f"| {col} | {dtype} |")

        # 示例数据
        sample_data = csv_info.get('sample_data', [])
        if sample_data:
            content.append("\n### 数据示例 (前3行)\n")
            if len(sample_data) > 0:
                headers = list(sample_data[0].keys())
                content.append("| " + " | ".join(headers) + " |")
                content.append("|" + "|".join(["---" for _ in headers]) + "|")
                for row in sample_data:
                    values = [str(row.get(h, ''))[:50] for h in headers]  # 限制长度
                    content.append("| " + " | ".join(values) + " |")

    content.append("\n---\n")
    content.append("## 🔍 分析过程\n")

    # 写入文件
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(content))


def _append_round_result(file_path: str, state: Dict[str, Any], round_num: int):
    """追加一轮分析结果"""
    analysis_rounds = state.get("analysis_rounds", [])

    if round_num > len(analysis_rounds):
        return

    round_data = analysis_rounds[round_num - 1]

    content = []
    content.append(f"\n### 第 {round_num} 轮分析\n")
    content.append(f"**分析任务**: {round_data.get('task', 'N/A')}")
    content.append(f"**时间**: {round_data.get('timestamp', 'N/A')}\n")

    # 生成的代码
    content.append("#### 生成的代码\n")
    content.append("```python")
    content.append(round_data.get('code', ''))
    content.append("```\n")

    # 执行结果
    execution_result = round_data.get('execution_result', {})
    if execution_result.get('success'):
        content.append("#### ✅ 执行结果\n")
        content.append("```")
        content.append(execution_result.get('output', ''))
        content.append("```\n")
    else:
        content.append("#### ❌ 执行失败\n")
        content.append("```")
        content.append(execution_result.get('error', ''))
        content.append("```\n")

    content.append("---")

    # 追加到文件
    with open(file_path, 'a', encoding='utf-8') as f:
        f.write("\n".join(content) + "\n")


def generate_final_report(state: Dict[str, Any], output_dir: str = ".") -> str:
    """
    生成最终的完整综合报告
    包含所有轮次的总结和深度分析

    Args:
        state: 分析状态字典
        output_dir: 输出目录

    Returns:
        str: 最终报告文件路径
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_basename = os.path.basename(state.get("csv_path", "unknown"))
    csv_name = os.path.splitext(csv_basename)[0]
    final_report_path = os.path.join(output_dir, f"analysis_final_{csv_name}_{timestamp}.md")

    content = []

    # 标题和元信息
    content.append("# 📈 数据分析综合报告\n")
    content.append(f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    content.append(f"**CSV 文件**: `{state.get('csv_path', 'N/A')}`")
    content.append(f"**分析轮次**: {state.get('current_round', 0)} 轮")
    content.append(f"**状态**: ✅ 分析完成\n")
    content.append("---\n")

    # 1. 执行摘要
    content.append("## 📋 执行摘要\n")
    analysis_plan = state.get("analysis_plan", [])
    completed_analyses = state.get("completed_analyses", [])

    content.append("### 分析计划")
    for i, task in enumerate(analysis_plan, 1):
        status = "✅" if task in completed_analyses else "⏭️"
        content.append(f"{i}. {status} {task}")

    content.append(f"\n**完成度**: {len(completed_analyses)}/{len(analysis_plan)}\n")

    # 2. 数据集信息
    csv_info = state.get("csv_info", {})
    content.append("---\n")
    content.append("## 📊 数据集信息\n")

    if csv_info:
        content.append(f"- **总行数**: {csv_info.get('rows', 0)}")
        content.append(f"- **总列数**: {len(csv_info.get('columns', []))}")

        columns = csv_info.get('columns', [])
        dtypes = csv_info.get('dtypes', {})

        if columns:
            content.append("\n### 列信息\n")
            content.append("| 列名 | 数据类型 |")
            content.append("|------|----------|")
            for col in columns:
                dtype = dtypes.get(col, 'unknown')
                content.append(f"| {col} | {dtype} |")

        # 数据质量
        summary = csv_info.get('summary', {})
        if summary:
            content.append("\n### 数据质量概况\n")
            null_counts = summary.get('null_counts', {})
            total_nulls = sum(null_counts.values())

            content.append(f"- **总缺失值**: {total_nulls}")
            if total_nulls > 0:
                content.append("\n**各列缺失情况**:")
                for col, count in null_counts.items():
                    if count > 0:
                        percentage = (count / csv_info.get('rows', 1)) * 100
                        content.append(f"  - {col}: {count} ({percentage:.1f}%)")

            numeric_cols = summary.get('numeric_cols', [])
            if numeric_cols:
                content.append(f"\n- **数值列**: {', '.join(numeric_cols)}")

    # 3. 详细分析过程
    content.append("\n---\n")
    content.append("## 🔍 详细分析过程\n")

    analysis_rounds = state.get("analysis_rounds", [])
    for i, round_data in enumerate(analysis_rounds, 1):
        content.append(f"\n### 第 {i} 轮：{round_data.get('task', 'N/A')}\n")
        content.append(f"**时间**: {round_data.get('timestamp', 'N/A')}\n")

        # 代码
        content.append("#### 分析代码\n")
        content.append("```python")
        content.append(round_data.get('code', ''))
        content.append("```\n")

        # 结果
        execution_result = round_data.get('execution_result', {})
        if execution_result.get('success'):
            content.append("#### 执行结果\n")
            content.append("```")
            content.append(execution_result.get('output', ''))
            content.append("```\n")
        else:
            content.append("#### ❌ 执行失败\n")
            content.append(f"**错误**: {execution_result.get('error', '')}\n")

        content.append("---")

    # 4. 流程日志
    content.append("\n## 📝 流程日志\n")
    messages = state.get("messages", [])
    if messages:
        for i, msg in enumerate(messages, 1):
            content.append(f"{i}. {msg}")
    else:
        content.append("*无日志信息*")

    # 5. 最终状态
    content.append("\n---\n")
    content.append("## ✨ 分析总结\n")

    error = state.get("error")
    if error:
        content.append(f"**状态**: ❌ 存在错误")
        content.append(f"**错误信息**: {error}\n")
    else:
        content.append(f"**状态**: ✅ 全部成功")
        content.append(f"**总轮次**: {len(analysis_rounds)}")
        content.append(f"**完成任务**: {len(completed_analyses)}\n")

    content.append("\n---")
    content.append(f"\n*报告生成于 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

    # 写入文件
    with open(final_report_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(content))

    return final_report_path
