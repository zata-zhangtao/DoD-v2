"""
简洁的 Excel 多轮分析工作流测试
测试真实的 Excel 文件读取和工作流执行

运行方法：
    PYTHONPATH=/code/DoD_v2 uv run pytest backend/tests/test_excel_simple.py -v -s

或者在项目根目录运行：
    cd /code/DoD_v2
    PYTHONPATH=. uv run pytest backend/tests/test_excel_simple.py -v -s
"""
import pytest
import os
from pathlib import Path
import sys
# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from app.nodes.code_analysis_nodes import (
    read_excel_info_node,
    plan_analysis_node,
    generate_code_node,
    execute_code_node,
)


@pytest.fixture
def excel_path():
    """使用真实的测试 Excel 文件"""
    path = Path(__file__).parent.parent / "data" / "raw" / "test_data.xlsx"
    assert path.exists(), f"测试文件不存在: {path}"
    return str(path)


@pytest.fixture
def initial_excel_state(excel_path):
    """Excel 分析的初始状态"""
    return {
        "csv_path": "",
        "excel_path": excel_path,
        "csv_info": {},
        "excel_info": {},
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


class TestExcelReading:
    """测试 Excel 文件读取"""

    def test_read_excel_info(self, initial_excel_state):
        """测试读取真实 Excel 文件"""
        result = read_excel_info_node(initial_excel_state)

        # 验证基本结构
        assert result["error"] is None
        assert "excel_info" in result

        excel_info = result["excel_info"]
        assert excel_info["total_sheets"] == 2
        assert len(excel_info["sheet_names"]) == 2
        assert "员工信息" in excel_info["sheet_names"]
        assert "销售数据" in excel_info["sheet_names"]

        # 验证员工信息 sheet
        sheet1 = excel_info["sheets"]["员工信息"]
        assert sheet1["rows"] == 25
        assert "name" in sheet1["columns"]
        assert "age" in sheet1["columns"]
        assert "score" in sheet1["columns"]
        assert len(sheet1["summary"]["numeric_cols"]) > 0

        # 验证销售数据 sheet
        sheet2 = excel_info["sheets"]["销售数据"]
        assert sheet2["rows"] == 15
        assert "product_name" in sheet2["columns"]
        assert "price" in sheet2["columns"]

        print(f"✓ Excel 文件读取成功: {excel_info['total_sheets']} sheets")


class TestExcelWorkflow:
    """测试完整的 Excel 多轮分析工作流"""

    def test_excel_end_to_end_workflow(self, initial_excel_state):
        """端到端测试：Excel 读取 → 执行代码"""
        # Step 1: 读取 Excel
        state = read_excel_info_node(initial_excel_state)
        assert state["error"] is None
        assert state["excel_info"]["total_sheets"] == 2
        print("✓ Step 1: Excel 读取成功")

        # Step 2: 手动设置分析计划（模拟 plan_analysis_node）
        state["analysis_plan"] = ["基础统计分析", "数据分布分析"]
        state["current_round"] = 0
        print("✓ Step 2: 分析计划已设置")

        # Step 3: 手动设置生成的代码（跳过 LLM 调用）
        state["current_round"] = 1
        state["generated_code"] = """
import pandas as pd

# 读取 Excel 文件的所有 sheet
file_path_var = file_path
excel_data = pd.ExcelFile(file_path_var)

print("=" * 60)
print("Excel 文件基础统计分析")
print("=" * 60)

for sheet_name in excel_data.sheet_names:
    print(f"\\n【Sheet: {sheet_name}】")
    df = pd.read_excel(file_path_var, sheet_name=sheet_name)
    print(f"行数: {len(df)}, 列数: {len(df.columns)}")
    print(f"列名: {df.columns.tolist()}")

    # 数值列统计
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        print(f"\\n数值列统计:")
        print(df[numeric_cols].describe())
"""
        print("✓ Step 3: 分析代码已准备")

        # Step 4: 执行代码
        state = execute_code_node(state)
        assert state["error"] is None
        assert state["execution_result"]["success"] is True
        assert "Excel 文件基础统计分析" in state["execution_result"]["output"]
        assert "员工信息" in state["execution_result"]["output"]
        assert "销售数据" in state["execution_result"]["output"]
        print("✓ Step 4: 代码执行成功")
        print(f"\\n执行输出预览:\\n{state['execution_result']['output'][:200]}...")

        # 验证多轮分析记录
        assert len(state["analysis_rounds"]) == 1
        assert state["analysis_rounds"][0]["round"] == 1
        assert state["analysis_rounds"][0]["task"] == "基础统计分析"


if __name__ == "__main__":
    # 可以直接运行此文件进行快速测试
    pytest.main([__file__, "-v", "-s"])
