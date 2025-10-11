"""
Unit tests for LangGraph nodes
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import os

from backend.app.nodes.code_analysis_nodes import (
    AnalysisState,
    read_csv_info_node,
    read_excel_info_node,
    generate_code_node,
    execute_code_node,
    summarize_node,
)


@pytest.fixture
def sample_csv_path(tmp_path):
    """Create a temporary CSV file for testing"""
    csv_file = tmp_path / "test.csv"
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "score": [85.5, 92.0, 78.5]
    })
    df.to_csv(csv_file, index=False)
    return str(csv_file)


@pytest.fixture
def sample_excel_path(tmp_path):
    """Create a temporary Excel file for testing"""
    excel_file = tmp_path / "test.xlsx"

    # Create first sheet
    df1 = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "score": [85.5, 92.0, 78.5]
    })

    # Create second sheet
    df2 = pd.DataFrame({
        "product": ["A", "B", "C"],
        "price": [100, 200, 300]
    })

    # Write to Excel with multiple sheets
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        df1.to_excel(writer, sheet_name='Sheet1', index=False)
        df2.to_excel(writer, sheet_name='Sheet2', index=False)

    return str(excel_file)


@pytest.fixture
def initial_state(sample_csv_path):
    """Create initial state for testing"""
    return {
        "csv_path": sample_csv_path,
        "csv_info": {},
        "prompt": "",
        "generated_code": "",
        "execution_result": {},
        "error": None,
        "messages": []
    }


class TestReadCsvInfoNode:
    """Tests for read_csv_info_node"""

    def test_read_csv_info_success(self, initial_state):
        """Test successful CSV reading"""
        result = read_csv_info_node(initial_state)

        assert result["error"] is None
        assert "csv_info" in result
        assert result["csv_info"]["rows"] == 3
        assert result["csv_info"]["columns"] == ["id", "name", "age", "score"]
        assert len(result["messages"]) == 1
        assert "成功读取" in result["messages"][0]
    def test_read_csv_info_contains_metadata(self, initial_state):
        """Test that CSV info contains required metadata"""
        result = read_csv_info_node(initial_state)
    
        csv_info = result["csv_info"]
        assert "file_path" in csv_info
        assert "rows" in csv_info
        assert "columns" in csv_info
        assert "dtypes" in csv_info
        assert "sample_data" in csv_info
        assert "summary" in csv_info
        assert "null_counts" in csv_info["summary"]
        assert "numeric_cols" in csv_info["summary"]


class TestReadExcelInfoNode:
    """Tests for read_excel_info_node"""

    def test_read_excel_info_success(self, sample_excel_path):
        """Test successful Excel reading"""
        state = {
            "csv_path": "",
            "excel_path": sample_excel_path,
            "csv_info": {},
            "excel_info": {},
            "prompt": "",
            "generated_code": "",
            "execution_result": {},
            "error": None,
            "messages": []
        }
        result = read_excel_info_node(state)

        assert result["error"] is None
        assert "excel_info" in result
        assert result["excel_info"]["total_sheets"] == 2
        assert result["excel_info"]["sheet_names"] == ["Sheet1", "Sheet2"]

        # Check Sheet1 data
        sheet1 = result["excel_info"]["sheets"]["Sheet1"]
        assert sheet1["rows"] == 3
        assert sheet1["columns"] == ["id", "name", "age", "score"]

        assert len(result["messages"]) == 1
        assert "成功读取" in result["messages"][0]

    def test_read_excel_info_contains_metadata(self, sample_excel_path):
        """Test that Excel info contains required metadata"""
        state = {
            "csv_path": "",
            "excel_path": sample_excel_path,
            "csv_info": {},
            "excel_info": {},
            "prompt": "",
            "generated_code": "",
            "execution_result": {},
            "error": None,
            "messages": []
        }
        result = read_excel_info_node(state)

        excel_info = result["excel_info"]
        # Check top-level structure
        assert "file_path" in excel_info
        assert "total_sheets" in excel_info
        assert "sheet_names" in excel_info
        assert "sheets" in excel_info

        # Check each sheet has required metadata
        for sheet_name in excel_info["sheet_names"]:
            sheet_data = excel_info["sheets"][sheet_name]
            assert "rows" in sheet_data
            assert "columns" in sheet_data
            assert "dtypes" in sheet_data
            assert "sample_data" in sheet_data
            assert "summary" in sheet_data
            assert "null_counts" in sheet_data["summary"]
            assert "numeric_cols" in sheet_data["summary"]
            assert "categorical_cols" in sheet_data["summary"]
            assert "basic_stats" in sheet_data["summary"]
            assert "categorical_distributions" in sheet_data["summary"]

    def test_read_excel_info_multiple_sheets(self, sample_excel_path):
        """Test handling of Excel files with multiple sheets"""
        state = {
            "csv_path": "",
            "excel_path": sample_excel_path,
            "csv_info": {},
            "excel_info": {},
            "prompt": "",
            "generated_code": "",
            "execution_result": {},
            "error": None,
            "messages": []
        }
        result = read_excel_info_node(state)

        assert result["error"] is None
        excel_info = result["excel_info"]
        assert excel_info["total_sheets"] == 2
        assert len(excel_info["sheet_names"]) == 2
        assert "Sheet1" in excel_info["sheet_names"]
        assert "Sheet2" in excel_info["sheet_names"]

        # Verify both sheets have data
        assert "Sheet1" in excel_info["sheets"]
        assert "Sheet2" in excel_info["sheets"]

        # Verify Sheet1 structure
        sheet1 = excel_info["sheets"]["Sheet1"]
        assert sheet1["rows"] == 3
        assert len(sheet1["columns"]) == 4

        # Verify Sheet2 structure
        sheet2 = excel_info["sheets"]["Sheet2"]
        assert sheet2["rows"] == 3
        assert len(sheet2["columns"]) == 2
        assert sheet2["columns"] == ["product", "price"]

    def test_read_excel_info_no_path(self):
        """Test error handling when excel_path is empty"""
        state = {
            "csv_path": "",
            "excel_path": "",
            "csv_info": {},
            "excel_info": {},
            "prompt": "",
            "generated_code": "",
            "execution_result": {},
            "error": None,
            "messages": []
        }
        result = read_excel_info_node(state)

        assert result["error"] is not None
        assert "未提供 Excel 文件路径" in result["error"]
        assert result["excel_info"] == {}

    def test_read_excel_info_file_not_found(self):
        """Test error handling for non-existent files"""
        state = {
            "csv_path": "",
            "excel_path": "/nonexistent/path/file.xlsx",
            "csv_info": {},
            "excel_info": {},
            "prompt": "",
            "generated_code": "",
            "execution_result": {},
            "error": None,
            "messages": []
        }
        result = read_excel_info_node(state)

        assert result["error"] is not None
        assert "读取 Excel 文件失败" in result["error"]
        assert result["excel_info"] == {}

    def test_read_excel_info_invalid_file(self, tmp_path):
        """Test error handling for corrupted/invalid Excel files"""
        # Create an invalid Excel file (just text content)
        invalid_excel = tmp_path / "invalid.xlsx"
        invalid_excel.write_text("This is not a valid Excel file")

        state = {
            "csv_path": "",
            "excel_path": str(invalid_excel),
            "csv_info": {},
            "excel_info": {},
            "prompt": "",
            "generated_code": "",
            "execution_result": {},
            "error": None,
            "messages": []
        }
        result = read_excel_info_node(state)

        assert result["error"] is not None
        assert "读取 Excel 文件失败" in result["error"]
        assert result["excel_info"] == {}


class TestGenerateCodeNode:
    """Tests for generate_code_node"""

    @pytest.fixture
    def state_with_csv_info(self, initial_state):
        """State after CSV has been read"""
        result = read_csv_info_node(initial_state)
        return result

    def test_generate_code_skips_on_error(self):
        """Test that generate_code_node skips if previous error exists"""
        state = {
            "csv_path": "test.csv",
            "csv_info": {},
            "prompt": "",
            "generated_code": "",
            "execution_result": {},
            "error": "Previous error",
            "messages": ["Error occurred"]
        }
        result = generate_code_node(state)

        assert result["error"] == "Previous error"
        assert result["generated_code"] == ""

    @patch("backend.app.nodes.code_analysis_nodes.get_dashscope_client")
    @patch("backend.app.nodes.code_analysis_nodes.get_model_name")
    def test_generate_code_success(self, mock_get_model, mock_get_client, state_with_csv_info):
        """Test successful code generation with mocked LLM"""
        # Mock LLM response
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = """```python
import pandas as pd
df = pd.read_csv(csv_path)
print(df.describe())
```"""
        mock_response.usage = {"prompt_tokens": 100, "completion_tokens": 50}

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_get_client.return_value = mock_client
        mock_get_model.return_value = "test-model"

        result = generate_code_node(state_with_csv_info)

        assert result["error"] is None
        assert len(result["generated_code"]) > 0
        assert "import pandas" in result["generated_code"]
        assert "LLM 成功生成" in result["messages"][-1]

    @patch("backend.app.nodes.code_analysis_nodes.get_dashscope_client")
    def test_generate_code_llm_failure(self, mock_get_client, state_with_csv_info):
        """Test LLM call failure"""
        mock_client = Mock()
        mock_client.chat.completions.create.side_effect = Exception("API Error")
        mock_get_client.return_value = mock_client

        result = generate_code_node(state_with_csv_info)

        assert result["error"] is not None
        assert "LLM 调用失败" in result["error"]
        assert result["generated_code"] == ""


class TestExecuteCodeNode:
    """Tests for execute_code_node"""

    @pytest.fixture
    def state_with_code(self, initial_state, sample_csv_path):
        """State with generated code"""
        return {
            **initial_state,
            "generated_code": f"""
import pandas as pd
df = pd.read_csv(csv_path)
print(f"Rows: {{len(df)}}")
print(df.columns.tolist())
""",
            "messages": ["CSV读取完成", "代码生成完成"]
        }

    def test_execute_code_skips_on_error(self):
        """Test that execute_code_node skips if previous error exists"""
        state = {
            "csv_path": "test.csv",
            "csv_info": {},
            "prompt": "",
            "generated_code": "print('test')",
            "execution_result": {},
            "error": "Previous error",
            "messages": []
        }
        result = execute_code_node(state)

        assert result["error"] == "Previous error"

    def test_execute_code_no_code(self):
        """Test execution with no generated code"""
        state = {
            "csv_path": "test.csv",
            "csv_info": {},
            "prompt": "",
            "generated_code": "",
            "execution_result": {},
            "error": None,
            "messages": []
        }
        result = execute_code_node(state)

        assert result["error"] is not None
        assert "没有生成的代码" in result["error"]

    def test_execute_code_success(self, state_with_code):
        """Test successful code execution"""
        result = execute_code_node(state_with_code)

        assert result["error"] is None
        assert result["execution_result"]["success"] is True
        assert "Rows:" in result["execution_result"]["output"]
        assert "代码执行成功" in result["messages"][-1]

    def test_execute_code_with_error(self, initial_state, sample_csv_path):
        """Test code execution with runtime error"""
        state = {
            **initial_state,
            "generated_code": "raise ValueError('Test error')",
            "messages": []
        }
        result = execute_code_node(state)

        assert result["execution_result"]["success"] is False
        assert "ValueError" in result["execution_result"]["error"]
        assert result["error"] is not None


class TestSummarizeNode:
    """Tests for summarize_node"""

    def test_summarize_success(self):
        """Test summarize node with successful execution"""
        state = {
            "csv_path": "test.csv",
            "csv_info": {},
            "prompt": "",
            "generated_code": "",
            "execution_result": {"success": True, "output": "Results here"},
            "error": None,
            "messages": ["Step 1", "Step 2", "Step 3"]
        }
        result = summarize_node(state)

        assert len(result["messages"]) == 4  # Original 3 + 1 new
        assert result["messages"][-1] == "分析流程已完成"

    def test_summarize_with_error(self):
        """Test summarize node with execution error"""
        state = {
            "csv_path": "test.csv",
            "csv_info": {},
            "prompt": "",
            "generated_code": "",
            "execution_result": {"success": False, "error": "Some error"},
            "error": "Execution failed",
            "messages": ["Step 1", "Error occurred"]
        }
        result = summarize_node(state)

        assert len(result["messages"]) == 3
        assert result["messages"][-1] == "分析流程已完成"

    def test_summarize_empty_execution_result(self):
        """Test summarize node with empty execution result"""
        state = {
            "csv_path": "test.csv",
            "csv_info": {},
            "prompt": "",
            "generated_code": "",
            "execution_result": {},
            "error": None,
            "messages": []
        }
        result = summarize_node(state)

        assert len(result["messages"]) == 1
        assert result["messages"][-1] == "分析流程已完成"


class TestIntegration:
    """Integration tests for node flow"""

    def test_full_workflow_without_llm(self, initial_state):
        """Test full workflow with mocked LLM"""
        # Step 1: Read CSV
        state = read_csv_info_node(initial_state)
        assert state["error"] is None

        # Step 2: Mock code generation (skip LLM call)
        state["generated_code"] = """
import pandas as pd
df = pd.read_csv(csv_path)
print(f"Shape: {df.shape}")
print(df.head())
"""
        state["messages"].append("代码已生成（测试）")

        # Step 3: Execute code
        state = execute_code_node(state)
        assert state["error"] is None
        assert state["execution_result"]["success"] is True

        # Step 4: Summarize
        state = summarize_node(state)
        assert "分析流程已完成" in state["messages"]

    def test_excel_workflow(self, sample_excel_path):
        """Test Excel workflow integration"""
        # Initial state for Excel
        initial_state = {
            "csv_path": "",
            "excel_path": sample_excel_path,
            "csv_info": {},
            "excel_info": {},
            "prompt": "",
            "generated_code": "",
            "execution_result": {},
            "error": None,
            "messages": []
        }

        # Step 1: Read Excel
        state = read_excel_info_node(initial_state)
        assert state["error"] is None
        assert state["excel_info"]["total_sheets"] == 2
        assert len(state["excel_info"]["sheet_names"]) == 2

        # Step 2: Verify Excel info propagation
        assert "excel_info" in state
        assert "sheets" in state["excel_info"]
        assert "Sheet1" in state["excel_info"]["sheets"]
        assert "Sheet2" in state["excel_info"]["sheets"]

        # Verify detailed info for Sheet1
        sheet1 = state["excel_info"]["sheets"]["Sheet1"]
        assert sheet1["rows"] == 3
        assert len(sheet1["summary"]["numeric_cols"]) > 0

        assert "成功读取 Excel 文件" in state["messages"][0]
