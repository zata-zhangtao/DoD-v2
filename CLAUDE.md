# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DoD_v2 is a LangGraph-based data analysis system that uses LLMs to automatically generate and execute Python code for CSV data analysis. The system uses Alibaba's Dashscope API (compatible with OpenAI API format) with the Qwen3-coder-plus model.

## Environment Setup

Create a `.env` file from `.env.example`:
```bash
cp .env.example .env
```

Required environment variables:
- `DASHSCOPE_API_KEY`: Alibaba Dashscope API key
- `DASHSCOPE_BASE_URL`: API endpoint (defaults to https://dashscope.aliyuncs.com/compatible-mode/v1)
- `DASHSCOPE_MODEL_NAME`: Model name (defaults to qwen3-coder-plus)

## Running Code

Use `uv` to run Python files:
```bash
# Run main analysis workflow
uv run main.py

# Run example LangGraph demos
uv run basic_langgraph.py
uv run langgraph_with_llm.py
```

## Architecture

### Core Workflow (LangGraph State Machine)

The system implements a 4-node linear workflow in `backend/app/graphs/code_analysis_graph.py`:

1. **read_csv_info_node**: Reads CSV file and extracts metadata (columns, dtypes, sample data, null counts)
2. **generate_code_node**: Calls LLM with CSV metadata to generate Python analysis code
3. **execute_code_node**: Safely executes generated code in sandboxed environment
4. **summarize_node**: Aggregates and logs workflow results

State flows linearly: `read_csv_info → generate_code → execute_code → summarize → END`

### Key Components

**State Management** (`backend/app/nodes/code_analysis_nodes.py`):
- `AnalysisState` TypedDict defines shared state across nodes
- Each node returns updated state dict to propagate data
- Errors are captured in `state["error"]` and logged to `state["messages"]`

**LLM Integration** (`backend/app/core/config.py`):
- `get_dashscope_client()`: Creates OpenAI-compatible client for Dashscope
- Uses `python-dotenv` to load API credentials
- Returns configured `OpenAI` client instance

**Code Execution** (`backend/app/utils/code_executor.py`):
- `execute_code_safely()`: Executes LLM-generated code with stdout/stderr capture
- Uses `exec()` with restricted namespace containing only `csv_path` variable
- `extract_code_from_llm_response()`: Extracts Python code from markdown-wrapped LLM responses

### Data Flow

```
CSV file → Pandas metadata extraction → LLM prompt with metadata →
Generated Python code → Safe execution → Results display
```

The LLM receives CSV structure (columns, types, sample rows) and generates code that:
- Uses the `csv_path` variable (injected into execution namespace)
- Prints analysis results (captured by stdout redirect)
- Avoids visualization libraries (matplotlib, seaborn)
- Performs statistical analysis on numeric columns

## Project Structure

```
backend/app/
├── core/          # Configuration and API client setup
├── nodes/         # LangGraph node implementations
├── graphs/        # LangGraph workflow definitions
└── utils/         # Code execution utilities

backend/data/raw/  # CSV data files for analysis
```

## Example Files

- `basic_langgraph.py`: Minimal LangGraph example with conditional edges
- `langgraph_with_llm.py`: Demonstrates different patterns for LLM integration in nodes (closure pattern, global instances, etc.)
- `main.py`: Entry point for running the data analysis workflow

## Testing

### Running Unit Tests

Install test dependencies and run pytest:
```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific test file
uv run pytest backend/tests/test_nodes.py

# Run specific test
uv run pytest backend/tests/test_nodes.py::TestReadCsvInfoNode::test_read_csv_info_success
```

Test coverage:
- `backend/tests/test_nodes.py`: Unit tests for all 4 nodes with mocked LLM calls
- Tests include success cases, error handling, and integration scenarios

### Interactive Node Testing

Use the interactive script to test individual nodes without running the full workflow:

```bash
uv run test_single_node.py
```

Features:
- Select individual nodes (1-4) to test in isolation
- Automatically prepares appropriate state for each node
- Option to use real LLM or mock code for node 3
- Step-by-step execution with confirmation prompts
- Test all nodes sequentially

Example workflow:
1. Choose CSV file (defaults to `backend/data/raw/test_data.csv`)
2. Select node to test (1=read_csv, 2=generate_code, 3=execute_code, 4=summarize)
3. Script automatically prepares required state from previous nodes
4. View detailed output and results

This is useful for:
- Debugging specific node behavior
- Testing without LLM API costs (using mock code)
- Rapid iteration during development
- Understanding state transformations

## Development Notes

- The codebase contains both Chinese and English comments/strings
- All nodes handle errors gracefully by setting `state["error"]` and continuing
- The workflow is deterministic (no conditional branching in production graph)
- Code execution is sandboxed but uses `exec()` - caution with untrusted code generation
