"""
Main entry file
Demonstrates how to use the data analysis workflow
"""
import os
from backend.app.graphs.code_analysis_graph import run_analysis


def main():
    """
    Main function: Run data analysis workflow
    """
    # CSV file path
    csv_path = "/code/DoD_v2/backend/data/raw/test_data.csv"

    # Check if file exists
    if not os.path.exists(csv_path):
        print(f"Error: CSV file does not exist: {csv_path}")
        return

    # Run analysis
    result = run_analysis(csv_path)

    # Display generated code
    print("\n" + "=" * 60)
    print("Generated Code:")
    print("=" * 60)
    print(result.get("generated_code", "None"))

    # Display execution result
    print("\n" + "=" * 60)
    print("Execution Result:")
    print("=" * 60)
    execution_result = result.get("execution_result", {})
    if execution_result.get("success"):
        print("✓ Execution successful")
    else:
        print(f"✗ Execution failed: {execution_result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()
