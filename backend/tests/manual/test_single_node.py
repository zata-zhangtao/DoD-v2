"""
交互式单节点测试脚本
允许单独测试 LangGraph 工作流中的任意节点
"""
import os
import sys

from backend.app.nodes.code_analysis_nodes import (
    AnalysisState,
    read_csv_info_node,
    generate_code_node,
    execute_code_node,
    summarize_node,
)


def create_mock_state(csv_path: str, node_number: int) -> AnalysisState:
    """
    根据要测试的节点创建合适的 mock state

    Args:
        csv_path: CSV 文件路径
        node_number: 要测试的节点编号 (1-4)

    Returns:
        AnalysisState: 准备好的状态
    """
    base_state: AnalysisState = {
        "csv_path": csv_path,
        "csv_info": {},
        "prompt": "",
        "generated_code": "",
        "execution_result": {},
        "error": None,
        "messages": []
    }

    if node_number == 1:
        # 测试节点1：read_csv_info_node，使用空状态
        return base_state

    elif node_number == 2:
        # 测试节点2：generate_code_node，需要先有 csv_info
        print("正在准备 csv_info...")
        state = read_csv_info_node(base_state)
        if state["error"]:
            print(f"⚠️  警告：读取 CSV 时出错: {state['error']}")
        return state

    elif node_number == 3:
        # 测试节点3：execute_code_node，需要有生成的代码
        print("正在准备 csv_info 和生成代码...")
        state = read_csv_info_node(base_state)

        if state["error"]:
            print(f"⚠️  警告：读取 CSV 时出错: {state['error']}")
            return state

        # 选项：使用真实 LLM 还是 mock 代码
        use_real_llm = input("是否调用真实 LLM 生成代码？(y/n，默认n): ").strip().lower() == 'y'

        if use_real_llm:
            print("调用 LLM 生成代码...")
            state = generate_code_node(state)
        else:
            # 使用 mock 代码
            print("使用 mock 代码...")
            state["generated_code"] = f"""
import pandas as pd

df = pd.read_csv(csv_path)

print("=" * 60)
print("数据基本信息")
print("=" * 60)
print(f"数据形状: {{df.shape}}")
print(f"列名: {{df.columns.tolist()}}")

print("\\n" + "=" * 60)
print("前5行数据")
print("=" * 60)
print(df.head())

print("\\n" + "=" * 60)
print("数据统计")
print("=" * 60)
print(df.describe())

print("\\n" + "=" * 60)
print("缺失值统计")
print("=" * 60)
print(df.isnull().sum())
"""
            state["messages"].append("使用 mock 代码（跳过 LLM 调用）")

        return state

    elif node_number == 4:
        # 测试节点4：summarize_node，需要完整的执行流程
        print("正在准备完整的执行流程...")
        state = read_csv_info_node(base_state)

        if state["error"]:
            print(f"⚠️  警告：读取 CSV 时出错: {state['error']}")
            return state

        # 使用 mock 代码
        print("使用 mock 代码...")
        state["generated_code"] = "import pandas as pd\ndf = pd.read_csv(csv_path)\nprint(df.head())"
        state["messages"].append("使用 mock 代码")

        # 执行代码
        print("执行代码...")
        state = execute_code_node(state)

        return state

    return base_state


def test_node_1(csv_path: str):
    """测试节点1：读取CSV信息"""
    print("\n" + "=" * 70)
    print("测试节点 1: read_csv_info_node")
    print("=" * 70)

    state = create_mock_state(csv_path, 1)
    result = read_csv_info_node(state)

    print("\n结果:")
    print(f"- 错误: {result['error']}")
    print(f"- CSV 行数: {result['csv_info'].get('rows', 'N/A')}")
    print(f"- CSV 列数: {len(result['csv_info'].get('columns', []))}")
    print(f"- 列名: {result['csv_info'].get('columns', [])}")
    print(f"- 消息: {result['messages']}")

    return result


def test_node_2(csv_path: str):
    """测试节点2：生成代码"""
    print("\n" + "=" * 70)
    print("测试节点 2: generate_code_node")
    print("=" * 70)

    state = create_mock_state(csv_path, 2)

    if state["error"]:
        print(f"\n⚠️  无法继续：{state['error']}")
        return state

    result = generate_code_node(state)

    print("\n结果:")
    print(f"- 错误: {result['error']}")
    print(f"- 生成代码长度: {len(result['generated_code'])} 字符")
    print(f"- 消息: {result['messages']}")

    if result['generated_code']:
        print("\n生成的代码:")
        print("-" * 70)
        print(result['generated_code'])
        print("-" * 70)

    return result


def test_node_3(csv_path: str):
    """测试节点3：执行代码"""
    print("\n" + "=" * 70)
    print("测试节点 3: execute_code_node")
    print("=" * 70)

    state = create_mock_state(csv_path, 3)

    if state["error"]:
        print(f"\n⚠️  无法继续：{state['error']}")
        return state

    print("\n将要执行的代码:")
    print("-" * 70)
    print(state['generated_code'][:500] + "..." if len(state['generated_code']) > 500 else state['generated_code'])
    print("-" * 70)

    confirm = input("\n确认执行？(y/n，默认y): ").strip().lower()
    if confirm == 'n':
        print("已取消执行")
        return state

    result = execute_code_node(state)

    print("\n结果:")
    print(f"- 执行成功: {result['execution_result'].get('success', False)}")
    print(f"- 错误: {result['error']}")
    print(f"- 消息: {result['messages']}")

    if result['execution_result'].get('success'):
        print("\n执行输出:")
        print("-" * 70)
        print(result['execution_result']['output'])
        print("-" * 70)
    else:
        print("\n执行错误:")
        print(result['execution_result'].get('error', 'Unknown error'))

    return result


def test_node_4(csv_path: str):
    """测试节点4：总结结果"""
    print("\n" + "=" * 70)
    print("测试节点 4: summarize_node")
    print("=" * 70)

    state = create_mock_state(csv_path, 4)
    result = summarize_node(state)

    print("\n结果:")
    print(f"- 消息数量: {len(result['messages'])}")
    print(f"- 所有消息: {result['messages']}")

    return result


def main():
    """主函数"""
    print("=" * 70)
    print("LangGraph 节点单独测试工具")
    print("=" * 70)

    # 默认 CSV 路径
    default_csv = "/code/DoD_v2/backend/data/raw/test_data.csv"

    print(f"\n默认 CSV 文件: {default_csv}")
    csv_path = input("输入 CSV 文件路径（回车使用默认）: ").strip()

    if not csv_path:
        csv_path = default_csv

    if not os.path.exists(csv_path):
        print(f"\n❌ 错误：文件不存在: {csv_path}")
        return

    print(f"\n使用 CSV 文件: {csv_path}")

    # 选择要测试的节点
    print("\n可用节点:")
    print("1. read_csv_info_node - 读取 CSV 信息")
    print("2. generate_code_node - 生成分析代码（调用 LLM）")
    print("3. execute_code_node - 执行生成的代码")
    print("4. summarize_node - 总结分析结果")
    print("5. 测试所有节点")

    choice = input("\n选择要测试的节点 (1-5): ").strip()

    if choice == "1":
        test_node_1(csv_path)
    elif choice == "2":
        test_node_2(csv_path)
    elif choice == "3":
        test_node_3(csv_path)
    elif choice == "4":
        test_node_4(csv_path)
    elif choice == "5":
        print("\n开始测试所有节点...")
        test_node_1(csv_path)
        input("\n按回车继续测试节点2...")
        test_node_2(csv_path)
        input("\n按回车继续测试节点3...")
        test_node_3(csv_path)
        input("\n按回车继续测试节点4...")
        test_node_4(csv_path)
    else:
        print("无效选择")
        return

    print("\n" + "=" * 70)
    print("测试完成")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n已中断")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
