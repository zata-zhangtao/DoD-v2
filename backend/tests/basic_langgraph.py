"""
最基础的 LangGraph 示例
展示如何创建一个简单的状态图，并集成 LLM
"""
from typing import TypedDict, Annotated
from langgraph.graph import StateGraph, END
from langchain_openai import ChatOpenAI
import operator
import os


# 1. 定义状态
class State(TypedDict):
    messages: Annotated[list[str], operator.add]
    counter: int


# 2. 初始化模型（方式1：全局配置）
def init_llm():
    """初始化 LLM，使用环境变量或自定义配置"""
    return ChatOpenAI(
        model="gpt-3.5-turbo",  # 或其他模型
        api_key=os.getenv("OPENAI_API_KEY", "your-api-key"),
        base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),  # 自定义 URL
        temperature=0.7
    )


# 3. 定义节点函数
def node_1(state: State) -> State:
    """第一个节点：使用 LLM 生成消息"""
    print("执行 Node 1 - 调用 LLM")

    # 初始化 LLM
    llm = init_llm()

    # 调用模型（示例：注释掉避免实际调用）
    # response = llm.invoke("Say hello")
    # message = response.content

    # 不调用模型的示例
    message = "Hello from Node 1 (模拟 LLM 响应)"

    return {
        "messages": [message],
        "counter": state["counter"] + 1
    }


def node_2(state: State) -> State:
    """第二个节点：添加消息并增加计数器"""
    print("执行 Node 2")
    return {
        "messages": ["Hello from Node 2"],
        "counter": state["counter"] + 1
    }


def node_3(state: State) -> State:
    """第三个节点：添加最终消息"""
    print("执行 Node 3")
    return {
        "messages": [f"完成！总计数: {state['counter']}"],
        "counter": state["counter"] + 1
    }


# 3. 定义条件边（可选）
def should_continue(state: State) -> str:
    """根据计数器决定流向"""
    if state["counter"] < 2:
        return "node_2"
    else:
        return "node_3"


# 4. 构建图
def create_graph():
    """创建并编译图"""
    # 初始化图
    workflow = StateGraph(State)

    # 添加节点
    workflow.add_node("node_1", node_1)
    workflow.add_node("node_2", node_2)
    workflow.add_node("node_3", node_3)

    # 设置入口点
    workflow.set_entry_point("node_1")

    # 添加边
    workflow.add_conditional_edges(
        "node_1",
        should_continue,
        {
            "node_2": "node_2",
            "node_3": "node_3"
        }
    )
    workflow.add_edge("node_2", "node_3")
    workflow.add_edge("node_3", END)

    # 编译图
    return workflow.compile()


# 5. 运行图
def main():
    """主函数"""
    # 创建图
    app = create_graph()

    # 初始化状态
    initial_state = {
        "messages": [],
        "counter": 0
    }

    # 运行图
    print("开始运行 LangGraph...")
    print("-" * 50)

    result = app.invoke(initial_state)

    print("-" * 50)
    print("\n最终结果:")
    print(f"消息列表: {result['messages']}")
    print(f"最终计数: {result['counter']}")


if __name__ == "__main__":
    main()
