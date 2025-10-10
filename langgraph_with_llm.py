"""
完整示例：在 LangGraph 中使用 LLM
展示如何配置和调用模型
"""
from typing import TypedDict, Annotated
from langgraph.graph import StateGraph, END
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
import operator


# 1. 定义状态
class State(TypedDict):
    messages: Annotated[list, operator.add]  # 支持 LangChain 消息对象
    counter: int

# 2. 配置模型 - 三种方式

# 方式 A：直接硬编码（仅用于测试）
def create_llm_hardcoded():
    return ChatOpenAI(
        model="gpt-3.5-turbo",
        api_key="your-api-key",
        base_url="https://api.openai.com/v1",
        temperature=0.7
    )


# 方式 B：从环境变量读取（推荐）
def create_llm_from_env():
    import os
    return ChatOpenAI(
        model=os.getenv("MODEL_NAME", "gpt-3.5-turbo"),
        api_key=os.getenv("OPENAI_API_KEY"),
        base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        temperature=float(os.getenv("TEMPERATURE", "0.7"))
    )


# 方式 C：通过参数传递（最灵活）
def create_llm_from_config(api_key: str, base_url: str, model: str = "gpt-3.5-turbo"):
    return ChatOpenAI(
        model=model,
        api_key=api_key,
        base_url=base_url,
        temperature=0.7
    )


# 3. 定义使用 LLM 的节点
def llm_node(state: State) -> State:
    """使用 LLM 处理消息"""
    print("执行 LLM Node")

    # 方式1：在节点内创建 LLM（每次调用都创建，开销较大）
    # llm = create_llm_from_env()

    # 方式2：使用全局 LLM 实例（推荐，见下方）
    llm = ChatOpenAI(
        model="gpt-3.5-turbo",
        api_key="your-api-key",  # 替换为你的 key
        base_url="https://api.openai.com/v1",  # 替换为你的 URL
    )

    # 调用 LLM
    try:
        response = llm.invoke([HumanMessage(content="用一句话介绍 LangGraph")])
        message = response.content
    except Exception as e:
        message = f"LLM 调用失败: {str(e)}"

    return {
        "messages": [message],
        "counter": state["counter"] + 1
    }


def summary_node(state: State) -> State:
    """总结节点"""
    print("执行 Summary Node")
    summary = f"处理完成！共处理 {state['counter']} 个节点，收到 {len(state['messages'])} 条消息"
    return {
        "messages": [summary],
        "counter": state["counter"] + 1
    }


# 4. 方式 D：使用闭包传递 LLM 实例（最推荐）
def create_graph_with_llm(api_key: str, base_url: str, model: str = "gpt-3.5-turbo"):
    """创建带有 LLM 配置的图"""

    # 创建 LLM 实例（只创建一次）
    llm = ChatOpenAI(
        model=model,
        api_key=api_key,
        base_url=base_url,
        temperature=0.7
    )

    # 定义使用这个 LLM 的节点
    def llm_node_closure(state: State) -> State:
        """使用闭包中的 LLM 实例"""
        print("执行 LLM Node (使用闭包)")

        try:
            response = llm.invoke([HumanMessage(content="用一句话介绍 LangGraph")])
            message = response.content
        except Exception as e:
            message = f"LLM 调用失败: {str(e)}"

        return {
            "messages": [message],
            "counter": state["counter"] + 1
        }

    # 构建图
    workflow = StateGraph(State)
    workflow.add_node("llm_node", llm_node_closure)
    workflow.add_node("summary", summary_node)

    workflow.set_entry_point("llm_node")
    workflow.add_edge("llm_node", "summary")
    workflow.add_edge("summary", END)

    return workflow.compile()


# 5. 运行示例
def main():
    """主函数 - 演示不同的配置方式"""

    print("=" * 60)
    print("方式 D：推荐方式 - 通过函数参数传递配置")
    print("=" * 60)

    # 配置你的模型参数
    API_KEY = "your-api-key-here"  # 替换为你的 API Key
    BASE_URL = "https://api.openai.com/v1"  # 替换为你的 API URL
    MODEL = "gpt-3.5-turbo"  # 替换为你要使用的模型

    # 创建图
    app = create_graph_with_llm(
        api_key=API_KEY,
        base_url=BASE_URL,
        model=MODEL
    )

    # 运行
    initial_state = {
        "messages": [],
        "counter": 0
    }

    try:
        result = app.invoke(initial_state)
        print("\n最终结果:")
        for i, msg in enumerate(result["messages"], 1):
            print(f"{i}. {msg}")
    except Exception as e:
        print(f"\n运行出错: {str(e)}")
        print("\n提示：请在代码中设置正确的 API_KEY 和 BASE_URL")


if __name__ == "__main__":
    main()
