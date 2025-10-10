"""
Configuration module for Dashscope API
"""
import os
from dotenv import load_dotenv
from openai import OpenAI


def load_env():
    """Load environment variables"""
    load_dotenv()


def get_dashscope_client() -> OpenAI:
    """
    Create Dashscope API client

    Returns:
        OpenAI: Configured OpenAI client instance

    Raises:
        ValueError: If required environment variables are missing
    """
    load_env()

    api_key = os.getenv("DASHSCOPE_API_KEY")
    base_url = os.getenv("DASHSCOPE_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")

    if not api_key:
        raise ValueError(
            "Missing DASHSCOPE_API_KEY environment variable. "
            "Please set it in .env file or as environment variable."
        )

    return OpenAI(
        api_key=api_key,
        base_url=base_url,
    )


def get_model_name() -> str:
    """
    Get model name

    Returns:
        str: Model name, defaults to qwen3-coder-plus
    """
    load_env()
    return os.getenv("MODEL_NAME", "qwen3-coder-plus")
