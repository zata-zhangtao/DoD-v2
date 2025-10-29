"""
SQL 执行器模块
安全地执行 LLM 生成的 SQL 查询
"""
import sqlite3
import re
from typing import Dict, Any, List, Tuple
import os


def validate_sql_safety(sql: str) -> Tuple[bool, str]:
    """
    验证 SQL 语句的安全性

    Args:
        sql: SQL 语句

    Returns:
        Tuple[bool, str]: (是否安全, 错误信息)
    """
    # 转换为小写便于检查
    sql_lower = sql.lower().strip()

    # 危险操作列表
    dangerous_keywords = [
        'drop', 'delete', 'truncate', 'update',
        'insert', 'alter', 'create', 'grant',
        'revoke', 'exec', 'execute', 'pragma'
    ]

    # 检查是否包含危险关键字
    for keyword in dangerous_keywords:
        # 使用正则确保是独立的关键字，而不是字段名的一部分
        pattern = r'\b' + keyword + r'\b'
        if re.search(pattern, sql_lower):
            return False, f"检测到危险操作: {keyword.upper()}"

    # 检查是否包含 SQL 注入常见模式
    injection_patterns = [
        r';\s*drop',
        r';\s*delete',
        r'union.*select',
        r'--',  # SQL 注释
        r'/\*.*\*/',  # 多行注释
    ]

    for pattern in injection_patterns:
        if re.search(pattern, sql_lower):
            return False, "检测到潜在的 SQL 注入模式"

    # 确保是 SELECT 查询
    if not sql_lower.startswith('select') and not sql_lower.startswith('with'):
        return False, "只允许 SELECT 查询"

    return True, ""


def execute_sql_safely(sql: str, db_path: str, limit: int = 1000) -> Dict[str, Any]:
    """
    安全地执行 SQL 查询

    Args:
        sql: SQL 查询语句
        db_path: 数据库文件路径
        limit: 最大返回行数，默认 1000

    Returns:
        Dict: 包含查询结果的字典
            - success: bool - 是否执行成功
            - data: List[Dict] - 查询结果（字典列表）
            - columns: List[str] - 列名列表
            - row_count: int - 返回的行数
            - error: str - 错误信息（如果有）
    """
    result = {
        "success": False,
        "data": [],
        "columns": [],
        "row_count": 0,
        "error": ""
    }

    # 验证数据库文件是否存在
    if not os.path.exists(db_path):
        result["error"] = f"数据库文件不存在: {db_path}"
        return result

    # 安全性检查
    is_safe, error_msg = validate_sql_safety(sql)
    if not is_safe:
        result["error"] = f"SQL 安全性验证失败: {error_msg}"
        return result

    # 自动添加 LIMIT 子句（如果没有）
    sql_lower = sql.lower().strip()
    if 'limit' not in sql_lower:
        # 移除末尾的分号（如果有）
        sql = sql.rstrip(';').strip()
        sql = f"{sql} LIMIT {limit}"

    try:
        # 连接数据库（只读模式）
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row  # 使结果可以按列名访问
        cursor = conn.cursor()

        # 执行查询
        cursor.execute(sql)

        # 获取列名
        columns = [description[0] for description in cursor.description] if cursor.description else []

        # 获取所有结果
        rows = cursor.fetchall()

        # 转换为字典列表
        data = [dict(row) for row in rows]

        result["success"] = True
        result["data"] = data
        result["columns"] = columns
        result["row_count"] = len(data)

        # 关闭连接
        cursor.close()
        conn.close()

    except sqlite3.Error as e:
        result["error"] = f"SQLite 错误: {str(e)}"
    except Exception as e:
        result["error"] = f"执行 SQL 时出错: {str(e)}"

    return result


def get_db_schema_info(db_path: str) -> Dict[str, Any]:
    """
    获取数据库的结构信息

    Args:
        db_path: 数据库文件路径

    Returns:
        Dict: 数据库结构信息
            - tables: List[Dict] - 表信息列表
            - table_count: int - 表数量
            - error: str - 错误信息（如果有）
    """
    result = {
        "tables": [],
        "table_count": 0,
        "error": ""
    }

    if not os.path.exists(db_path):
        result["error"] = f"数据库文件不存在: {db_path}"
        return result

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 获取所有表名
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        table_names = [row[0] for row in cursor.fetchall()]

        # 获取每个表的详细信息
        for table_name in table_names:
            # 获取表结构
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = cursor.fetchall()

            columns = []
            for col in columns_info:
                columns.append({
                    "name": col[1],
                    "type": col[2],
                    "nullable": not col[3],
                    "primary_key": bool(col[5])
                })

            # 获取表的行数
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]

            # 获取样例数据（前3行）
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
            sample_rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            sample_data = [dict(zip(column_names, row)) for row in sample_rows]

            result["tables"].append({
                "name": table_name,
                "columns": columns,
                "row_count": row_count,
                "sample_data": sample_data
            })

        result["table_count"] = len(table_names)

        cursor.close()
        conn.close()

    except sqlite3.Error as e:
        result["error"] = f"SQLite 错误: {str(e)}"
    except Exception as e:
        result["error"] = f"获取数据库信息时出错: {str(e)}"

    return result


def extract_sql_from_llm_response(response: str) -> str:
    """
    从 LLM 响应中提取 SQL 语句

    Args:
        response: LLM 的响应文本

    Returns:
        str: 提取的 SQL 语句
    """
    # 尝试提取 Markdown 代码块中的 SQL
    if "```sql" in response.lower():
        # 提取 ```sql ... ``` 之间的内容
        start = response.lower().find("```sql") + len("```sql")
        end = response.find("```", start)
        if end != -1:
            return response[start:end].strip()

    elif "```" in response:
        # 提取 ``` ... ``` 之间的内容
        start = response.find("```") + len("```")
        end = response.find("```", start)
        if end != -1:
            sql_content = response[start:end].strip()
            # 移除可能的语言标识符（如 sql, SQL）
            if sql_content.lower().startswith('sql'):
                sql_content = sql_content[3:].strip()
            return sql_content

    # 如果没有代码块标记，尝试查找 SELECT 语句
    lines = response.split('\n')
    sql_lines = []
    in_sql = False

    for line in lines:
        line_lower = line.strip().lower()
        if line_lower.startswith('select') or line_lower.startswith('with'):
            in_sql = True

        if in_sql:
            sql_lines.append(line)
            # 如果遇到分号，可能是 SQL 结束
            if ';' in line:
                break

    if sql_lines:
        return '\n'.join(sql_lines).strip()

    # 如果都没找到，返回原始响应
    return response.strip()


def format_query_result(result: Dict[str, Any], max_rows: int = 20) -> str:
    """
    格式化查询结果为易读的字符串

    Args:
        result: execute_sql_safely 返回的结果
        max_rows: 最多显示的行数

    Returns:
        str: 格式化的结果字符串
    """
    if not result["success"]:
        return f"查询失败: {result['error']}"

    if result["row_count"] == 0:
        return "查询成功，但没有返回数据。"

    output = []
    output.append(f"查询成功！返回 {result['row_count']} 行数据。\n")

    # 显示列名
    columns = result["columns"]
    output.append("列名: " + ", ".join(columns))
    output.append("-" * 60)

    # 显示数据行
    data = result["data"][:max_rows]
    for i, row in enumerate(data, 1):
        output.append(f"第 {i} 行:")
        for col in columns:
            value = row.get(col)
            output.append(f"  {col}: {value}")
        output.append("")

    # 如果数据被截断，提示
    if result["row_count"] > max_rows:
        output.append(f"... （还有 {result['row_count'] - max_rows} 行未显示）")

    return "\n".join(output)
