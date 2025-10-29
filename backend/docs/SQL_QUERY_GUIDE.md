# SQL 自然语言查询功能使用指南

本指南介绍如何使用新增的 SQL 自然语言查询功能。

## 功能概述

该功能允许用户使用自然语言查询 SQLite 数据库，系统会自动：
1. 理解用户的查询意图
2. 生成对应的 SQL 语句
3. 验证 SQL 的安全性
4. 执行查询并返回结果
5. 用通俗易懂的语言解释结果

## 快速开始

### 1. 初始化数据库

首先运行数据库初始化脚本，创建包含测试数据的 SQLite 数据库：

```bash
python3 scripts/init_database.py
```

这将在 `data/analytics.db` 创建一个包含以下数据的数据库：
- **sales_data**: 销售数据（1780+ 条记录）
- **user_metrics**: 用户行为指标（540+ 条记录）
- **performance_stats**: 性能统计数据（69000+ 条记录）

### 2. 验证数据库

运行独立测试验证数据库功能：

```bash
python3 test_sql_standalone.py
```

### 3. 使用自然语言查询

#### 方式一：使用示例脚本

```bash
cd /codes/DoD/backend
python3 examples/sql_query_example.py
```

选择不同的示例模式：
- **模式 1**: 单个查询示例
- **模式 2**: 批量查询示例
- **模式 3**: 业务问题分析
- **模式 4**: 交互式查询模式

#### 方式二：编程方式

```python
from app.graphs.code_analysis_graph import run_sql_analysis

# 数据库路径
db_path = "data/analytics.db"

# 自然语言查询
query = "查询过去30天销售额最高的前10个产品"

# 执行查询
result = run_sql_analysis(db_path, query)

# 获取结果
if not result.get("error"):
    print("生成的 SQL:", result["generated_sql"])
    print("查询结果:", result["sql_execution_result"])
    print("结果解释:", result["interpretation"])
```

#### 方式三：批量查询

```python
from app.graphs.code_analysis_graph import run_multi_query_analysis

# 多个查询
queries = [
    "各个地区的总销售额排名",
    "iOS平台的平均7日留存率",
    "响应时间最慢的3个服务"
]

# 执行批量查询
results = run_multi_query_analysis(db_path, queries)
```

## 查询示例

### 销售分析

```
- "查询最近7天每个产品类别的销售额"
- "哪个地区的电子产品销售最好？"
- "折扣率大于10%的订单有多少？"
- "计算每个地区的平均订单金额"
```

### 用户分析

```
- "各平台的日活用户数趋势"
- "Android平台的7日留存率是多少？"
- "新增用户最多的是哪一天？"
- "转化率最高的平台是哪个？"
```

### 性能分析

```
- "响应时间超过300ms的服务有哪些？"
- "payment-service的平均响应时间"
- "错误率最高的5个端点"
- "最近24小时各服务的QPS"
```

## 工作流程

```
┌─────────────────┐
│ 自然语言查询    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 读取数据库结构  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ LLM 生成 SQL    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ SQL 安全性验证  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 执行 SQL 查询   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ LLM 解释结果    │
└─────────────────┘
```

## 安全性

系统内置了多重安全保护：

1. **SQL 注入防护**: 检测并拦截潜在的 SQL 注入攻击
2. **只读查询**: 只允许 SELECT 查询，禁止 INSERT、UPDATE、DELETE、DROP 等写操作
3. **数据库只读模式**: 使用只读模式打开数据库连接
4. **结果数量限制**: 自动添加 LIMIT 子句防止返回过多数据

被拦截的危险操作示例：
- `DROP TABLE users`
- `DELETE FROM sales_data`
- `UPDATE user_metrics SET ...`
- `SELECT * FROM users; DROP TABLE users`

## 文件结构

```
backend/
├── app/
│   ├── graphs/
│   │   └── code_analysis_graph.py    # 主工作流（新增 SQL 功能）
│   ├── nodes/
│   │   ├── code_analysis_nodes.py    # CSV 分析节点（原有）
│   │   └── sql_analysis_nodes.py     # SQL 查询节点（新增）
│   └── utils/
│       ├── code_executor.py          # Python 代码执行器（原有）
│       └── sql_executor.py           # SQL 执行器（新增）
├── data/
│   └── analytics.db                  # SQLite 数据库
├── scripts/
│   └── init_database.py              # 数据库初始化脚本
├── examples/
│   └── sql_query_example.py          # 使用示例
└── test_sql_standalone.py            # 独立测试脚本
```

## API 参考

### run_sql_analysis(db_path, natural_query)

运行单个自然语言 SQL 查询。

**参数:**
- `db_path` (str): 数据库文件路径
- `natural_query` (str): 自然语言查询

**返回:**
- `dict`: 包含以下字段的结果字典
  - `generated_sql`: 生成的 SQL 语句
  - `sql_execution_result`: SQL 执行结果
  - `interpretation`: LLM 对结果的解释
  - `error`: 错误信息（如果有）

### run_multi_query_analysis(db_path, queries)

运行多个自然语言查询。

**参数:**
- `db_path` (str): 数据库文件路径
- `queries` (list): 自然语言查询列表

**返回:**
- `list`: 每个查询的结果列表

## 与 CSV 分析功能的关系

SQL 查询功能与原有的 CSV 分析功能是**并存**的关系：

- **CSV 分析**: 使用 `run_analysis(csv_path)` - 适用于 CSV 文件的多轮迭代分析
- **SQL 查询**: 使用 `run_sql_analysis(db_path, query)` - 适用于数据库的自然语言查询

两个功能独立运行，互不干扰。

## 常见问题

### Q: 如何添加自己的数据？

A: 修改 `scripts/init_database.py` 脚本，添加自己的表结构和数据生成逻辑。

### Q: 支持哪些数据库？

A: 目前只支持 SQLite。未来可以扩展支持 MySQL、PostgreSQL 等。

### Q: 查询失败怎么办？

A: 检查以下几点：
1. 数据库文件是否存在
2. 查询是否包含危险操作（会被安全检查拦截）
3. 表名和字段名是否正确
4. 查看错误信息中的详细说明

### Q: 如何优化查询性能？

A:
1. 在表的常用查询字段上创建索引
2. 使用 LIMIT 限制返回结果数量
3. 避免复杂的子查询和多表连接

## 进一步开发

### 扩展功能建议

1. **支持更多数据库**: 添加 MySQL、PostgreSQL 适配器
2. **查询优化**: 自动分析慢查询并提供优化建议
3. **可视化**: 将查询结果自动生成图表
4. **查询模板**: 预定义常用查询模板
5. **权限控制**: 添加更细粒度的数据访问控制

### 自定义节点

可以在 `app/nodes/sql_analysis_nodes.py` 中添加新的节点，例如：
- 查询结果缓存节点
- 查询性能分析节点
- 自动索引建议节点

## 技术栈

- **LangGraph**: 工作流编排
- **SQLite**: 数据库
- **OpenAI API (DashScope)**: LLM 服务
- **Python 3**: 开发语言

## 许可证

与项目主许可证相同。
