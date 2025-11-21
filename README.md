# gstria-ppg-batch-load

PostgreSQL 批量数据导入工具。专用于将目录下的 `.tbl` 文件批量导入 PG 数据库（geomesa-gt为sft创建的视图），支持动态分区名解析与表锁定机制。

## 目录结构

- **src/**: 源代码目录
- **scripts/**: 辅助安装与运维脚本
- **logs/**: 运行日志

## 环境要求

- Linux / macOS (本项目包含安装脚本)
- Docker (目前只支持向本机的docker容器里的PG数据库导入)

## 快速开始

### 1. 安装 uv 包管理器

本项目使用 [uv](https://github.com/astral-sh/uv) 进行依赖管理。为了方便部署，项目中已内置安装脚本。

执行以下命令安装 uv（安装至当前用户目录）：

```bash
# 运行内置安装脚本
sh scripts/uv-installer.sh
```

### 2. 初始化环境
安装完 uv 后，同步项目依赖：

```bash
uv sync
```

### 3. 配置环境变量
复制示例配置文件并根据实际环境修改：

```bash
cp .env.example .env
vim .env
```

## 4. 配置说明：

```bash
# Postgres 连接配置
PG_USER=postgres
PG_DB=postgres
PG_CONTAINER_NAME=my-postgis-container
```

### 5. 运行模式与命令

无需手动激活虚拟环境，直接使用 uv run 即可运行工具。

本项目提供两种导入模式，分别对应不同的命令行入口。请根据需求选择：

#### A. 标准模式 (Standard Mode)

命令: gstria-ppg-batch-load

仅执行基础的索引维护（备份并删除辅助索引 -> 导入 -> 恢复辅助索引），不触碰主键。

适用于常规增量导入，或不需要重置主键的场景。

```bash
uv run gstria-ppg-batch-load -f <table_name> -d <tbl_directory>
```

#### B. Collatec 模式 (Collatec Mode)

命令: gstria-ppg-batch-load-collatec

在标准模式的基础上，增加了 步骤 1.6：主键重置（先删除主键约束，导入后立即重建主键）。

适用于需要通过重建主键来整理碎片、优化存储或解决特定约束冲突的场景。

```bash
uv run gstria-ppg-batch-load-collatec -f <table_name> -d <tbl_directory>
```

## 示例
### 示例 1：使用标准模式导入

```bash
# 导入 beijing_100k 数据到 performance 基础表
uv run gstria-ppg-batch-load -f performance -d /data/datasets/beijing_100k
```

### 示例 2：使用 Collatec 模式导入（含主键重建）

```bash
# 导入相同数据，但在过程中重建主键
uv run gstria-ppg-batch-load-collatec -f performance -d /data/datasets/beijing_100k
```

### 参数说明

| 参数                 | 缩写 | 说明                                 | 默认值             |
| -------------------- | ---- | ------------------------------------ | ------------------ |
| --table              | -f   | [必填] 目标表的基本名称（Base Name） | -                  |
| --directory          | -d   | [必填] 包含 .tbl 文件的本地目录路径  | -                  |
| --clean / --no-clean | -    | 是否在导入前清空表数据               | --clean (默认开启) |

### 注意事项

> 数据格式: 默认假设输入文件分隔符为 |，列顺序为 fid, geom, dtg, taxi_id。
> 
> 分区逻辑: 工具依赖数据库中存在 geomesa_wa_seq 表来查询具体的分区表名。请确保数据库 Schema 正确。
