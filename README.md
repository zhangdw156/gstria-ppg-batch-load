# gstria-ppg-batch-load

PostgreSQL 批量数据导入工具。专用于将目录下的 `.tbl` 文件批量导入 PG 数据库，支持动态分区名解析与表锁定机制。

## 环境要求

- Python 3.9+
- [uv](https://github.com/astral-sh/uv) (推荐的包管理器)
- Docker (如果目标数据库在 Docker 容器中且配置为使用 Docker exec)

## 安装与设置

### 安装依赖

使用 uv 同步依赖环境：

```bash
uv sync
```

### 配置环境变量

复制示例配置文件并修改：

```bash
cp .env.example .env
vim .env
```
如果数据库在 Docker 容器中，请填写 PG_CONTAINER_NAME。
如果是远程或本地直连（非 Docker exec 模式），请清空 PG_CONTAINER_NAME，并确保本地安装了 psql 且配置了 .pgpass 或环境变量以支持免密登录。

### 使用方法

激活虚拟环境并运行工具：

## 基本用法

```bash
uv run gstria-ppg-batch-load -f <table_name> -d <tbl_directory>
```

### 示例

```bash
uv run gstria-ppg-batch-load -f performance -d /data/datasets/beijing_100k
```

### 参数说明

-f, --table: 目标表的基本名称（Base Name）。
-d, --directory: 包含 .tbl 文件的本地目录路径。
--clean / --no-clean: 是否在导入前清空表数据（默认开启 --clean）。

### 注意事项

数据格式: 默认假设输入文件分隔符为 |，列顺序为 fid,geom,dtg,taxi_id。
分区逻辑: 工具依赖数据库中存在 geomesa_wa_seq 表来查询具体的分区表名。

