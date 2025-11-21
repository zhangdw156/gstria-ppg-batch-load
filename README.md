# gstria-ppg-batch-load

PostgreSQL 批量数据导入工具。专用于将目录下的 `.tbl` 文件批量导入 PG 数据库，支持动态分区名解析与表锁定机制。

## 目录结构

- **src/**: 源代码目录
- **scripts/**: 辅助安装与运维脚本
- **logs/**: 运行日志

## 环境要求

- Python 3.9+
- Linux / macOS (本项目包含安装脚本)
- Docker (可选，如果目标数据库在 Docker 容器中)

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

> Docker 环境: 如果数据库运行在 Docker 容器中，请填写 PG_CONTAINER_NAME。
>
> 直连模式: 如果是远程数据库或本地直连（非 Docker exec 模式）：
>
> 请留空 PG_CONTAINER_NAME。
> 
> 确保本地已安装 psql 客户端工具。
> 
> 确保配置了 ~/.pgpass 文件或设置了 PGPASSWORD 环境变量以支持免密登录。


### 5. 使用方法
无需手动激活虚拟环境，直接使用 uv run 即可运行工具。

```bash
uv run gstria-ppg-batch-load -f <table_name> -d <tbl_directory> [OPTIONS]
```

## 示例：导入 beijing_100k 数据到 performance 基础表

uv run gstria-ppg-batch-load -f performance -d /data/datasets/beijing_100k

### 参数说明
> 
> 参数	缩写	说明	默认值
> 
> --table	-f	[必填] 目标表的基本名称（Base Name）	-
> 
> --directory	-d	[必填] 包含 .tbl 文件的本地目录路径	-
> 
> --clean / --no-clean	-	是否在导入前清空表数据	--clean (默认开启)

### 注意事项

> 数据格式: 默认假设输入文件分隔符为 |，列顺序为 fid, geom, dtg, taxi_id。
> 
> 分区逻辑: 工具依赖数据库中存在 geomesa_wa_seq 表来查询具体的分区表名。请确保数据库 Schema 正确。
