import os
import sys
import time
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ==================== 配置获取 ====================
DB_USER = os.getenv("PG_USER", "postgres")
DB_NAME = os.getenv("PG_DB", "postgres")

# 新增 Host 和 Port 配置
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
DB_PASSWORD = os.getenv("PG_PASSWORD", "")
CONTAINER_NAME = os.getenv("PG_CONTAINER_NAME", "")  # 默认为空字符串

# Cron 调度配置
CRON_SCHEDULE_ROLL_WA = os.getenv("CRON_SCHEDULE_ROLL_WA", "")
CRON_SCHEDULE_MAINTENANCE = os.getenv("CRON_SCHEDULE_MAINTENANCE", "")


def setup_logging():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"import_log_{time.strftime('%Y%m%d')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler(sys.stdout)]
    )

# ==================== 新增: SwanLab 环境配置字典 ====================
# 专门用于传递给 swanlab.init(config=...)
# 显式排除 DB_PASSWORD，包含其他所有环境参数
SWANLAB_ENV_SETTINGS = {
    "Env/PG_USER": DB_USER,
    "Env/PG_DB": DB_NAME,
    "Env/PG_HOST": PG_HOST,
    "Env/PG_PORT": PG_PORT,
    "Env/CONTAINER_NAME": CONTAINER_NAME,
    "Env/CRON_ROLL_WA": CRON_SCHEDULE_ROLL_WA,
    "Env/CRON_MAINTENANCE": CRON_SCHEDULE_MAINTENANCE,
}