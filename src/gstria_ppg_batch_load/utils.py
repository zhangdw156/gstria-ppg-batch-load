import subprocess
import shlex
import logging
import os
from .config import DB_USER, DB_NAME, CONTAINER_NAME, PG_HOST, PG_PORT, DB_PASSWORD


def build_psql_prefix(interactive=False):
    """
    构建 psql 命令前缀。

    模式 A (Docker): docker exec -i <container> psql -U <user> -d <db>
    模式 B (Direct): psql -h <host> -p <port> -U <user> -d <db>
    """
    flags = "-i" if interactive else ""

    if CONTAINER_NAME:
        # Docker 模式：通常直接在容器内连接，不需要指定 -h/-p (默认为 socket 或 localhost)
        return f"docker exec {flags} {CONTAINER_NAME} psql -U {DB_USER} -d {DB_NAME}"
    else:
        # 直连模式：必须指定 Host 和 Port
        # 注意：-h 和 -p 参数放在前面比较规范
        return f"psql -h {PG_HOST} -p {PG_PORT} -U {DB_USER} -d {DB_NAME}"


def run_command(cmd, check=True, capture_output=False, env=None):
    """
    增加 env 参数支持，用于安全传递复杂字符串
    """
    # 如果没有传入 env，默认使用当前系统环境变量
    run_env = env if env is not None else os.environ.copy()

    if DB_PASSWORD:
        run_env["PGPASSWORD"] = DB_PASSWORD



    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=check,
            capture_output=capture_output,
            text=True,
            env=run_env
        )
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"Cmd Failed: {e.cmd}")
        if capture_output and e.stderr: logging.error(f"Stderr: {e.stderr.strip()}")
        raise e


def run_sql_command(sql, fetch_output=False):
    base_cmd = build_psql_prefix(interactive=False)
    flags = " -tA" if fetch_output else ""
    cmd = f"{base_cmd}{flags} -c {shlex.quote(sql)}"
    return run_command(cmd, capture_output=fetch_output)
