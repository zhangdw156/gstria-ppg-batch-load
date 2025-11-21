import subprocess
import shlex
import logging
import os
from .config import DB_USER, DB_NAME, CONTAINER_NAME


def build_psql_prefix(interactive=False):
    flags = "-i" if interactive else ""
    if CONTAINER_NAME:
        return f"docker exec {flags} {CONTAINER_NAME} psql -U {DB_USER} -d {DB_NAME}"
    return f"psql -U {DB_USER} -d {DB_NAME}"


def run_command(cmd, check=True, capture_output=False, env=None):
    """
    增加 env 参数支持，用于安全传递复杂字符串
    """
    # 如果没有传入 env，默认使用当前系统环境变量
    run_env = env if env is not None else os.environ.copy()

    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=check,
            capture_output=capture_output,
            text=True,
            env=run_env  # <--- 关键修改：传入环境变量
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