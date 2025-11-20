#!/usr/bin/env python3
import os
import sys
import time
import logging
import subprocess
import shlex
from pathlib import Path
import click
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# ==================== 配置获取 ====================
DB_USER = os.getenv("PG_USER", "postgres")
DB_NAME = os.getenv("PG_DB", "postgres")
CONTAINER_NAME = os.getenv("PG_CONTAINER_NAME") # 如果为空，则不使用 docker exec
ROWS_PER_FILE = int(os.getenv("ROWS_PER_FILE", "100000"))

# ==================== 日志配置 ====================
def setup_logging():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"import_log_{time.strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout)
        ]
    )

# ==================== 工具函数 ====================
def build_psql_prefix(interactive=False):
    """构建 psql 调用命令的前缀，判断是否使用 docker"""
    flags = "-i" if interactive else ""
    if CONTAINER_NAME:
        return f"docker exec {flags} {CONTAINER_NAME} psql -U {DB_USER} -d {DB_NAME}"
    else:
        # 如果不使用 Docker，直接调用本地 psql
        return f"psql -U {DB_USER} -d {DB_NAME}"

def run_command(cmd, check=True, capture_output=False):
    """执行简单 shell 命令"""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=check,
            capture_output=capture_output,
            text=True
        )
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"命令执行失败: {e.cmd}")
        if capture_output and e.stderr:
            logging.error(f"错误输出: {e.stderr.strip()}")
        raise e

# ==================== 核心导入函数 ====================
def import_single_file_with_lock(file_path, table_base_name):
    """使用 Python Popen 流式传输数据"""

    # --- 步骤 1: 获取动态分区表名 (保留原逻辑) ---
    # 注意：这里依赖数据库中存在 geomesa_wa_seq 表
    get_partition_sql = (
        f"SELECT '\"{table_base_name}_wa_' || lpad(value::text, 3, '0') || '\"' "
        f"FROM \"public\".\"geomesa_wa_seq\" WHERE type_name = '{table_base_name}'"
    )

    base_cmd = build_psql_prefix(interactive=False)
    get_partition_cmd = f"{base_cmd} -tA -c {shlex.quote(get_partition_sql)}"

    try:
        result = run_command(get_partition_cmd, capture_output=True)
        partition_name = result.stdout.strip()
        if not partition_name:
            raise ValueError(f"未能从 geomesa_wa_seq 获取分区名 (table: {table_base_name})")
        logging.info(f"      -> 动态获取分区表名: {partition_name}")
    except Exception as e:
        logging.error(f"      -> 获取分区表名失败: {e}")
        return subprocess.CompletedProcess(args=get_partition_cmd, returncode=1, stderr=str(e))

    # --- 步骤 2: 使用 Popen 进行精确的数据管道传输 ---
    
    lock_table_name = f'"{table_base_name}_wa"'
    # 注意：这里的列名 fid,geom,dtg,taxi_id 是硬编码的，如需通用需进一步传参
    copy_options = "WITH (FORMAT text, DELIMITER E'|', NULL E'')"
    copy_sql = f"COPY public.{partition_name}(fid,geom,dtg,taxi_id) FROM STDIN {copy_options};"

    sql_header = (
        f"BEGIN;\n"
        f"LOCK TABLE public.{lock_table_name} IN SHARE UPDATE EXCLUSIVE MODE;\n"
        f"{copy_sql}\n"
    ).encode('utf-8')

    sql_footer = b"\\.\nCOMMIT;\n"

    # 启动 psql 进程
    # -q: quiet, -v ON_ERROR_STOP=1: 报错即停
    psql_cmd = f"{build_psql_prefix(interactive=True)} -q -v ON_ERROR_STOP=1"

    proc = None
    try:
        proc = subprocess.Popen(
            psql_cmd,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        proc.stdin.write(sql_header)

        has_trailing_newline = False
        with open(file_path, 'rb') as f:
            while chunk := f.read(1024 * 1024):
                proc.stdin.write(chunk)
                if chunk:
                    has_trailing_newline = chunk.endswith(b'\n')

        if not has_trailing_newline:
            proc.stdin.write(b'\n')

        proc.stdin.write(sql_footer)
        stdout_bytes, stderr_bytes = proc.communicate()

        stdout_str = stdout_bytes.decode('utf-8', errors='replace')
        stderr_str = stderr_bytes.decode('utf-8', errors='replace')

        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, psql_cmd, output=stdout_str, stderr=stderr_str)

        return subprocess.CompletedProcess(args=psql_cmd, returncode=0, stdout=stdout_str, stderr=stderr_str)

    except Exception as e:
        error_msg = f"Import 异常: {str(e)}"
        logging.error(error_msg)
        if proc:
            proc.kill()
        return subprocess.CompletedProcess(args=psql_cmd, returncode=1, stderr=error_msg)

# ==================== Click 命令行入口 ====================
@click.command()
@click.option('-f', '--table', required=True, help="目标表名 (Target Table Base Name)")
@click.option('-d', '--directory', required=True, type=click.Path(exists=True, file_okay=False), help="TBL 文件所在目录")
@click.option('--clean/--no-clean', default=True, help="是否在导入前清空目标表 (默认清空)")
def cli(table, directory, clean):
    """批量导入 .tbl 工具"""
    setup_logging()
    tbl_dir = Path(directory)
    
    logging.info("=" * 50)
    start_total_time = time.time()
    logging.info(f"开始全量数据导入流程")
    logging.info(f"目标表: {table}")
    logging.info(f"数据目录: {tbl_dir}")
    if CONTAINER_NAME:
        logging.info(f"运行模式: Docker ({CONTAINER_NAME})")
    else:
        logging.info(f"运行模式: 本地直连")
    logging.info("=" * 50)

    # 1. 清空目标表
    if clean:
        logging.info(f"\n>>> 阶段 1: 清空数据 '{table}'...")
        base_cmd = build_psql_prefix(interactive=True)
        try:
            run_command(f"{base_cmd} -c 'DELETE FROM {table};'")
            logging.info("表数据已清空。")
        except subprocess.CalledProcessError:
            logging.error("清空表失败，程序终止。")
            sys.exit(1)
    else:
        logging.info(f"\n>>> 阶段 1: 跳过清空数据...")

    # 2. 查找文件
    logging.info(f"\n>>> 阶段 2: 查找数据文件...")
    tbl_files = sorted(tbl_dir.glob("*.tbl"))
    total_files = len(tbl_files)
    if total_files == 0:
        logging.error(f"在目录 '{tbl_dir}' 中未找到任何 .tbl 文件。")
        sys.exit(1)
    logging.info(f"共找到 {total_files} 个文件需要导入。")

    # 3. 循环导入
    logging.info(f"\n>>> 阶段 3: 开始循环导入文件...")
    success_count = 0
    fail_count = 0
    total_import_duration = 0.0

    for i, file_path in enumerate(tbl_files, 1):
        filename = file_path.name
        logging.info(f"  -> ({i}/{total_files}) 正在导入: {filename} ... ")

        import_start = time.time()
        # 调用导入函数
        result = import_single_file_with_lock(file_path, table)
        import_end = time.time()
        import_duration = import_end - import_start

        if result.returncode == 0:
            success_count += 1
            total_import_duration += import_duration
            logging.info(f"     ✅ 成功 (耗时: {import_duration:.3f}s)")
        else:
            fail_count += 1
            logging.error(f"     ❌ 失败: {filename}")
            if result.stderr:
                logging.error(f"     DETAILS: {result.stderr.strip()}")

    # 4. 生成报告
    logging.info(f"\n>>> 阶段 4: 统计与验证...")
    logging.info("=" * 50)

    end_total_time = time.time()
    logging.info(f"总耗时: {end_total_time - start_total_time:.3f} 秒")
    logging.info(f"成功: {success_count} | 失败: {fail_count}")

    if success_count > 0 and total_import_duration > 0:
        total_rows = success_count * ROWS_PER_FILE
        throughput = int(total_rows / total_import_duration)
        logging.info(f"估算吞吐量: {throughput} 条/秒")

    # 验证
    base_cmd = build_psql_prefix(interactive=True)
    verify_cmd = f"{base_cmd} -t -c 'SELECT count(1) FROM {table};' 2>/dev/null | tr -d '[:space:]'"
    try:
        result = run_command(verify_cmd, check=False, capture_output=True)
        final_count = result.stdout.strip()
        if result.returncode == 0 and final_count.isdigit():
            logging.info(f"数据库最终行数 ({table}): {final_count}")
        else:
            logging.warning("无法获取最终行数统计。")
    except Exception:
        pass
    
    logging.info("=" * 50)

if __name__ == "__main__":
    cli()
