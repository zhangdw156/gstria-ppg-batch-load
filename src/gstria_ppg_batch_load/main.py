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
    """
    使用 Python Popen 流式传输数据，并同时统计行数。
    返回: (CompletedProcess, row_count)
    """

    # --- 步骤 1: 获取动态分区表名 ---
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
        return subprocess.CompletedProcess(args=get_partition_cmd, returncode=1, stderr=str(e)), 0

    # --- 步骤 2: 使用 Popen 进行数据传输和行数统计 ---
    lock_table_name = f'"{table_base_name}_wa"'
    copy_options = "WITH (FORMAT text, DELIMITER E'|', NULL E'')"
    copy_sql = f"COPY public.{partition_name}(fid,geom,dtg,taxi_id) FROM STDIN {copy_options};"

    sql_header = (
        f"BEGIN;\n"
        f"LOCK TABLE public.{lock_table_name} IN SHARE UPDATE EXCLUSIVE MODE;\n"
        f"{copy_sql}\n"
    ).encode('utf-8')

    sql_footer = b"\\.\nCOMMIT;\n"

    psql_cmd = f"{build_psql_prefix(interactive=True)} -q -v ON_ERROR_STOP=1"

    proc = None
    rows_in_file = 0 # 初始化行数计数器

    try:
        proc = subprocess.Popen(
            psql_cmd,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # A. 写入 SQL 头部
        proc.stdin.write(sql_header)

        # B. 流式写入文件内容并统计行数
        has_trailing_newline = False
        with open(file_path, 'rb') as f:
            # 使用较大的 buffer size 提高读取效率
            while chunk := f.read(1024 * 1024): 
                # 统计二进制块中的换行符数量
                rows_in_file += chunk.count(b'\n')
                proc.stdin.write(chunk)
                if chunk:
                    has_trailing_newline = chunk.endswith(b'\n')

        # C. 补换行符 (如果最后一行没有换行符，需要补上并计入行数)
        if not has_trailing_newline:
            proc.stdin.write(b'\n')
            rows_in_file += 1

        # D. 写入 SQL 尾部
        proc.stdin.write(sql_footer)
        
        # E. 关闭 stdin 并获取输出
        stdout_bytes, stderr_bytes = proc.communicate()

        stdout_str = stdout_bytes.decode('utf-8', errors='replace')
        stderr_str = stderr_bytes.decode('utf-8', errors='replace')

        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, psql_cmd, output=stdout_str, stderr=stderr_str)

        return subprocess.CompletedProcess(args=psql_cmd, returncode=0, stdout=stdout_str, stderr=stderr_str), rows_in_file

    except Exception as e:
        error_msg = f"Import 异常: {str(e)}"
        logging.error(error_msg)
        if proc:
            proc.kill()
        return subprocess.CompletedProcess(args=psql_cmd, returncode=1, stderr=error_msg), 0


# ==================== Click 命令行入口 ====================
@click.command()
@click.option('-f', '--table', required=True, help="目标表名 (Target Table Base Name)")
@click.option('-d', '--directory', required=True, type=click.Path(exists=True, file_okay=False), help="TBL 文件所在目录")
@click.option('--clean/--no-clean', default=True, help="是否在导入前清空目标表 (默认清空)")
def cli(table, directory, clean):
    """批量导入 .tbl 工具 (自动统计行数版)"""
    setup_logging()
    tbl_dir = Path(directory)
    
    logging.info("=" * 50)
    start_total_time = time.time()
    logging.info(f"开始全量数据导入流程")
    logging.info(f"目标表: {table}")
    logging.info(f"数据目录: {tbl_dir}")
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
    
    total_import_duration = 0.0 # 纯导入耗时累加
    total_rows_imported = 0     # 成功导入的总行数累加

    for i, file_path in enumerate(tbl_files, 1):
        filename = file_path.name
        logging.info(f"  -> ({i}/{total_files}) 正在导入: {filename} ... ")

        import_start = time.time()
        # 调用函数，获取结果和行数
        result, row_count = import_single_file_with_lock(file_path, table)
        import_end = time.time()
        
        import_duration = import_end - import_start

        if result.returncode == 0:
            success_count += 1
            total_import_duration += import_duration
            total_rows_imported += row_count
            
            # 计算单文件速度
            speed = int(row_count / import_duration) if import_duration > 0 else 0
            logging.info(f"     ✅ 成功 (耗时: {import_duration:.2f}s | 行数: {row_count} | 速度: {speed} row/s)")
        else:
            fail_count += 1
            logging.error(f"     ❌ 失败: {filename}")
            if result.stderr:
                logging.error(f"     DETAILS: {result.stderr.strip()}")

    # 4. 生成报告
    logging.info(f"\n>>> 阶段 4: 统计与验证...")
    logging.info("=" * 50)

    end_total_time = time.time()
    total_script_duration = end_total_time - start_total_time

    logging.info(f"脚本总运行时长: {total_script_duration:.3f} 秒")
    logging.info(f"文件统计: 成功 {success_count} | 失败 {fail_count}")
    logging.info("-" * 30)
    logging.info(f"成功导入总行数: {total_rows_imported}")
    
    if success_count > 0 and total_import_duration > 0:
        avg_throughput = int(total_rows_imported / total_import_duration)
        avg_time_per_file = total_import_duration / success_count
        logging.info(f"平均导入耗时: {avg_time_per_file:.3f} 秒/文件")
        logging.info(f"整体纯导入吞吐量: {avg_throughput} 条/秒")
    
    logging.info("-" * 30)

    # 验证
    base_cmd = build_psql_prefix(interactive=True)
    verify_cmd = f"{base_cmd} -t -c 'SELECT count(1) FROM {table};' 2>/dev/null | tr -d '[:space:]'"
    
    try:
        result = run_command(verify_cmd, check=False, capture_output=True)
        db_count_str = result.stdout.strip()
        
        if result.returncode == 0 and db_count_str.isdigit():
            db_count = int(db_count_str)
            logging.info(f"数据库最终实际行数 ({table}): {db_count}")
            
            diff = db_count - total_rows_imported
            if diff == 0:
                logging.info("✅ 数据量验证完美匹配！")
            else:
                logging.warning(f"⚠️ 数据量不匹配！差异: {diff} (数据库: {db_count}, 脚本统计: {total_rows_imported})")
                logging.warning("   (提示: 如果 clean=False，数据库中可能包含旧数据)")
        else:
            logging.warning("无法获取最终行数统计。")
    except Exception as e:
        logging.warning(f"验证过程出错: {e}")
    
    logging.info("=" * 50)

if __name__ == "__main__":
    cli()
