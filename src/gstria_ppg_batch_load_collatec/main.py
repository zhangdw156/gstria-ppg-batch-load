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
CONTAINER_NAME = os.getenv("PG_CONTAINER_NAME", "my-postgis-container")


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
    flags = "-i" if interactive else ""
    if CONTAINER_NAME:
        return f"docker exec {flags} {CONTAINER_NAME} psql -U {DB_USER} -d {DB_NAME}"
    else:
        return f"psql -U {DB_USER} -d {DB_NAME}"


def run_command(cmd, check=True, capture_output=False):
    try:
        result = subprocess.run(
            cmd, shell=True, check=check, capture_output=capture_output, text=True
        )
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"命令执行失败: {e.cmd}")
        if capture_output and e.stderr:
            logging.error(f"错误输出: {e.stderr.strip()}")
        raise e


def run_sql_command(sql, fetch_output=False):
    base_cmd = build_psql_prefix(interactive=False)
    flags = " -tA" if fetch_output else ""
    cmd = f"{base_cmd}{flags} -c {shlex.quote(sql)}"
    return run_command(cmd, capture_output=fetch_output)


# ==================== 索引管理函数 ====================

def backup_and_drop_indexes(partition_name_quoted):
    """
    备份并删除非主键索引
    """
    partition_name_pure = partition_name_quoted.replace('"', '')
    logging.info(f"      [Index Backup & Drop] 正在分析 {partition_name_quoted} 的辅助索引...")

    backup_sql = (
        f"SELECT i.relname, pg_get_indexdef(ix.indexrelid) "
        f"FROM pg_index ix "
        f"JOIN pg_class t ON t.oid = ix.indrelid "
        f"JOIN pg_class i ON i.oid = ix.indexrelid "
        f"JOIN pg_namespace n ON n.oid = t.relnamespace "
        f"WHERE t.relname = '{partition_name_pure}' "
        f"AND n.nspname = 'public' "
        f"AND ix.indisprimary = 'f';"
    )

    restore_sqls = []

    try:
        cmd_result = run_sql_command(backup_sql, fetch_output=True)
        lines = [line for line in cmd_result.stdout.strip().split('\n') if line]

        if not lines:
            logging.info("      -> 未发现辅助索引，无需操作。")
            return []

        for line in lines:
            parts = line.split('|', 1)
            if len(parts) < 2:
                continue

            idx_name = parts[0]
            idx_def = parts[1]

            restore_sqls.append(f"{idx_def};")

            logging.info(f"      -> Backup & Dropping: {idx_name}")
            run_sql_command(f"DROP INDEX IF EXISTS \"public\".\"{idx_name}\";")

        logging.info(f"      -> 已备份并删除 {len(restore_sqls)} 个辅助索引。")
        return restore_sqls

    except Exception as e:
        logging.error(f"      -> 索引备份/删除失败: {e}")
        raise e


def reset_primary_key(partition_name_quoted):
    """
    【新增 Step 1.6】重置主键索引：先删除，再立即重建。
    使用 pg_get_constraintdef 保持泛化性，不硬编码字段。
    """
    partition_name_pure = partition_name_quoted.replace('"', '')
    logging.info(f"      [PKey Reset] 正在重置 {partition_name_quoted} 的主键 (Drop -> Create)...")

    # 1. 获取主键约束名称和定义
    # contype = 'p' 代表 Primary Key
    find_pkey_sql = (
        f"SELECT conname, pg_get_constraintdef(oid) "
        f"FROM pg_constraint "
        f"WHERE conrelid = 'public.{partition_name_pure}'::regclass "
        f"AND contype = 'p';"
    )

    try:
        result = run_sql_command(find_pkey_sql, fetch_output=True)
        output = result.stdout.strip()

        if not output:
            logging.warning(f"      -> 未找到主键约束，跳过重置。")
            return

        # 解析输出 (conname|definition)
        parts = output.split('|', 1)
        if len(parts) < 2:
            logging.warning(f"      -> 主键信息解析失败，跳过重置: {output}")
            return

        pkey_name = parts[0]
        pkey_def = parts[1]  # 例如 "PRIMARY KEY (fid, dtg)"

        # 2. 删除主键约束
        logging.info(f"      -> Dropping PKey: {pkey_name}")
        drop_sql = f"ALTER TABLE \"public\".\"{partition_name_pure}\" DROP CONSTRAINT IF EXISTS \"{pkey_name}\";"
        run_sql_command(drop_sql)

        # 3. 立即重建主键约束
        logging.info(f"      -> Recreating PKey: {pkey_name} ...")
        create_sql = f"ALTER TABLE \"public\".\"{partition_name_pure}\" ADD CONSTRAINT \"{pkey_name}\" {pkey_def};"

        # 计时主键重建
        t_start = time.time()
        run_sql_command(create_sql)
        t_cost = time.time() - t_start

        logging.info(f"      -> 主键重置完毕 (耗时: {t_cost:.2f}s)。")

    except Exception as e:
        logging.error(f"      -> 主键重置失败: {e}")
        # 这里抛出异常，因为如果主键没了又没建成功，后续可能有问题
        raise e


def restore_indexes(restore_sqls):
    """
    恢复辅助索引
    """
    if not restore_sqls:
        return

    logging.info(f"      [Index Restore] 正在恢复 {len(restore_sqls)} 个辅助索引...")
    try:
        for sql in restore_sqls:
            run_sql_command(sql)
        logging.info("      -> 辅助索引恢复完毕。")
    except Exception as e:
        logging.error(f"      -> 索引恢复失败: {e}")
        raise e


# ==================== 核心导入函数 ====================
def import_single_file_with_lock(file_path, table_base_name):
    """
    处理单个文件的完整流程。

    返回: (CompletedProcess, row_count, pure_copy_duration)
    """

    # --- 步骤 1: 获取动态分区表名 ---
    get_partition_sql = (
        f"SELECT '\"{table_base_name}_wa_' || lpad(value::text, 3, '0') || '\"' "
        f"FROM \"public\".\"geomesa_wa_seq\" WHERE type_name = '{table_base_name}'"
    )

    try:
        result = run_sql_command(get_partition_sql, fetch_output=True)
        partition_name = result.stdout.strip()
        if not partition_name:
            raise ValueError(f"未能从 geomesa_wa_seq 获取分区名 (table: {table_base_name})")
        logging.info(f"      -> 动态获取分区表名: {partition_name}")
    except Exception as e:
        logging.error(f"      -> 获取分区表名失败: {e}")
        return subprocess.CompletedProcess(args="get_partition", returncode=1, stderr=str(e)), 0, 0.0

    # --- 步骤 2: 备份并删除辅助索引 ---
    stored_index_sqls = []
    try:
        stored_index_sqls = backup_and_drop_indexes(partition_name)
    except Exception as e:
        return subprocess.CompletedProcess(args="drop_index", returncode=1, stderr=str(e)), 0, 0.0

    # --- 步骤 2.5 (即 Step 1.6): 重置主键索引 ---
    try:
        reset_primary_key(partition_name)
    except Exception as e:
        # 如果主键操作失败，为了安全，尝试恢复辅助索引后退出
        try:
            restore_indexes(stored_index_sqls)
        except:
            pass
        return subprocess.CompletedProcess(args="reset_pkey", returncode=1, stderr=str(e)), 0, 0.0

    # --- 步骤 3: 使用 Popen 进行数据传输 (计时重点区域) ---
    logging.info(f"      [Import] 开始数据导入事务...")
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
    rows_in_file = 0
    copy_duration = 0.0  # 初始化纯复制耗时

    try:
        # ========== 计时开始 (仅统计 COPY 过程) ==========
        t_start_copy = time.time()

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
                rows_in_file += chunk.count(b'\n')
                proc.stdin.write(chunk)
                if chunk:
                    has_trailing_newline = chunk.endswith(b'\n')

        if not has_trailing_newline:
            proc.stdin.write(b'\n')
            rows_in_file += 1

        proc.stdin.write(sql_footer)

        stdout_bytes, stderr_bytes = proc.communicate()

        # ========== 计时结束 ==========
        t_end_copy = time.time()
        copy_duration = t_end_copy - t_start_copy

        stdout_str = stdout_bytes.decode('utf-8', errors='replace')
        stderr_str = stderr_bytes.decode('utf-8', errors='replace')

        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, psql_cmd, output=stdout_str, stderr=stderr_str)

    except Exception as e:
        error_msg = f"Import 异常: {str(e)}"
        logging.error(error_msg)
        if proc:
            proc.kill()
        # 尝试恢复索引
        try:
            logging.warning("      -> 导入失败，尝试恢复索引...")
            restore_indexes(stored_index_sqls)
        except:
            pass
        return subprocess.CompletedProcess(args=psql_cmd, returncode=1, stderr=error_msg), 0, copy_duration

    # --- 步骤 4: 恢复辅助索引 ---
    try:
        restore_indexes(stored_index_sqls)
    except Exception as e:
        return subprocess.CompletedProcess(args="restore_index", returncode=1,
                                           stderr=str(e)), rows_in_file, copy_duration

    return subprocess.CompletedProcess(args=psql_cmd, returncode=0, stdout=stdout_str,
                                       stderr=stderr_str), rows_in_file, copy_duration


# ==================== Click 命令行入口 ====================
@click.command()
@click.option('-f', '--table', required=True, help="目标表名 (Target Table Base Name)")
@click.option('-d', '--directory', required=True, type=click.Path(exists=True, file_okay=False),
              help="TBL 文件所在目录")
@click.option('--clean/--no-clean', default=True, help="是否在导入前清空目标表 (默认清空)")
def cli(table, directory, clean):
    """批量导入 .tbl 工具 (Step 1.6 重置主键版)"""
    setup_logging()
    tbl_dir = Path(directory)

    logging.info("=" * 50)
    start_total_time = time.time()
    logging.info(f"开始全量数据导入流程")
    logging.info(f"目标表: {table}")
    logging.info("=" * 50)

    if clean:
        logging.info(f"\n>>> 阶段 1: 清空数据 '{table}'...")
        try:
            run_sql_command(f"DELETE FROM \"public\".\"{table}\";")
            logging.info("表数据已清空。")
        except subprocess.CalledProcessError:
            logging.error("清空表失败，程序终止。")
            sys.exit(1)
    else:
        logging.info(f"\n>>> 阶段 1: 跳过清空数据...")

    logging.info(f"\n>>> 阶段 2: 查找数据文件...")
    tbl_files = sorted(tbl_dir.glob("*.tbl"))
    total_files = len(tbl_files)
    if total_files == 0:
        logging.error(f"未找到 .tbl 文件。")
        sys.exit(1)
    logging.info(f"共找到 {total_files} 个文件。")

    logging.info(f"\n>>> 阶段 3: 开始循环导入 (Auto Index Backup & Drop -> Reset PKey -> Import -> Restore)...")
    success_count = 0
    fail_count = 0

    # 统计纯 COPY 的总耗时
    total_pure_copy_duration = 0.0
    total_rows_imported = 0

    for i, file_path in enumerate(tbl_files, 1):
        filename = file_path.name
        logging.info(f"  -> ({i}/{total_files}) 正在处理: {filename} ... ")

        process_start = time.time()

        # 调用核心函数
        result, row_count, pure_copy_duration = import_single_file_with_lock(file_path, table)

        process_end = time.time()
        process_duration = process_end - process_start

        if result.returncode == 0:
            success_count += 1
            total_pure_copy_duration += pure_copy_duration
            total_rows_imported += row_count

            speed = int(row_count / pure_copy_duration) if pure_copy_duration > 0 else 0

            logging.info(
                f"     ✅ 成功 (纯COPY耗时: {pure_copy_duration:.2f}s | 总耗时: {process_duration:.2f}s | 行数: {row_count} | 速度: {speed}/s)")
        else:
            fail_count += 1
            logging.error(f"     ❌ 失败: {filename}")
            if result.stderr:
                logging.error(f"     DETAILS: {result.stderr.strip()}")

    # 报告
    logging.info(f"\n>>> 阶段 4: 统计...")
    logging.info("-" * 30)

    real_total_time = time.time() - start_total_time
    logging.info(f"脚本运行时长(含索引开销): {real_total_time:.3f}s | 成功: {success_count} | 失败: {fail_count}")

    logging.info("-" * 30)
    logging.info(f"成功导入总行数: {total_rows_imported}")
    logging.info(f"纯 COPY 总耗时: {total_pure_copy_duration:.3f}s")

    if success_count > 0 and total_pure_copy_duration > 0:
        avg_throughput = int(total_rows_imported / total_pure_copy_duration)
        logging.info(f"整体纯导入吞吐量: {avg_throughput} 条/秒")

    logging.info("-" * 30)

    # 验证
    verify_cmd = build_psql_prefix(interactive=False) + f" -t -c 'SELECT count(1) FROM \"public\".\"{table}\";'"
    try:
        result = run_command(verify_cmd, check=False, capture_output=True)
        db_count = int(result.stdout.strip()) if result.stdout.strip().isdigit() else -1

        if db_count == total_rows_imported:
            logging.info(f"✅ 验证通过: 数据库行数 {db_count} 与 导入行数 {total_rows_imported} 一致。")
        else:
            logging.warning(f"⚠️ 验证失败: 数据库 {db_count} vs 导入 {total_rows_imported}")
    except Exception as e:
        logging.warning(f"验证出错: {e}")


if __name__ == "__main__":
    cli()