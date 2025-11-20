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
    """
    执行 Shell 命令
    """
    try:
        # 注意：为了使用 Shell 管道特性 (cat | psql)，这里必须 shell=True
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
    # 使用 shlex.quote 确保 SQL 里的特殊字符不会破坏 shell 命令结构
    cmd = f"{base_cmd}{flags} -c {shlex.quote(sql)}"
    return run_command(cmd, capture_output=fetch_output)


# ==================== 泛化索引/主键管理函数 ====================

def backup_and_drop_indexes(partition_name_quoted):
    """
    Step 1.5: 备份并删除非主键索引 (泛化)
    """
    partition_name_pure = partition_name_quoted.replace('"', '')
    logging.info(f"      [Index Backup & Drop] 正在分析 {partition_name_quoted} 的辅助索引...")

    # 动态查询非主键索引定义
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
            # psql -tA 输出格式默认用 | 分隔
            parts = line.split('|', 1)
            if len(parts) < 2: continue

            idx_name = parts[0]
            idx_def = parts[1]

            restore_sqls.append(f"{idx_def};")

            logging.info(f"      -> Dropping: {idx_name}")
            run_sql_command(f"DROP INDEX IF EXISTS \"public\".\"{idx_name}\";")

        logging.info(f"      -> 已删除 {len(restore_sqls)} 个辅助索引。")
        return restore_sqls

    except Exception as e:
        logging.error(f"      -> 索引操作失败: {e}")
        raise e


def reset_primary_key(partition_name_quoted):
    """
    Step 1.6: 重置主键索引 (先删后建)
    使用 pg_get_constraintdef 实现泛化，不硬编码字段。
    """
    partition_name_pure = partition_name_quoted.replace('"', '')
    logging.info(f"      [PKey Reset] 正在重置 {partition_name_quoted} 的主键...")

    # 1. 动态查询主键定义
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
            logging.warning(f"      -> 未找到主键约束，跳过。")
            return

        parts = output.split('|', 1)
        if len(parts) < 2: return

        pkey_name = parts[0]
        pkey_def = parts[1]  # 例如 "PRIMARY KEY (fid, dtg)"

        # 2. 删除主键
        # logging.info(f"      -> Dropping PKey: {pkey_name}")
        drop_sql = f"ALTER TABLE \"public\".\"{partition_name_pure}\" DROP CONSTRAINT IF EXISTS \"{pkey_name}\";"
        run_sql_command(drop_sql)

        # 3. 立即重建主键
        # logging.info(f"      -> Recreating PKey: {pkey_name}")
        create_sql = f"ALTER TABLE \"public\".\"{partition_name_pure}\" ADD CONSTRAINT \"{pkey_name}\" {pkey_def};"

        t_start = time.time()
        run_sql_command(create_sql)
        logging.info(f"      -> 主键重置完成 (重建耗时: {time.time() - t_start:.2f}s)。")

    except Exception as e:
        logging.error(f"      -> 主键重置失败: {e}")
        raise e


def restore_indexes(restore_sqls):
    """
    Step 3.5: 恢复辅助索引
    """
    if not restore_sqls: return

    logging.info(f"      [Index Restore] 正在恢复 {len(restore_sqls)} 个辅助索引...")
    try:
        for sql in restore_sqls:
            run_sql_command(sql)
        logging.info("      -> 辅助索引恢复完毕。")
    except Exception as e:
        logging.error(f"      -> 索引恢复失败: {e}")
        raise e


# ==================== 核心导入函数 (极致速度 + Shell 内部计时版) ====================
def import_single_file_with_lock(file_path, table_base_name):
    """
    流程：
    1. 获取分区名
    2. 索引/主键处理
    3. 执行 COPY (Shell Pipeline + Shell In-band Timing)
    4. 恢复索引

    返回: (CompletedProcess, pure_copy_duration)
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
            raise ValueError("分区名查询为空")
        logging.info(f"      -> 分区表: {partition_name}")
    except Exception as e:
        logging.error(f"      -> 获取分区失败: {e}")
        return subprocess.CompletedProcess(args="get_partition", returncode=1, stderr=str(e)), 0.0

    # --- 步骤 2: 索引处理 ---
    stored_index_sqls = []
    try:
        stored_index_sqls = backup_and_drop_indexes(partition_name)
        reset_primary_key(partition_name)
    except Exception as e:
        try:
            restore_indexes(stored_index_sqls)
        except:
            pass
        return subprocess.CompletedProcess(args="index_opt", returncode=1, stderr=str(e)), 0.0

    # --- 步骤 3: 极致速度导入 (Shell Pipeline + Internal Timing) ---
    logging.info(f"      [Import] 开始 Shell 管道高速导入...")

    lock_table_name = f'"{table_base_name}_wa"'
    copy_options = "WITH (FORMAT text, DELIMITER E'|', NULL E'')"
    copy_cmd_str = f"COPY public.{partition_name}(fid,geom,dtg,taxi_id) FROM STDIN {copy_options};"

    safe_file_path = shlex.quote(str(file_path))

    # 构建核心业务管道
    core_pipeline = (
        f"("
        f"echo 'BEGIN;';"
        f"echo 'LOCK TABLE public.{lock_table_name} IN SHARE UPDATE EXCLUSIVE MODE;';"
        f"echo {shlex.quote(copy_cmd_str)};"
        f"cat {safe_file_path};"
        f"if [ -n \"$(tail -c 1 {safe_file_path})\" ]; then echo ''; fi;"
        f"echo '\\.'; "
        f"echo 'COMMIT;'"
        f") | {build_psql_prefix(interactive=True)} -q -v ON_ERROR_STOP=1"
    )

    # 【关键修改】在 Shell 内部包裹一层计时逻辑
    # 使用 date +%s.%N 获取纳秒级时间戳
    # 将时间信息写入 stderr (>&2)，以免干扰 stdout (虽然 psql -q 不太会有输出)
    timed_shell_cmd = (
        f"ts_start=$(date +%s.%N); "  # 1. 记录 Shell 启动时刻
        f"{core_pipeline}; "  # 2. 执行核心管道
        f"exit_code=$?; "  # 3. 捕获退出码
        f"ts_end=$(date +%s.%N); "  # 4. 记录 Shell 结束时刻
        f"echo \"TIME_METRIC:$ts_start:$ts_end\" >&2; "  # 5. 输出时间标记到 stderr
        f"exit $exit_code"  # 6. 返回原始退出码
    )

    copy_duration = 0.0

    try:
        # 执行命令 (这里 Python 只是个触发器，不负责计时)
        result = run_command(timed_shell_cmd, check=False, capture_output=True)

        # 解析 Shell 返回的精确时间
        if result.stderr:
            for line in result.stderr.splitlines():
                if "TIME_METRIC:" in line:
                    try:
                        # 格式: TIME_METRIC:171000000.123:171000001.456
                        _, t_start, t_end = line.split(":")
                        copy_duration = float(t_end) - float(t_start)
                        # 从 stderr 中移除这一行，保持日志干净 (可选)
                        # result.stderr = result.stderr.replace(line, "")
                    except ValueError:
                        logging.warning("无法解析 Shell 时间戳，回退到 Python 计时")
                        pass

        if result.returncode != 0:
            raise subprocess.CalledProcessError(result.returncode, timed_shell_cmd, output=result.stdout,
                                                stderr=result.stderr)

    except Exception as e:
        logging.error(f"      -> 导入失败: {e}")
        if hasattr(e, 'stderr') and e.stderr:
            logging.error(f"      -> 输出: {e.stderr.strip()}")

        try:
            restore_indexes(stored_index_sqls)
        except:
            pass
        return subprocess.CompletedProcess(args="copy", returncode=1, stderr=str(e)), copy_duration

    # --- 步骤 4: 恢复索引 ---
    try:
        restore_indexes(stored_index_sqls)
    except Exception as e:
        return subprocess.CompletedProcess(args="restore_index", returncode=1, stderr=str(e)), copy_duration

    return result, copy_duration


# ==================== Click 命令行入口 ====================
@click.command()
@click.option('-f', '--table', required=True, help="目标表名")
@click.option('-d', '--directory', required=True, type=click.Path(exists=True, file_okay=False), help="数据目录")
@click.option('--clean/--no-clean', default=True, help="导入前清空表")
def cli(table, directory, clean):
    """批量导入工具 (Python逻辑控制 + Shell原生速度)"""
    setup_logging()
    tbl_dir = Path(directory)

    logging.info("=" * 50)
    start_total_time = time.time()
    logging.info(f"开始全量数据导入流程 (混合模式)")
    logging.info("=" * 50)

    if clean:
        logging.info(f"\n>>> 阶段 1: 清空数据 '{table}'...")
        try:
            run_sql_command(f"DELETE FROM \"public\".\"{table}\";")
            logging.info("表数据已清空。")
        except:
            sys.exit(1)
    else:
        logging.info(f"\n>>> 阶段 1: 跳过清空...")

    logging.info(f"\n>>> 阶段 2: 扫描文件...")
    tbl_files = sorted(tbl_dir.glob("*.tbl"))
    total_files = len(tbl_files)
    if total_files == 0:
        logging.error("未找到文件。")
        sys.exit(1)
    logging.info(f"共 {total_files} 个文件。")

    logging.info(f"\n>>> 阶段 3: 导入 (Index Opt + PKey Reset + Shell Pipe)...")

    success_count = 0
    fail_count = 0
    total_pure_copy_duration = 0.0

    for i, file_path in enumerate(tbl_files, 1):
        filename = file_path.name
        logging.info(f"  -> ({i}/{total_files}) 处理: {filename}")

        p_start = time.time()

        # 执行导入
        result, copy_time = import_single_file_with_lock(file_path, table)

        p_end = time.time()
        total = p_end - p_start

        if result.returncode == 0:
            success_count += 1
            total_pure_copy_duration += copy_time
            # 注意：为了速度，不再统计单文件行数，只显示时间
            logging.info(f"     ✅ 成功 (Shell COPY: {copy_time:.2f}s | 全流程: {total:.2f}s)")
        else:
            fail_count += 1
            logging.error(f"     ❌ 失败")

        # 统计
        logging.info(f"\n>>> 阶段 4: 统计...")
        real_time = time.time() - start_total_time
        logging.info(f"总运行时长: {real_time:.3f}s")
        logging.info(f"纯 COPY 耗时: {total_pure_copy_duration:.3f}s")

        # 验证 & 吞吐量计算
        try:
            verify_cmd = build_psql_prefix() + f" -t -c 'SELECT count(1) FROM \"public\".\"{table}\";'"
            res = run_command(verify_cmd, check=False, capture_output=True)
            cnt = int(res.stdout.strip()) if res.stdout.strip().isdigit() else -1
            logging.info(f"数据库最终行数: {cnt}")

            # ==================== 新增计算逻辑 ====================
            if cnt > 0:
                # 1. 综合吞吐量：总行数 / 脚本总运行时间 (包含索引重建、主键重置等所有开销)
                # 这是最真实的“业务吞吐量”
                tps_total = int(cnt / real_time) if real_time > 0 else 0

                # 2. 纯COPY吞吐量：总行数 / 纯数据传输时间
                # 这代表了网络/磁盘IO的极限性能
                tps_copy = int(cnt / total_pure_copy_duration) if total_pure_copy_duration > 0 else 0

                logging.info("-" * 30)
                logging.info(f"平均吞吐量 (Total): {tps_total} rows/s (含索引维护)")
                logging.info(f"纯COPY吞吐量 (Copy):  {tps_copy} rows/s (仅数据传输)")
                logging.info("-" * 30)
            # =====================================================

        except Exception as e:
            logging.warning(f"统计验证出错: {e}")


if __name__ == "__main__":
    cli()