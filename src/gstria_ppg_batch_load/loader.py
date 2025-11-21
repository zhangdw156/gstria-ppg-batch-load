import subprocess
import shlex
import logging
from .utils import run_command, build_psql_prefix
from .db_ops import get_partition_name, backup_and_drop_indexes, reset_primary_key, restore_indexes

def import_single_file_with_lock(file_path, table_base_name, enable_pk_reset=False):
    """
    完全复用"第一段代码"的成功逻辑：
    1. 获取分区
    2. 索引/主键处理 (根据 enable_pk_reset 决定是否重置主键)
    3. 执行 COPY (Shell Pipeline + Shell In-band Timing) - 逻辑照搬
    4. 恢复索引
    """

    # --- 步骤 1: 获取动态分区表名 ---
    try:
        partition_name = get_partition_name(table_base_name)
    except Exception as e:
        return subprocess.CompletedProcess("part", 1, stderr=str(e)), 0.0

    # --- 步骤 2: 索引处理 ---
    stored_index_sqls = []
    try:
        stored_index_sqls = backup_and_drop_indexes(partition_name)
        # === 差异化逻辑: Collatec 模式下重置主键 ===
        if enable_pk_reset:
            reset_primary_key(partition_name)
        # ========================================
    except Exception as e:
        try:
            restore_indexes(stored_index_sqls)
        except:
            pass
        return subprocess.CompletedProcess("index_opt", 1, stderr=str(e)), 0.0

    # --- 步骤 3: 极致速度导入 (逻辑完全照搬第一段代码) ---
    logging.info(f"      [Import] Shell 管道导入...")

    lock_table_name = f'"{table_base_name}_wa"'
    # 照搬: 使用 PostgreSQL 扩展语法 E'|' 和 E''
    copy_options = "WITH (FORMAT text, DELIMITER E'|', NULL E'')"
    copy_cmd_str = f"COPY public.{partition_name}(fid,geom,dtg,taxi_id) FROM STDIN {copy_options};"

    safe_file_path = shlex.quote(str(file_path))

    # 照搬: 核心业务管道构建
    # 使用 shlex.quote 确保 SQL 字符串在 Shell echo 中安全
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

    # 照搬: Shell 内部计时逻辑
    timed_shell_cmd = (
        f"ts_start=$(date +%s.%N); "  # 1. 记录 Shell 启动时刻
        f"{core_pipeline}; "          # 2. 执行核心管道
        f"exit_code=$?; "             # 3. 捕获退出码
        f"ts_end=$(date +%s.%N); "    # 4. 记录 Shell 结束时刻
        f"echo \"TIME_METRIC:$ts_start:$ts_end\" >&2; "  # 5. 输出时间标记到 stderr
        f"exit $exit_code"            # 6. 返回原始退出码
    )

    copy_duration = 0.0

    try:
        # 执行命令
        result = run_command(timed_shell_cmd, check=False, capture_output=True)

        # 解析 Shell 返回的精确时间
        if result.stderr:
            for line in result.stderr.splitlines():
                if "TIME_METRIC:" in line:
                    try:
                        # 格式: TIME_METRIC:171000000.123:171000001.456
                        _, t_start, t_end = line.split(":")
                        copy_duration = float(t_end) - float(t_start)
                    except ValueError:
                        logging.warning("无法解析 Shell 时间戳")
                        pass

        if result.returncode != 0:
            raise subprocess.CalledProcessError(result.returncode, timed_shell_cmd, output=result.stdout, stderr=result.stderr)

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