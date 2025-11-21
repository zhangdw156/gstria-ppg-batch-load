import subprocess
import shlex
import logging
from .utils import run_command, build_psql_prefix
from .db_ops import get_partition_name, backup_and_drop_indexes, reset_primary_key, restore_indexes

def import_single_file_with_lock(file_path, table_base_name, enable_pk_reset=False):
    """
    修改后：返回 (CompletedProcess, stats_dict)
    stats_dict 包含: partition, t_idx_drop, t_pk_reset, t_copy, t_idx_restore
    """

    # 初始化统计字典
    stats = {
        "partition": "N/A",
        "t_idx_drop": 0.0,
        "t_pk_reset": 0.0,
        "t_copy": 0.0,
        "t_idx_restore": 0.0
    }

    # --- 步骤 1: 获取动态分区表名 ---
    try:
        partition_name = get_partition_name(table_base_name)
        stats["partition"] = partition_name
    except Exception as e:
        return subprocess.CompletedProcess("part", 1, stderr=str(e)), stats

    # --- 步骤 2: 索引处理 ---
    stored_index_sqls = []
    try:
        # 计时：备份并删除索引
        t0 = time.time()
        stored_index_sqls = backup_and_drop_indexes(partition_name)
        stats["t_idx_drop"] = time.time() - t0

        # === 差异化逻辑: Collatec 模式下重置主键 ===
        if enable_pk_reset:
            # 计时：重置主键
            t1 = time.time()
            reset_primary_key(partition_name)
            stats["t_pk_reset"] = time.time() - t1
        # ========================================
    except Exception as e:
        try:
            restore_indexes(stored_index_sqls)
        except:
            pass
        return subprocess.CompletedProcess("index_opt", 1, stderr=str(e)), stats

    # --- 步骤 3: 极致速度导入 ---
    logging.info(f"      [Import] Shell 管道导入...")

    lock_table_name = f'"{table_base_name}_wa"'
    copy_options = "WITH (FORMAT text, DELIMITER E'|', NULL E'')"
    copy_cmd_str = f"COPY public.{partition_name}(fid,geom,dtg,taxi_id) FROM STDIN {copy_options};"

    safe_file_path = shlex.quote(str(file_path))

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

    timed_shell_cmd = (
        f"ts_start=$(date +%s.%N); "
        f"{core_pipeline}; "
        f"exit_code=$?; "
        f"ts_end=$(date +%s.%N); "
        f"echo \"TIME_METRIC:$ts_start:$ts_end\" >&2; "
        f"exit $exit_code"
    )

    copy_duration = 0.0

    try:
        result = run_command(timed_shell_cmd, check=False, capture_output=True)

        if result.stderr:
            for line in result.stderr.splitlines():
                if "TIME_METRIC:" in line:
                    try:
                        _, t_start, t_end = line.split(":")
                        copy_duration = float(t_end) - float(t_start)
                    except ValueError:
                        pass

        # 记录 COPY 时间
        stats["t_copy"] = copy_duration

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
        return subprocess.CompletedProcess(args="copy", returncode=1, stderr=str(e)), stats

    # --- 步骤 4: 恢复索引 ---
    try:
        # 计时：恢复索引
        t2 = time.time()
        restore_indexes(stored_index_sqls)
        stats["t_idx_restore"] = time.time() - t2
    except Exception as e:
        return subprocess.CompletedProcess(args="restore_index", returncode=1, stderr=str(e)), stats

    return result, stats