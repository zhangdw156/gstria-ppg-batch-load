import subprocess
import shlex
import logging
import time
from .utils import run_command, build_psql_prefix
from .db_ops import get_partition_name, backup_and_drop_indexes, reset_primary_key, restore_indexes


def import_single_file_with_lock(file_path, table_base_name, enable_pk_reset=False):
    """
    修改后：返回 (CompletedProcess, metrics_dict)
    """

    # 初始化指标字典
    metrics = {
        "time_drop_index": 0.0,
        "time_reset_pk": 0.0,
        "time_copy": 0.0,
        "time_restore_index": 0.0,
        "partition_name": "N/A"  # 确保有默认值
    }

    # --- 步骤 1: 获取动态分区表名 ---
    try:
        partition_name = get_partition_name(table_base_name)
        metrics["partition_name"] = partition_name  # <--- 关键点：这里存入字典
    except Exception as e:
        return subprocess.CompletedProcess("part", 1, stderr=str(e)), metrics

    # --- 步骤 2: 索引处理 ---
    stored_index_sqls = []
    try:
        t_start = time.time()
        stored_index_sqls = backup_and_drop_indexes(partition_name)
        metrics["time_drop_index"] = time.time() - t_start

        if enable_pk_reset:
            t_start = time.time()
            reset_primary_key(partition_name)
            metrics["time_reset_pk"] = time.time() - t_start
    except Exception as e:
        try:
            restore_indexes(stored_index_sqls)
        except:
            pass
        return subprocess.CompletedProcess("index_opt", 1, stderr=str(e)), metrics

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

        metrics["time_copy"] = copy_duration

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
        return subprocess.CompletedProcess(args="copy", returncode=1, stderr=str(e)), metrics

    # --- 步骤 4: 恢复索引 ---
    try:
        t_start = time.time()
        restore_indexes(stored_index_sqls)
        metrics["time_restore_index"] = time.time() - t_start
    except Exception as e:
        return subprocess.CompletedProcess(args="restore_index", returncode=1, stderr=str(e)), metrics

    return result, metrics
