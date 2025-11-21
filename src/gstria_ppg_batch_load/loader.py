import subprocess
import shlex
import logging
import os
from .utils import run_command, build_psql_prefix
from .db_ops import get_partition_name, backup_and_drop_indexes, reset_primary_key, restore_indexes


def import_single_file_with_lock(file_path, table_base_name, enable_pk_reset=False):
    """
    enable_pk_reset: True 则执行步骤 1.6 (Collatec 逻辑)，False 则跳过 (基础逻辑)
    """
    # 1. 获取分区
    try:
        partition_name = get_partition_name(table_base_name)
    except Exception as e:
        return subprocess.CompletedProcess("part", 1, stderr=str(e)), 0.0

    # 2. 索引/主键处理
    stored_sqls = []
    try:
        stored_sqls = backup_and_drop_indexes(partition_name)
        if enable_pk_reset:
            reset_primary_key(partition_name)
    except Exception as e:
        try:
            restore_indexes(stored_sqls)
        except:
            pass
        return subprocess.CompletedProcess("idx_opt", 1, stderr=str(e)), 0.0

    # 3. 导入 (Shell Pipeline + Timing)
    logging.info(f"      [Import] Shell 管道导入...")
    lock_tbl = f'"{table_base_name}_wa"'

    # --- 构造干净的 SQL ---
    # 注意：这里我们不需要任何 weird 的转义，因为我们通过环境变量传递它
    copy_sql = f"COPY public.{partition_name}(fid,geom,dtg,taxi_id) FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL '')"

    # --- 准备环境变量 ---
    # 复制当前环境并注入 SQL，避免污染全局环境
    cmd_env = os.environ.copy()
    cmd_env["PG_COPY_SQL"] = copy_sql

    safe_path = shlex.quote(str(file_path))

    # --- 构造 Shell 命令 ---
    # 关键点：使用 printf "%s\n" "$PG_COPY_SQL"
    # Shell 会安全地展开环境变量，保留其中的引号和特殊字符，不会被再次解析
    pipeline = (f"("
                f"printf '%s\\n' 'BEGIN;'; "
                f"printf '%s\\n' 'LOCK TABLE public.{lock_tbl} IN SHARE UPDATE EXCLUSIVE MODE;'; "
                f"printf '%s\\n' \"$PG_COPY_SQL\"; "  # <--- 引用环境变量
                f"cat {safe_path}; "
                f"if [ -n \"$(tail -c 1 {safe_path})\" ]; then echo ''; fi; "
                f"printf '%s\\n' '\\.'; "
                f"printf '%s\\n' 'COMMIT;'"
                f") | {build_psql_prefix(True)} -q -v ON_ERROR_STOP=1")

    # Shell 内部计时
    cmd = (f"t1=$(date +%s.%N); {pipeline}; rc=$?; t2=$(date +%s.%N); "
           f"echo \"TIME_METRIC:$t1:$t2\" >&2; exit $rc")

    copy_dur = 0.0
    try:
        # --- 传入 cmd_env ---
        res = run_command(cmd, check=False, capture_output=True, env=cmd_env)

        if res.stderr:
            for line in res.stderr.splitlines():
                if "TIME_METRIC:" in line:
                    try:
                        copy_dur = float(line.split(":")[2]) - float(line.split(":")[1])
                    except:
                        pass
        if res.returncode != 0: raise subprocess.CalledProcessError(res.returncode, cmd, res.stdout, res.stderr)
    except Exception as e:
        logging.error(f"      -> Import Error: {e}")
        if hasattr(e, 'stderr') and e.stderr: logging.error(f"      -> Stderr: {e.stderr.strip()}")
        try:
            restore_indexes(stored_sqls)
        except:
            pass
        return subprocess.CompletedProcess("copy", 1, stderr=str(e)), copy_dur

    # 4. 恢复索引
    try:
        restore_indexes(stored_sqls)
    except Exception as e:
        return subprocess.CompletedProcess("restore", 1, stderr=str(e)), copy_dur

    return res, copy_dur