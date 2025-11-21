import subprocess
import shlex
import logging
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
        # === 差异化逻辑 ===
        if enable_pk_reset:
            reset_primary_key(partition_name)
        # =================
    except Exception as e:
        try:
            restore_indexes(stored_sqls)
        except:
            pass
        return subprocess.CompletedProcess("idx_opt", 1, stderr=str(e)), 0.0

    # 3. 导入 (Shell Pipeline + Timing)
    logging.info(f"      [Import] Shell 管道导入...")
    lock_tbl = f'"{table_base_name}_wa"'

    # --- 修复点 1: 移除 E 前缀，简化引号嵌套 ---
    # 原来: DELIMITER E'|', NULL E''
    # 现在: DELIMITER '|', NULL ''
    copy_sql = f"COPY public.{partition_name}(fid,geom,dtg,taxi_id) FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL '')"

    safe_path = shlex.quote(str(file_path))

    # --- 修复点 2: 使用 printf 替代 echo ---
    # echo 在处理带有引号的字符串时，如果不小心可能会输出不符合预期的转义字符
    # printf '%s\n' 是最安全的打印方式
    pipeline = (f"("
                f"printf '%s\\n' 'BEGIN;'; "
                f"printf '%s\\n' 'LOCK TABLE public.{lock_tbl} IN SHARE UPDATE EXCLUSIVE MODE;'; "
                f"printf '%s\\n' {shlex.quote(copy_sql)}; "
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
        res = run_command(cmd, check=False, capture_output=True)
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