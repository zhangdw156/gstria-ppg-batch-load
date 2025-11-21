import subprocess
import shlex
import logging
import os
import tempfile  # <--- 新增引入
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

    # --- 彻底修复方案: 使用临时文件承载 SQL ---
    # 这样不需要考虑任何 Shell 转义、引号嵌套或环境变量丢失的问题

    copy_sql = f"COPY public.{partition_name}(fid,geom,dtg,taxi_id) FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL '')"

    # 构造 SQL 头文件内容
    sql_header_content = (
        f"BEGIN;\n"
        f"LOCK TABLE public.{lock_tbl} IN SHARE UPDATE EXCLUSIVE MODE;\n"
        f"{copy_sql}\n"
    )

    # 创建临时文件
    tmp_header_path = ""
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as tmp_f:
            tmp_f.write(sql_header_content)
            tmp_header_path = tmp_f.name

        # 构造安全路径
        safe_header_path = shlex.quote(tmp_header_path)
        safe_data_path = shlex.quote(str(file_path))

        # 构造管道: cat SQL头 -> cat 数据 -> echo 结束符 -> psql
        pipeline = (f"("
                    f"cat {safe_header_path}; "
                    f"cat {safe_data_path}; "
                    f"if [ -n \"$(tail -c 1 {safe_data_path})\" ]; then echo ''; fi; "
                    f"printf '%s\\n' '\\.'; "
                    f"printf '%s\\n' 'COMMIT;'"
                    f") | {build_psql_prefix(True)} -q -v ON_ERROR_STOP=1")

        # Shell 内部计时
        cmd = (f"t1=$(date +%s.%N); {pipeline}; rc=$?; t2=$(date +%s.%N); "
               f"echo \"TIME_METRIC:$t1:$t2\" >&2; exit $rc")

        copy_dur = 0.0

        # 执行
        res = run_command(cmd, check=False, capture_output=True)

        if res.stderr:
            for line in res.stderr.splitlines():
                if "TIME_METRIC:" in line:
                    try:
                        copy_dur = float(line.split(":")[2]) - float(line.split(":")[1])
                    except:
                        pass
        if res.returncode != 0:
            raise subprocess.CalledProcessError(res.returncode, cmd, res.stdout, res.stderr)

    except Exception as e:
        logging.error(f"      -> Import Error: {e}")
        if hasattr(e, 'stderr') and e.stderr: logging.error(f"      -> Stderr: {e.stderr.strip()}")
        try:
            restore_indexes(stored_sqls)
        except:
            pass
        return subprocess.CompletedProcess("copy", 1, stderr=str(e)), copy_dur

    finally:
        # 清理临时文件
        if tmp_header_path and os.path.exists(tmp_header_path):
            os.remove(tmp_header_path)

    # 4. 恢复索引
    try:
        restore_indexes(stored_sqls)
    except Exception as e:
        return subprocess.CompletedProcess("restore", 1, stderr=str(e)), copy_dur

    return res, copy_dur