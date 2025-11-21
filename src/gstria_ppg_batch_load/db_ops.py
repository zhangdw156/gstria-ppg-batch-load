import time
import logging
from .utils import run_sql_command
from .config import CRON_SCHEDULE_ROLL_WA, CRON_SCHEDULE_MAINTENANCE


def get_partition_name(table_base_name):
    sql = (f"SELECT '\"{table_base_name}_wa_' || lpad(value::text, 3, '0') || '\"' "
           f"FROM \"public\".\"geomesa_wa_seq\" WHERE type_name = '{table_base_name}'")
    try:
        res = run_sql_command(sql, fetch_output=True)
        if not res.stdout.strip(): raise ValueError("Empty partition name")
        logging.info(f"      -> 分区表: {res.stdout.strip()}")
        return res.stdout.strip()
    except Exception as e:
        logging.error(f"      -> Get Partition Failed: {e}")
        raise e


def backup_and_drop_indexes(partition_name_quoted):
    pure_name = partition_name_quoted.replace('"', '')
    logging.info(f"      [Index Backup] 分析 {partition_name_quoted} 辅助索引...")
    sql = (f"SELECT i.relname, pg_get_indexdef(ix.indexrelid) FROM pg_index ix "
           f"JOIN pg_class t ON t.oid = ix.indrelid JOIN pg_class i ON i.oid = ix.indexrelid "
           f"JOIN pg_namespace n ON n.oid = t.relnamespace WHERE t.relname = '{pure_name}' "
           f"AND n.nspname = 'public' AND ix.indisprimary = 'f';")
    restore_sqls = []
    try:
        res = run_sql_command(sql, fetch_output=True)
        lines = [l for l in res.stdout.strip().split('\n') if l]
        if not lines:
            logging.info("      -> 无辅助索引。")
            return []
        for line in lines:
            parts = line.split('|', 1)
            if len(parts) < 2: continue
            restore_sqls.append(f"{parts[1]};")
            run_sql_command(f"DROP INDEX IF EXISTS \"public\".\"{parts[0]}\";")
        logging.info(f"      -> 删除 {len(restore_sqls)} 个辅助索引。")
        return restore_sqls
    except Exception as e:
        logging.error(f"      -> Index Ops Failed: {e}")
        raise e


def reset_primary_key(partition_name_quoted):
    """Step 1.6: 仅在 Collatec 模式下调用"""
    pure_name = partition_name_quoted.replace('"', '')
    logging.info(f"      [PKey Reset] 重置 {partition_name_quoted} 主键...")
    sql = (f"SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint "
           f"WHERE conrelid = 'public.{pure_name}'::regclass AND contype = 'p';")
    try:
        res = run_sql_command(sql, fetch_output=True)
        if not res.stdout.strip(): return
        parts = res.stdout.strip().split('|', 1)
        if len(parts) < 2: return
        pk_name, pk_def = parts[0], parts[1]
        run_sql_command(f"ALTER TABLE \"public\".\"{pure_name}\" DROP CONSTRAINT IF EXISTS \"{pk_name}\";")
        t0 = time.time()
        run_sql_command(f"ALTER TABLE \"public\".\"{pure_name}\" ADD CONSTRAINT \"{pk_name}\" {pk_def};")
        logging.info(f"      -> 主键重建完成 (耗时: {time.time() - t0:.2f}s)。")
    except Exception as e:
        logging.error(f"      -> PKey Reset Failed: {e}")
        raise e


def restore_indexes(restore_sqls):
    if not restore_sqls: return
    logging.info(f"      [Index Restore] 恢复 {len(restore_sqls)} 个索引...")
    try:
        for sql in restore_sqls: run_sql_command(sql)
    except Exception as e:
        logging.error(f"      -> Restore Failed: {e}")
        raise e


def update_cron_jobs(table_base_name):
    """
    根据 .env 配置更新 pg_cron 的调度时间
    注意 jobname 的命名规则:
    1. {table}-roll-wa
    2. {table}_partition_maintenance
    """
    logging.info(">>> 检查定时任务配置...")

    # 1. 更新 roll-wa 任务
    if CRON_SCHEDULE_ROLL_WA:
        job_name = f"{table_base_name}-roll-wa"
        logging.info(f"   -> 更新 Cron Job '{job_name}' Schedule 为: {CRON_SCHEDULE_ROLL_WA}")
        sql = (f"UPDATE cron.job SET schedule = '{CRON_SCHEDULE_ROLL_WA}' "
               f"WHERE jobname = '{job_name}';")
        try:
            run_sql_command(sql)
        except Exception as e:
            logging.warning(f"   -> 更新失败 (可能缺少权限或表不存在): {e}")
    else:
        logging.info(f"   -> 跳过更新 {table_base_name}-roll-wa (配置为空)")

    # 2. 更新 partition_maintenance 任务
    if CRON_SCHEDULE_MAINTENANCE:
        job_name = f"{table_base_name}_partition_maintenance"
        logging.info(f"   -> 更新 Cron Job '{job_name}' Schedule 为: {CRON_SCHEDULE_MAINTENANCE}")
        sql = (f"UPDATE cron.job SET schedule = '{CRON_SCHEDULE_MAINTENANCE}' "
               f"WHERE jobname = '{job_name}';")
        try:
            run_sql_command(sql)
        except Exception as e:
            logging.warning(f"   -> 更新失败: {e}")
    else:
        logging.info(f"   -> 跳过更新 {table_base_name}_partition_maintenance (配置为空)")
