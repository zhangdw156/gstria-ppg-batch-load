#!/usr/bin/env python3
import sys
import time
import logging
import click
import swanlab  # 引入 swanlab
from pathlib import Path
from .config import setup_logging
from .utils import run_sql_command, run_command, build_psql_prefix
from .loader import import_single_file_with_lock


def run_main_logic(table, directory, clean, enable_pk_reset):
    """通用业务逻辑控制器"""
    setup_logging()
    tbl_dir = Path(directory)
    mode_name = "Collatec Mode" if enable_pk_reset else "Standard Mode"
    full_mode_desc = "Collatec Mode (含主键重置)" if enable_pk_reset else "Standard Mode (基础模式)"

    logging.info("=" * 60)
    start_time = time.time()
    logging.info(f"开始数据导入流程 - {full_mode_desc}")
    logging.info("=" * 60)

    # ==========================
    # SwanLab 初始化
    # ==========================
    swanlab.init(
        project="PG-Batch-Load-Monitor",
        experiment_name=f"{table}_{time.strftime('%Y%m%d_%H%M%S')}",
        description=f"Importing data into {table} using {mode_name}",
        config={
            "table_name": table,
            "data_directory": str(directory),
            "clean_start": clean,
            "enable_pk_reset": enable_pk_reset,
            "db_user": "postgres"  # 可以从 config 导入更多
        }
    )

    if clean:
        logging.info(f"\n>>> 阶段 1: 清空表 '{table}'...")
        try:
            run_sql_command(f"DELETE FROM \"public\".\"{table}\";")
            logging.info("表已清空。")
        except Exception as e:
            logging.error(f"清空失败: {e}")
            sys.exit(1)
    else:
        logging.info(f"\n>>> 阶段 1: 跳过清空...")

    tbl_files = sorted(tbl_dir.glob("*.tbl"))
    if not tbl_files:
        logging.error("未找到 .tbl 文件。")
        sys.exit(1)
    logging.info(f"共 {len(tbl_files)} 个文件。")

    logging.info(f"\n>>> 阶段 2: 导入处理...")
    success, fail, total_copy_time = 0, 0, 0.0

    for i, fpath in enumerate(tbl_files, 1):
        logging.info(f"  -> ({i}/{len(tbl_files)}) {fpath.name}")
        p_start = time.time()

        # === 调用 Loader，获取结果和指标字典 ===
        res, metrics = import_single_file_with_lock(fpath, table, enable_pk_reset=enable_pk_reset)

        # 计算单文件总处理时间
        file_process_time = time.time() - p_start
        copy_t = metrics.get("time_copy", 0.0)

        # ==========================
        # SwanLab Logging
        # ==========================
        # 即使失败也记录部分耗时，status 标记为 0 或 1
        log_payload = {
            "Time/Total_Process": file_process_time,
            "Time/Drop_Index": metrics.get("time_drop_index", 0.0),
            "Time/Copy_Data": copy_t,
            "Time/Restore_Index": metrics.get("time_restore_index", 0.0),
            "Status": 1 if res.returncode == 0 else 0
        }

        # 如果开启了主键重置，记录该项时间
        if enable_pk_reset:
            log_payload["Time/Reset_PK"] = metrics.get("time_reset_pk", 0.0)

        # 记录到 SwanLab，step 设为当前文件序号
        swanlab.log(log_payload, step=i)

        if res.returncode == 0:
            success += 1
            total_copy_time += copy_t
            logging.info(f"     ✅ 成功 (Shell COPY: {copy_t:.2f}s | 全程: {file_process_time:.2f}s)")
        else:
            fail += 1
            logging.error(f"     ❌ 失败")

    logging.info(f"\n>>> 阶段 3: 统计 ({full_mode_desc})...")
    real_time = time.time() - start_time
    logging.info(f"总耗时: {real_time:.3f}s | 纯COPY耗时: {total_copy_time:.3f}s")

    try:
        res = run_command(build_psql_prefix() + f" -t -c 'SELECT count(1) FROM \"public\".\"{table}\";'", check=False,
                          capture_output=True)
        cnt = int(res.stdout.strip()) if res.stdout.strip().isdigit() else 0
        logging.info(f"最终行数: {cnt}")

        # 计算吞吐量
        throughput_total = int(cnt / real_time) if real_time > 0 else 0
        throughput_copy = int(cnt / total_copy_time) if total_copy_time > 0 else 0

        if cnt > 0:
            logging.info(f"平均吞吐量 (Total): {throughput_total} rows/s")
            if total_copy_time > 0:
                logging.info(f"纯COPY吞吐量 (Copy):  {throughput_copy} rows/s")

        # 记录最终汇总指标到 SwanLab
        swanlab.log({
            "Summary/Total_Rows": cnt,
            "Summary/Throughput_Global": throughput_total,
            "Summary/Throughput_PureCopy": throughput_copy,
            "Summary/Total_Duration_Sec": real_time
        })

    except Exception as e:
        logging.warning(f"统计失败: {e}")

    # 结束实验
    swanlab.finish()


# =========================================================
# 定义两个 CLI 入口点 (保持不变)
# =========================================================

@click.command()
@click.option('-f', '--table', required=True, help="目标表名")
@click.option('-d', '--directory', required=True, type=click.Path(exists=True, file_okay=False), help="数据目录")
@click.option('--clean/--no-clean', default=True, help="导入前清空表")
def cli_standard(table, directory, clean):
    """基础导入工具 (不重置主键)"""
    run_main_logic(table, directory, clean, enable_pk_reset=False)


@click.command()
@click.option('-f', '--table', required=True, help="目标表名")
@click.option('-d', '--directory', required=True, type=click.Path(exists=True, file_okay=False), help="数据目录")
@click.option('--clean/--no-clean', default=True, help="导入前清空表")
def cli_collatec(table, directory, clean):
    """加强版导入工具 (包含主键重置步骤)"""
    run_main_logic(table, directory, clean, enable_pk_reset=True)
