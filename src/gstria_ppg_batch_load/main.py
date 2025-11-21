#!/usr/bin/env python3
import sys
import time
import logging
import click
import re  # 新增: 用于提取分区号数字
import swanlab
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
            "mode": mode_name
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

        file_process_time = time.time() - p_start
        copy_t = metrics.get("time_copy", 0.0)
        partition_name = metrics.get("partition_name", "N/A")

        # ==========================
        # SwanLab Logging
        # ==========================

        # 基础耗时指标
        log_payload = {
            "Time/Total_Process": file_process_time,
            "Time/Drop_Index": metrics.get("time_drop_index", 0.0),
            "Time/Copy_Data": copy_t,
            "Time/Restore_Index": metrics.get("time_restore_index", 0.0),
            "Status": 1 if res.returncode == 0 else 0
        }

        # 记录主键重置时间（如果在该模式下）
        if enable_pk_reset:
            log_payload["Time/Reset_PK"] = metrics.get("time_reset_pk", 0.0)

        # === 新增: 记录分区表信息 ===

        # 1. 记录文本信息 (在 SwanLab 面板的 Media/Text 栏查看)
        # 去掉表名的双引号以便阅读
        clean_part_name = partition_name.replace('"', '')
        log_payload["Info/Partition_Name"] = swanlab.Text(clean_part_name, caption=f"File: {fpath.name}")

        # 2. 尝试提取数字后缀画折线图 (例如 performance_wa_005 -> 5)
        # 这会在图表中形成一个漂亮的阶梯状曲线，显示数据分布的切换点
        try:
            # 匹配字符串末尾的数字
            match = re.search(r'(\d+)$', clean_part_name)
            if match:
                part_idx = int(match.group(1))
                log_payload["Info/Partition_Index"] = part_idx
        except:
            pass

        # 发送日志
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

        throughput_total = int(cnt / real_time) if real_time > 0 else 0
        throughput_copy = int(cnt / total_copy_time) if total_copy_time > 0 else 0

        if cnt > 0:
            logging.info(f"平均吞吐量 (Total): {throughput_total} rows/s")
            if total_copy_time > 0:
                logging.info(f"纯COPY吞吐量 (Copy):  {throughput_copy} rows/s")

        # 记录汇总
        swanlab.log({
            "Summary/Total_Rows": cnt,
            "Summary/Throughput_Global": throughput_total,
            "Summary/Throughput_PureCopy": throughput_copy,
            "Summary/Total_Duration_Sec": real_time
        })

    except Exception as e:
        logging.warning(f"统计失败: {e}")

    swanlab.finish()


@click.command()
@click.option('-f', '--table', required=True, help="目标表名")
@click.option('-d', '--directory', required=True, type=click.Path(exists=True, file_okay=False), help="数据目录")
@click.option('--clean/--no-clean', default=True, help="导入前清空表")
def cli_standard(table, directory, clean):
    run_main_logic(table, directory, clean, enable_pk_reset=False)


@click.command()
@click.option('-f', '--table', required=True, help="目标表名")
@click.option('-d', '--directory', required=True, type=click.Path(exists=True, file_okay=False), help="数据目录")
@click.option('--clean/--no-clean', default=True, help="导入前清空表")
def cli_collatec(table, directory, clean):
    run_main_logic(table, directory, clean, enable_pk_reset=True)
