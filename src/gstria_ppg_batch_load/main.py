#!/usr/bin/env python3
import sys
import time
import logging
import click
from pathlib import Path
from .config import setup_logging
from .utils import run_sql_command, run_command, build_psql_prefix
from .loader import import_single_file_with_lock


def run_main_logic(table, directory, clean, enable_pk_reset):
    """通用业务逻辑控制器"""
    setup_logging()
    tbl_dir = Path(directory)
    mode_name = "Collatec Mode (含主键重置)" if enable_pk_reset else "Standard Mode (基础模式)"

    logging.info("=" * 60)
    start_time = time.time()
    logging.info(f"开始数据导入流程 - {mode_name}")
    logging.info("=" * 60)

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

        # === 调用 Loader，传入差异化参数 ===
        res, copy_t = import_single_file_with_lock(fpath, table, enable_pk_reset=enable_pk_reset)

        if res.returncode == 0:
            success += 1
            total_copy_time += copy_t
            logging.info(f"     ✅ 成功 (Shell COPY: {copy_t:.2f}s | 全程: {time.time() - p_start:.2f}s)")
        else:
            fail += 1
            logging.error(f"     ❌ 失败")

    logging.info(f"\n>>> 阶段 3: 统计 ({mode_name})...")
    real_time = time.time() - start_time
    logging.info(f"总耗时: {real_time:.3f}s | 纯COPY耗时: {total_copy_time:.3f}s")

    try:
        res = run_command(build_psql_prefix() + f" -t -c 'SELECT count(1) FROM \"public\".\"{table}\";'", check=False,
                          capture_output=True)
        cnt = int(res.stdout.strip()) if res.stdout.strip().isdigit() else 0
        logging.info(f"最终行数: {cnt}")
        if cnt > 0:
            logging.info(f"平均吞吐量 (Total): {int(cnt / real_time)} rows/s")
            if total_copy_time > 0:
                logging.info(f"纯COPY吞吐量 (Copy):  {int(cnt / total_copy_time)} rows/s")
    except Exception as e:
        logging.warning(f"统计失败: {e}")


# =========================================================
# 定义两个 CLI 入口点
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
