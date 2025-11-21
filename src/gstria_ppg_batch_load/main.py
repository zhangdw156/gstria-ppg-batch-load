#!/usr/bin/env python3
import sys
import time
import logging
import click
import csv  # 新增
from pathlib import Path
from .config import setup_logging
from .utils import run_sql_command, run_command, build_psql_prefix
from .loader import import_single_file_with_lock


def run_main_logic(table, directory, clean, enable_pk_reset):
    """通用业务逻辑控制器"""
    setup_logging()

    # 定义日志和报告目录
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    current_date = time.strftime('%Y%m%d')

    # 定义 CSV 和 Summary 文件路径
    csv_file_path = log_dir / f"import_details_{current_date}.csv"
    summary_file_path = log_dir / f"import_summary_{current_date}.txt"

    tbl_dir = Path(directory)
    mode_name = "Collatec Mode (含主键重置)" if enable_pk_reset else "Standard Mode (基础模式)"

    logging.info("=" * 60)
    start_time = time.time()
    logging.info(f"开始数据导入流程 - {mode_name}")
    logging.info("=" * 60)

    # === 初始化 CSV 文件 ===
    # 写入表头
    csv_headers = [
        "File_Name", "Partition", "Status",
        "Idx_Drop_Time_s", "PK_Reset_Time_s",
        "Copy_Shell_Time_s", "Idx_Restore_Time_s",
        "Total_Process_Time_s"
    ]

    # 使用 'a' 模式追加，或者 'w' 覆盖，这里假设每次运行通过日期区分，如果是同日多次运行建议追加或加时间戳
    # 这里为了安全使用追加模式，但如果是新文件则写表头
    file_exists = csv_file_path.exists()
    csv_file = open(csv_file_path, mode='a', newline='', encoding='utf-8')
    csv_writer = csv.writer(csv_file)
    if not file_exists:
        csv_writer.writerow(csv_headers)

    if clean:
        logging.info(f"\n>>> 阶段 1: 清空表 '{table}'...")
        try:
            run_sql_command(f"DELETE FROM \"public\".\"{table}\";")
            logging.info("表已清空。")
        except Exception as e:
            logging.error(f"清空失败: {e}")
            csv_file.close()  # 异常退出前关闭文件
            sys.exit(1)
    else:
        logging.info(f"\n>>> 阶段 1: 跳过清空...")

    tbl_files = sorted(tbl_dir.glob("*.tbl"))
    if not tbl_files:
        logging.error("未找到 .tbl 文件。")
        csv_file.close()
        sys.exit(1)
    logging.info(f"共 {len(tbl_files)} 个文件。")

    logging.info(f"\n>>> 阶段 2: 导入处理...")
    success, fail, total_copy_time = 0, 0, 0.0

    try:
        for i, fpath in enumerate(tbl_files, 1):
            logging.info(f"  -> ({i}/{len(tbl_files)}) {fpath.name}")
            p_start = time.time()

            # === 调用 Loader，获取 result 和 stats 字典 ===
            res, stats = import_single_file_with_lock(fpath, table, enable_pk_reset=enable_pk_reset)

            process_time = time.time() - p_start
            copy_t = stats.get('t_copy', 0.0)

            # 准备 CSV 行数据
            row_status = "Success" if res.returncode == 0 else "Failed"
            csv_row = [
                fpath.name,
                stats.get('partition', 'N/A'),
                row_status,
                f"{stats.get('t_idx_drop', 0):.3f}",
                f"{stats.get('t_pk_reset', 0):.3f}",
                f"{copy_t:.3f}",
                f"{stats.get('t_idx_restore', 0):.3f}",
                f"{process_time:.3f}"
            ]

            # 写入 CSV
            csv_writer.writerow(csv_row)
            # 立即刷新缓冲区，防止程序崩溃数据丢失
            csv_file.flush()

            if res.returncode == 0:
                success += 1
                total_copy_time += copy_t
                logging.info(f"     ✅ 成功 (Shell COPY: {copy_t:.2f}s | 全程: {process_time:.2f}s)")
            else:
                fail += 1
                logging.error(f"     ❌ 失败")
    finally:
        # 确保文件被关闭
        csv_file.close()

    # === 阶段 3: 统计与报告文件生成 ===
    logging.info(f"\n>>> 阶段 3: 统计 ({mode_name})...")
    real_time = time.time() - start_time

    # 准备汇总信息的文本列表
    summary_lines = []
    summary_lines.append(f"Report Generated At: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    summary_lines.append(f"Mode: {mode_name}")
    summary_lines.append(f"Total Files: {len(tbl_files)} (Success: {success}, Failed: {fail})")
    summary_lines.append("-" * 40)

    log_msg_1 = f"总耗时: {real_time:.3f}s | 纯COPY耗时: {total_copy_time:.3f}s"
    logging.info(log_msg_1)
    summary_lines.append(log_msg_1)

    try:
        res = run_command(build_psql_prefix() + f" -t -c 'SELECT count(1) FROM \"public\".\"{table}\";'", check=False,
                          capture_output=True)
        cnt = int(res.stdout.strip()) if res.stdout.strip().isdigit() else 0

        log_msg_2 = f"最终行数: {cnt}"
        logging.info(log_msg_2)
        summary_lines.append(log_msg_2)

        if cnt > 0:
            log_msg_3 = f"平均吞吐量 (Total): {int(cnt / real_time)} rows/s"
            logging.info(log_msg_3)
            summary_lines.append(log_msg_3)

            if total_copy_time > 0:
                log_msg_4 = f"纯COPY吞吐量 (Copy):  {int(cnt / total_copy_time)} rows/s"
                logging.info(log_msg_4)
                summary_lines.append(log_msg_4)
    except Exception as e:
        err_msg = f"统计失败: {e}"
        logging.warning(err_msg)
        summary_lines.append(err_msg)

    # === 写入汇总文件 ===
    try:
        with open(summary_file_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(summary_lines))
        logging.info(f"汇总报告已生成: {summary_file_path}")
        logging.info(f"详细CSV已生成: {csv_file_path}")
    except Exception as e:
        logging.error(f"写入汇总文件失败: {e}")


# =========================================================
# CLI 入口点 (保持不变)
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