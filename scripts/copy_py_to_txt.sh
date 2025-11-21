#!/bin/bash
set -euo pipefail

# ==================== 路径配置 ====================
# 获取脚本所在目录的绝对路径（确保执行目录不影响路径计算）
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# 项目根目录（scripts 文件夹的父目录）
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")
# 源文件根目录：./src
SRC_ROOT="${PROJECT_ROOT}/src"
# 目标文件根目录：./src_txt
DEST_ROOT="${PROJECT_ROOT}/src_txt"

# ==================== 前置检查 ====================
# 检查源根目录是否存在
if [ ! -d "${SRC_ROOT}" ]; then
    echo -e "\033[31m错误：源根目录 ${SRC_ROOT} 不存在！\033[0m"
    exit 1
fi

# ==================== 核心逻辑 ====================
echo -e "\033[32m开始执行：复制 ${SRC_ROOT} 下的 .py 文件到 ${DEST_ROOT}（后缀改为 .txt）\033[0m"

# 1. 遍历源目录下所有子目录，同步创建目标目录（先删后建，确保目录干净）
find "${SRC_ROOT}" -type d | while read -r src_dir; do
    # 计算当前源目录对应的目标目录路径
    dest_dir="${src_dir/${SRC_ROOT}/${DEST_ROOT}}"

    # 若目标目录已存在，先删除（避免残留旧文件）
    if [ -d "${dest_dir}" ]; then
        echo -e "  清理旧目录：${dest_dir}"
        rm -rf "${dest_dir}"
    fi

    # 重新创建目标目录（mkdir -p 自动创建多级父目录）
    echo -e "  创建目录：${dest_dir}"
    mkdir -p "${dest_dir}"
done

# 2. 遍历所有 .py 文件，复制并修改后缀为 .txt
find "${SRC_ROOT}" -type f -name "*.py" | while read -r src_file; do
    # 计算目标文件路径：替换根目录 + 改后缀
    dest_file="${src_file/${SRC_ROOT}/${DEST_ROOT}}"  # 替换根目录
    dest_file="${dest_file%.py}.txt"                 # 后缀 .py → .txt

    # 复制文件（-f 强制覆盖，避免交互提示）
    echo -e "  复制文件：${src_file} → ${dest_file}"
    cp -f "${src_file}" "${dest_file}"
done

# ==================== 执行完成 ====================
echo -e "\033[32m✅ 所有操作执行完成！\033[0m"
echo -e "📁 源目录：${SRC_ROOT}"
echo -e "📁 目标目录：${DEST_ROOT}"
echo -e "🔧 效果：所有 .py 文件已复制并转为 .txt 格式，目录结构完全同步"