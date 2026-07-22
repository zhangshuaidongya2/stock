#!/bin/zsh

PROJECT_DIR="/Users/zxd/Projects/stock"
PYTHON="$PROJECT_DIR/.venv/bin/python"
SCRIPT="$PROJECT_DIR/flow/grap_flow_today.py"

MAX_ATTEMPTS=3
RETRY_DELAY=300

cd "$PROJECT_DIR" || exit 1

attempt=1
while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 开始执行，第 $attempt/$MAX_ATTEMPTS 次"

    if [ "$attempt" -eq 1 ]; then
        # 首次执行时清除当天旧文件，完整重新抓取。
        "$PYTHON" "$SCRIPT" --recreate
    else
        # 重试时保留已写入的数据，只抓取尚未完成的股票。
        "$PYTHON" "$SCRIPT"
    fi

    exit_code=$?
    if [ "$exit_code" -eq 0 ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] 执行成功"
        exit 0
    fi

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 第 $attempt 次执行失败，退出码：$exit_code"
    if [ "$attempt" -lt "$MAX_ATTEMPTS" ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] 保留已有数据，等待 $RETRY_DELAY 秒后继续抓取"
        sleep "$RETRY_DELAY"
    fi

    attempt=$((attempt + 1))
done

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 连续 $MAX_ATTEMPTS 次执行失败，请检查错误日志"
exit 1
