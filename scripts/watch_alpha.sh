#!/usr/bin/env bash
# 看门狗（Linux/Ubuntu）：监控实时服务，异常退出时自动重启
set -euo pipefail

CACHE_DIR=.cache
CHECK_INTERVAL=5

while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--cache-dir) CACHE_DIR="$2"; shift 2;;
    -i|--interval) CHECK_INTERVAL="$2"; shift 2;;
    *) echo "未知参数: $1"; exit 1;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
WATCH_LOG="$LOG_DIR/watchdog.log"

PY="$(command -v python3 || command -v python || true)"
if [[ -z "$PY" ]]; then
  echo "未找到 python3/python，请先安装并加入 PATH"; exit 1
fi
export PYTHONPATH="$PROJECT_ROOT/src:${PYTHONPATH:-}"

RT_DIR="$PROJECT_ROOT/$CACHE_DIR/realtime"
STATUS_FILE="$RT_DIR/service_status.json"
PID_FILE="$RT_DIR/service_pid.txt"
RT_OUT="$LOG_DIR/realtime.out.log"
RT_ERR="$LOG_DIR/realtime.err.log"

is_alive() {
  local pid="$1"
  [[ -n "$pid" && "$pid" -gt 0 && -d "/proc/$pid" ]]
}

start_service() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] watchdog: 启动实时服务" >> "$WATCH_LOG"
  (cd "$PROJECT_ROOT" && nohup "$PY" -m alphahunter.realtime_service \
    > "$RT_OUT" 2> "$RT_ERR" & disown)
}

echo "看门狗已启动（每 ${CHECK_INTERVAL}s 检查一次）。Ctrl+C 可退出。"
while true; do
  stop_req=false
  if [[ -f "$STATUS_FILE" ]]; then
    if grep -q '"stop_requested"\s*:\s*true' "$STATUS_FILE"; then
      stop_req=true
    fi
  fi

  if [[ "$stop_req" == "true" ]]; then
    echo "检测到 stop_requested，看门狗退出。"; break
  fi

  pid=0
  if [[ -f "$PID_FILE" ]]; then
    pid="$(cat "$PID_FILE" 2>/dev/null || echo 0)"
  fi

  running=false
  if [[ -f "$STATUS_FILE" ]]; then
    if grep -q '"running"\s*:\s*true' "$STATUS_FILE"; then
      running=true
    fi
  fi

  if ! is_alive "$pid" || [[ "$running" != "true" ]]; then
    start_service
  fi

  sleep "$CHECK_INTERVAL"
done