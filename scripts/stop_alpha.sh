#!/usr/bin/env bash
# 优雅停止 AlphaHunter（Linux/Ubuntu）：写入 control.json 请求停止，并释放 UI 端口
set -euo pipefail

PORT=8503
CACHE_DIR=.cache
TIMEOUT=30
FORCE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--port) PORT="$2"; shift 2;;
    -c|--cache-dir) CACHE_DIR="$2"; shift 2;;
    -t|--timeout) TIMEOUT="$2"; shift 2;;
    -f|--force) FORCE=true; shift;;
    *) echo "未知参数: $1"; exit 1;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RT_DIR="$PROJECT_ROOT/$CACHE_DIR/realtime"
CONTROL_FILE="$RT_DIR/control.json"
STATUS_FILE="$RT_DIR/service_status.json"
PID_FILE="$RT_DIR/service_pid.txt"

mkdir -p "$RT_DIR"

# 请求停止
printf '{"stop": true, "paused": false}\n' > "$CONTROL_FILE"
echo "已写入停止请求：$CONTROL_FILE"

# 等待服务停止（基于 PID 是否存活）
elapsed=0
pid=0
if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || echo 0)"
fi

while [[ $elapsed -lt $TIMEOUT ]]; do
  if [[ -n "$pid" && "$pid" -gt 0 && -d "/proc/$pid" ]]; then
    sleep 1
    elapsed=$((elapsed+1))
  else
    break
  fi
done

if [[ $elapsed -ge $TIMEOUT && "$FORCE" == "true" ]]; then
  if [[ -n "$pid" && "$pid" -gt 0 && -d "/proc/$pid" ]]; then
    echo "优雅停止超时，强制结束服务进程 $pid"
    kill -9 "$pid" || true
  fi
fi

# 释放 UI 端口
release_port() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    local pids
    pids=$(lsof -tiTCP:"$port" -sTCP:LISTEN || true)
    if [[ -n "$pids" ]]; then
      echo "释放端口 $port 的监听进程: $pids"
      kill $pids || true
    else
      echo "端口 $port 未被占用。"
    fi
  elif command -v fuser >/dev/null 2>&1; then
    fuser -k "$port"/tcp || true
  else
    echo "未找到 lsof/fuser，跳过端口释放。"
  fi
}

release_port "$PORT"
echo "停止流程完成。"