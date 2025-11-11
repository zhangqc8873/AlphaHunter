#!/usr/bin/env bash
# 启动 AlphaHunter 的后台实时服务与（可选）Streamlit UI（Linux/Ubuntu）
set -euo pipefail

# 默认参数
PORT=8503
CACHE_DIR=.cache
NO_UI=false

# 参数解析
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--port)
      PORT="$2"; shift 2;;
    -c|--cache-dir)
      CACHE_DIR="$2"; shift 2;;
    -n|--no-ui)
      NO_UI=true; shift;;
    *)
      echo "未知参数: $1"; exit 1;;
  esac
done

# 目录解析
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

# Python 解析
PY="$(command -v python3 || command -v python || true)"
if [[ -z "$PY" ]]; then
  echo "未找到 python3/python，请先安装 Python 并在 PATH 中"; exit 1
fi

# 确保 src 布局可导入
export PYTHONPATH="$PROJECT_ROOT/src:${PYTHONPATH:-}"

# 路径
RT_DIR="$PROJECT_ROOT/$CACHE_DIR/realtime"
mkdir -p "$RT_DIR"
STATUS_FILE="$RT_DIR/service_status.json"
PID_FILE="$RT_DIR/service_pid.txt"

is_service_running() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || echo 0)"
    if [[ -n "$pid" && "$pid" -gt 0 && -d "/proc/$pid" ]]; then
      return 0
    fi
  fi
  return 1
}

# 启动后台实时服务
if is_service_running; then
  echo "实时服务已在运行中，跳过启动。"
else
  echo "正在后台启动实时服务..."
  (cd "$PROJECT_ROOT" && nohup "$PY" -m alphahunter.realtime_service \
    > "$LOG_DIR/realtime.out.log" 2> "$LOG_DIR/realtime.err.log" & disown)
fi

# 启动 UI（可选）
if [[ "$NO_UI" == "false" ]]; then
  port_in_use=false
  if command -v ss >/dev/null 2>&1; then
    if ss -ltn | awk '{print $4}' | grep -q ":$PORT$"; then port_in_use=true; fi
  elif command -v lsof >/dev/null 2>&1; then
    if lsof -iTCP -sTCP:LISTEN -P -n | grep -q ":$PORT"; then port_in_use=true; fi
  fi
  if [[ "$port_in_use" == "true" ]]; then
    echo "Streamlit UI 已在端口 $PORT 监听，跳过启动。"
  else
    echo "正在后台启动 Streamlit UI（端口 $PORT）..."
    (cd "$PROJECT_ROOT" && nohup "$PY" -m streamlit run src/alphahunter/ui_app.py --server.port "$PORT" \
      > "$LOG_DIR/ui.out.log" 2> "$LOG_DIR/ui.err.log" & disown)
  fi
fi

echo "完成。日志位于：$LOG_DIR"