#!/usr/bin/env bash
# Z-score 漏斗 · Web 控制台启动器（云端/手机访问）
# 用法:
#   ./start_web.sh                  # 默认 0.0.0.0:8080
#   PORT=9000 ./start_web.sh        # 自定义端口
#   HOST=127.0.0.1 ./start_web.sh   # 仅本机访问（不加端口转发）
# 云端务必用 HOST=0.0.0.0（默认已如此），并在安全组放行端口。
set -u
cd "$(dirname "$0")"
export HOST="${HOST:-0.0.0.0}"
export PORT="${PORT:-8080}"
export OUT_DIR="${OUT_DIR:-/workspace/reports/sailing-daily}"
echo "▶ Web 控制台: http://${HOST}:${PORT}  (OUT_DIR=${OUT_DIR})"
exec python3.11 "$(dirname "$0")/web_app.py"
