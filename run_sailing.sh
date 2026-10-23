#!/usr/bin/env bash
# 全市场分层 Z-score 漏斗日报 · 自包含启动器
# 用法: ./run_sailing.sh [SCAN_SAMPLE=30] [ACCT_CAPITAL=1000000] [BJ_INCLUDE=false]
# 说明: cd 到本脚本所在目录后执行引擎；所有参数可经环境变量覆盖。
set -u
cd "$(dirname "$0")"
export SCAN_SAMPLE="${SCAN_SAMPLE:-${1:-30}}"
export ACCT_CAPITAL="${ACCT_CAPITAL:-${2:-1000000}}"
export BJ_INCLUDE="${BJ_INCLUDE:-${3:-false}}"
export OUT_DIR="${OUT_DIR:-/workspace/reports/sailing-daily}"
python3.11 "$(dirname "$0")/funnel_scan.py"
