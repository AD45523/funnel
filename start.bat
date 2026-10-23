@echo off
chcp 65001 >nul 2>&1
REM ============================================================
REM  全市场分层 Z-score 漏斗 · Windows 一键启动脚本
REM  双击本文件即可启动 Web 控制台（手机/浏览器可访问）
REM  注意：本窗口是服务器，运行期间不要关闭；Ctrl+C 可停止
REM ============================================================
SETLOCAL

REM ---- 自动定位到本脚本所在目录（放在 scripts/ 里即可）----
SET "BASE=%~dp0"
CD /D "%BASE%"

REM ---- 监听地址与端口：云端/手机访问务必用 0.0.0.0 ----
SET "HOST=0.0.0.0"
SET "PORT=8080"

REM ---- 报告输出目录（放在脚本旁边，自动创建）----
SET "OUT_DIR=%BASE%reports\sailing-daily"

REM ---- 优先用 python，找不到再试 python3 ----
WHERE python >nul 2>nul
IF %ERRORLEVEL%==0 (
    SET "PY=python"
) ELSE (
    SET "PY=python3"
)

ECHO ===================================================
ECHO  启动 Z-score 漏斗 Web 控制台
ECHO  监听:  http://%HOST%:%PORT%
ECHO  本机:  http://127.0.0.1:%PORT%
ECHO  报告:  %OUT_DIR%
ECHO  提示:  手机连同一 WiFi，用电脑局域网IP访问
ECHO         例如 http://192.168.1.x:8080
ECHO  停止:  按 Ctrl+C 或关闭本窗口
ECHO ===================================================
ECHO.

%PY% web_app.py
IF %ERRORLEVEL% NEQ 0 (
    ECHO.
    ECHO [错误] 启动失败。请确认已安装 Python 3.11 并执行过：
    ECHO        pip install akshare flask
    ECHO.
)
PAUSE
