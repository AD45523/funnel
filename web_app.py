#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全市场分层 Z-score 漏斗 · Web 控制台
==================================================================
把 funnel_scan.py 包装成一个可联网访问的 Web 服务：
  - 手机/电脑浏览器打开即可触发扫描、查看实时日志、阅读九节报告
  - 默认绑定 0.0.0.0（所有网卡），方便云端/局域网/手机访问
  - 扫描在进程内后台线程跑，前端轮询状态，不阻塞界面

启动:
    python3.11 web_app.py                 # 默认 http://0.0.0.0:8080
    PORT=9000 HOST=127.0.0.1 python3.11 web_app.py

环境变量:
    PORT       监听端口，默认 8080
    HOST       监听地址，默认 0.0.0.0（云端/手机访问务必用 0.0.0.0）
    OUT_DIR    报告目录，默认 /workspace/reports/sailing-daily
==================================================================
"""
import os
import sys
import json
import time
import subprocess
import threading
from pathlib import Path

from flask import (
    Flask, request, jsonify, send_file,
    render_template_string, abort,
)
import markdown

# ---------------- 路径与配置 ----------------
BASE = Path(__file__).resolve().parent
ENGINE = BASE / "funnel_scan.py"
PY = sys.executable

OUT_DIR = Path(os.environ.get("OUT_DIR", "/workspace/reports/sailing-daily"))
OUT_DIR.mkdir(parents=True, exist_ok=True)

PORT = int(os.environ.get("PORT", "8080"))
HOST = os.environ.get("HOST", "0.0.0.0")

RUN_LOG = OUT_DIR / "web_run.log"          # 每次扫描的实时日志
CONFIG_FILE = BASE / "web_config.json"     # 持久化配置

DEFAULT_CONFIG = {
    "SCAN_SAMPLE": "30",
    "ACCT_CAPITAL": "1000000",
    "BJ_INCLUDE": "false",
    "AUTO_RUN_ON_START": "false",
}


def load_config():
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_FILE.exists():
        try:
            cfg.update(json.loads(CONFIG_FILE.read_text(encoding="utf-8")))
        except Exception:
            pass
    return cfg


def save_config(cfg):
    CONFIG_FILE.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


CONFIG = load_config()

# ---------------- 运行时状态（进程内） ----------------
state = {
    "running": False,
    "pid": None,
    "start_ts": None,
    "end_ts": None,
    "exit_code": None,
    "last_report": None,
}
_proc = None
_lock = threading.Lock()


def latest_reports(n=50):
    files = sorted(OUT_DIR.glob("sailing_daily_*.md"), reverse=True)
    return [f.name for f in files[:n]]


def find_report_path(name):
    if name in (None, "", "latest"):
        files = sorted(OUT_DIR.glob("sailing_daily_*.md"), reverse=True)
        return files[0] if files else None
    p = OUT_DIR / name
    return p if p.exists() else None


def write_run_log(line):
    try:
        with open(RUN_LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def tail_run_log(lines=200):
    if not RUN_LOG.exists():
        return ""
    try:
        with open(RUN_LOG, "r", encoding="utf-8") as f:
            data = f.readlines()
        return "".join(data[-lines:])
    except Exception:
        return ""


def run_scan_thread(cfg):
    """在子进程跑引擎，实时把 stdout/stderr 写入 web_run.log。"""
    global _proc, state
    env = dict(os.environ)
    env["SCAN_SAMPLE"] = str(cfg.get("SCAN_SAMPLE", "30"))
    env["ACCT_CAPITAL"] = str(cfg.get("ACCT_CAPITAL", "1000000"))
    env["BJ_INCLUDE"] = str(cfg.get("BJ_INCLUDE", "false"))
    env["OUT_DIR"] = str(OUT_DIR)

    # 清空上一轮日志
    try:
        RUN_LOG.write_text("", encoding="utf-8")
    except Exception:
        pass

    with _lock:
        state["running"] = True
        state["start_ts"] = time.time()
        state["end_ts"] = None
        state["exit_code"] = None
        state["last_report"] = None

    write_run_log(f"[WEB] 启动扫描 {time.strftime('%Y-%m-%d %H:%M:%S')} config={cfg}")

    try:
        _proc = subprocess.Popen(
            [PY, str(ENGINE)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(BASE),
            bufsize=1,
            universal_newlines=True,
        )
        with _lock:
            state["pid"] = _proc.pid
        for line in _proc.stdout:
            line = line.rstrip("\n")
            if line:
                write_run_log(line)
        _proc.stdout.close()
        rc = _proc.wait()
    except Exception as e:
        write_run_log(f"[WEB] 启动失败: {e}")
        rc = -1

    # 完成后定位最新报告
    rep = find_report_path("latest")
    with _lock:
        state["running"] = False
        state["end_ts"] = time.time()
        state["exit_code"] = rc
        state["last_report"] = rep.name if rep else None
        state["pid"] = None
    write_run_log(f"[WEB] 扫描结束 exit={rc} report={rep.name if rep else '无'}")


def trigger_run(cfg=None):
    global _proc
    with _lock:
        if state["running"]:
            return False, "已有扫描在运行，请等待完成"
    if cfg:
        save_config(cfg)
        CONFIG.update(cfg)
    t = threading.Thread(target=run_scan_thread, args=(CONFIG,), daemon=True)
    t.start()
    return True, "已启动"


# ---------------- Flask App ----------------
app = Flask(__name__)

PAGE = """
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover"/>
<title>Z-score 漏斗控制台</title>
<style>
  :root{--bg:#0f1115;--card:#1a1d24;--fg:#e6e8eb;--mut:#9aa3ad;--acc:#3b82f6;--ok:#22c55e;--warn:#f59e0b;--bad:#ef4444;}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif;}
  .wrap{max-width:780px;margin:0 auto;padding:16px;}
  h1{font-size:20px;margin:8px 0 4px}
  .sub{color:var(--mut);font-size:13px;margin-bottom:14px}
  .card{background:var(--card);border-radius:12px;padding:14px;margin-bottom:14px;box-shadow:0 1px 3px rgba(0,0,0,.3)}
  label{display:block;font-size:13px;color:var(--mut);margin:8px 0 4px}
  input,select{width:100%;padding:10px;border-radius:8px;border:1px solid #2a2f38;background:#0f1115;color:var(--fg);font-size:15px}
  .row{display:flex;gap:10px}
  .row>div{flex:1}
  button{width:100%;padding:12px;border:0;border-radius:10px;background:var(--acc);color:#fff;font-size:16px;font-weight:600;margin-top:10px;cursor:pointer}
  button:disabled{opacity:.5}
  button.ghost{background:#2a2f38}
  .status{font-size:14px;margin:6px 0}
  .badge{display:inline-block;padding:3px 10px;border-radius:999px;font-size:12px;font-weight:600}
  .b-run{background:rgba(245,158,11,.18);color:var(--warn)}
  .b-idle{background:rgba(34,197,94,.18);color:var(--ok)}
  pre{background:#0b0d10;border-radius:8px;padding:12px;overflow:auto;max-height:300px;font:12px/1.45 ui-monospace,Menlo,Consolas,monospace;white-space:pre-wrap;word-break:break-all}
  .reports a{color:var(--acc);text-decoration:none;display:block;padding:8px 0;border-bottom:1px solid #23272f}
  table{border-collapse:collapse;width:100%;font-size:13px}
  th,td{border:1px solid #2a2f38;padding:6px 8px;text-align:left}
  th{background:#0b0d10}
  .help{color:var(--mut);font-size:12px}
  .big{font-size:22px;font-weight:700}
</style>
</head>
<body>
<div class="wrap">
  <h1>🧭 全市场分层 Z-score 漏斗</h1>
  <div class="sub">Web 控制台 · 手机/云端可访问 · 数据缺失自动标 UNKNOWN，绝不自动下单</div>

  <div class="card">
    <div class="status">状态：<span id="badge" class="badge b-idle">空闲</span>
      <span id="meta" class="help"></span></div>
  </div>

  <div class="card">
    <form id="cfg">
      <div class="row">
        <div><label>抽样数 SCAN_SAMPLE（默认30，全市场扫请调大）</label>
          <input name="SCAN_SAMPLE" id="SCAN_SAMPLE" value="{{cfg.SCAN_SAMPLE}}"/></div>
        <div><label>账户资金(元) ACCT_CAPITAL</label>
          <input name="ACCT_CAPITAL" id="ACCT_CAPITAL" value="{{cfg.ACCT_CAPITAL}}"/></div>
      </div>
      <div class="row">
        <div><label>纳入北交所 BJ_INCLUDE</label>
          <select name="BJ_INCLUDE" id="BJ_INCLUDE">
            <option value="false" {{'selected' if cfg.BJ_INCLUDE=='false' else ''}}>否</option>
            <option value="true" {{'selected' if cfg.BJ_INCLUDE=='true' else ''}}>是</option>
          </select></div>
        <div><label>&nbsp;</label>
          <button type="button" id="runBtn">▶ 运行扫描</button></div>
      </div>
    </form>
  </div>

  <div class="card">
    <div class="status">实时日志</div>
    <pre id="log">（等待运行…）</pre>
  </div>

  <div class="card">
    <div class="status">历史报告</div>
    <div class="reports" id="reports">
      {% for r in reports %}<a href="/report?file={{r}}">{{r}}</a>{% endfor %}
      {% if not reports %}<span class="help">暂无报告</span>{% endif %}
    </div>
  </div>

  <div class="card help">
    <b>手机云端怎么跑：</b><br/>
    1. 把本目录部署到任意云主机/容器（轻量云、Railway、Render、Fly.io 等）。<br/>
    2. 启动时务必 <code>HOST=0.0.0.0</code>，云端平台若提供公网 URL 直接用；若是裸云主机，需在安全组/防火墙放行该端口。<br/>
    3. 手机浏览器打开 <code>http://&lt;公网IP或域名&gt;:端口</code> 即可触发扫描、看日志、读报告。<br/>
    4. 若平台只暴露 80/443，可用反向代理（Nginx/Caddy）把端口转过去，或设 <code>PORT=80</code>。
  </div>
</div>

<script>
const $=(s)=>document.querySelector(s);
async function refresh(){
  const r=await fetch('/api/status');const s=await r.json();
  const b=$('#badge');
  if(s.running){b.className='badge b-run';b.textContent='运行中';}
  else{b.className='badge b-idle';b.textContent='空闲';}
  $('#meta').textContent=s.meta||'';
  $('#log').textContent=s.log||'';
  if(s.reports){const box=$('#reports');box.innerHTML=s.reports.map(x=>`<a href="/report?file=${x}">${x}</a>`).join('')||'<span class="help">暂无报告</span>';}
}
$('#runBtn').onclick=async()=>{
  const cfg={SCAN_SAMPLE:$('#SCAN_SAMPLE').value,ACCT_CAPITAL:$('#ACCT_CAPITAL').value,BJ_INCLUDE:$('#BJ_INCLUDE').value};
  const r=await fetch('/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(cfg)});
  const j=await r.json();alert(j.msg);refresh();
};
refresh();setInterval(refresh,3000);
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(
        PAGE, cfg=CONFIG, reports=latest_reports()
    )


@app.route("/run", methods=["POST"])
def run():
    try:
        data = request.get_json(force=True) or {}
    except Exception:
        data = {}
    ok, msg = trigger_run(data if data else None)
    return jsonify({"ok": ok, "msg": msg})


@app.route("/api/status")
def status():
    with _lock:
        running = state["running"]
        start = state["start_ts"]
        end = state["end_ts"]
        exit_code = state["exit_code"]
        last = state["last_report"]
    meta = ""
    if running and start:
        meta = f"已运行 {int(time.time()-start)}s"
    elif end and start:
        meta = f"用时 {int(end-start)}s · exit={exit_code} · 报告={last or '无'}"
    return jsonify({
        "running": running,
        "meta": meta,
        "log": tail_run_log(300),
        "reports": latest_reports(50),
        "config": CONFIG,
    })


@app.route("/report")
def report_view():
    name = request.args.get("file", "latest")
    p = find_report_path(name)
    if not p:
        return f"<p style='color:#aaa;padding:20px'>未找到报告：{name}</p>"
    text = p.read_text(encoding="utf-8")
    html = markdown.markdown(
        text, extensions=["tables", "fenced_code", "toc"]
    )
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{p.name}</title>
<style>
body{{background:#0f1115;color:#e6e8eb;font:15px/1.7 -apple-system,"PingFang SC","Microsoft YaHei",sans-serif;max-width:860px;margin:0 auto;padding:16px}}
h1,h2,h3{{color:#fff;border-bottom:1px solid #2a2f38;padding-bottom:4px}}
table{{border-collapse:collapse;width:100%;font-size:13px;margin:10px 0}}
th,td{{border:1px solid #2a2f38;padding:6px 8px}}
th{{background:#1a1d24}}
code{{background:#1a1d24;padding:2px 5px;border-radius:4px}}
pre{{background:#0b0d10;padding:12px;border-radius:8px;overflow:auto}}
a{{color:#3b82f6}}
.back{{position:fixed;top:10px;right:10px;background:#3b82f6;color:#fff;padding:8px 14px;border-radius:8px;text-decoration:none}}
</style></head><body>
<a class="back" href="/">← 控制台</a>
{html}
</body></html>"""


@app.route("/download/<path:name>")
def download(name):
    p = OUT_DIR / name
    if not p.exists():
        abort(404)
    return send_file(str(p), as_attachment=True)


def maybe_auto_run():
    if str(CONFIG.get("AUTO_RUN_ON_START", "false")).lower() in ("1", "true", "yes"):
        trigger_run()


if __name__ == "__main__":
    print(f"[WEB] 监听 http://{HOST}:{PORT}  OUT_DIR={OUT_DIR}")
    if not ENGINE.exists():
        print(f"[WEB][WARN] 引擎文件不存在: {ENGINE}")
    threading.Thread(target=maybe_auto_run, daemon=True).start()
    app.run(host=HOST, port=PORT, threaded=True, debug=False)
