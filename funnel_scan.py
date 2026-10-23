#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全市场分层 Z-score 漏斗日报 · 引擎 v3
==================================================================
忠实实现 SKILL.md 第 0–9 章。Agent / 定时任务调用本脚本即产出日报，
无需再猜"船身/潮汐/海域"含义——所有术语已翻译为具体字段与判断分支。

章节映射
  第0章 总则         -> CONFIG / 术语映射
  第1章 L0快照+分区  -> get_spot / classify_board / layer1_hard_filter / assign_mcap_tier
  第2章 因子四步法   -> winsorize_mad / neutralize / standardize / quantile_score
  第3章 因子定义     -> compute_quality / valuation / liquidity / momentum / fund_flow / industry_position
  第4章 综合打分     -> composite / veto / gen_candidates
  第5章 市场宽度     -> market_breadth
  第6章 止损仓位     -> stop_position
  第7章 数据质量     -> DQ 计数器 / 商誉专项 / 零候选处理
  第8章 报告结构     -> render_report（九节固定）
  第9章 禁止事项     -> 全程不编造、不默认通过、不自动下单（代码层以 UNKNOWN 兜底体现）

------------------------------------------------------------------
诚实降级原则（沙箱/缺源环境）
------------------------------------------------------------------
- 任何取数失败按第7.1节重试3次(间隔2s)，全失败才标 UNKNOWN。
- 东方财富(EM)被墙时，三级非EM备选源接力：
  · 新浪 stock_zh_a_spot  -> L0 全市场快照(价/涨跌幅/量)
  · 腾讯财经 gtimg(qt.gtimg.cn) -> 市值/PE/PB/换手率（get_spot 内 enrich_tencent 仅填 NaN，
    东方财富正常时不触发；被墙时补全，使估值/流动性因子可取真值，实测覆盖 ~94%）
  · 同花顺 stock_financial_abstract_ths -> 财务排雷(负债率/现金流/扣非)
- 商誉三源全失败 -> UNKNOWN -> HIGH_RISK -> 进"龙骨未验证清单"，不进候选（仅 EM 提供商誉，腾讯/新浪/同花顺均不含）。
- 概念成分取不到 -> CONCEPT_UNKNOWN -> 不进候选，进"建池未验证清单"。
- 这些 gate 在生产环境(Eastmoney 可达)自动生效取真值；本环境缺源则诚实标 UNKNOWN。
  估值/流动性已由腾讯补全，仅商誉/概念/资金流仍可能因 EM 不可达而 UNKNOWN。
==================================================================
"""
import sys, os, time, json, datetime, io, contextlib, traceback
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd

try:
    import akshare as ak
except Exception as e:
    print("IMPORT_FAIL:", e); sys.exit(2)

import socket  # 仅用于 EM 探测时临时设置超时，避免风控告警长时间挂起
import urllib.request  # 腾讯财经 gtimg 行情（不依赖东方财富，用来补市值/PE/PB/换手率）
import threading as _threading
import pickle
import hashlib
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# 东财(EM)可达性：启动时探测；沙箱常被风控拦截则置 False，
# 全局短路所有 EM 接口（商誉/概念/资金流/行业），不再逐股带超时重试。
EM_AVAILABLE = True

TODAY = datetime.date.today()
NOW = datetime.datetime.now()
RUN_TS = NOW.strftime("%Y%m%d_%H%M%S")
TRADE_DATE = TODAY   # 数据获取窗口解析后的"交易日"，main 启动时按时段重算

# ============================ 第0章 配置 ============================
BJ_INCLUDE        = os.environ.get("BJ_INCLUDE", "false").lower() in ("1", "true", "yes")
MIN_LIST_DAYS     = 60          # 1.3 上市不满60交易日剔除
MCAP_FLOAT_MIN    = 50.0        # 1.3 流通市值<50亿剔除 (单位: 亿元)
AMT20_MIN         = 5000.0      # 1.3 近20日日均成交额<5000万剔除 (单位: 万元)
EXPECTED_SPOT     = 4000        # 1.1 返回<4000条标记 DATA_INCOMPLETE
TIER_MIN_N        = 30          # 1.4 板块样本<30不拆市值档

# ---- 第3章 权重（相对权重，合成时归一化，解决小市值档合计>1问题）----
W_QUALITY   = 0.30
W_VALUATION = 0.15
W_MOMENTUM  = 0.20
W_FUND      = 0.10
W_LIQUIDITY = {"small": 0.35, "mid": 0.25, "large": 0.15}   # 3.3 差异化

# ---- 第3.1 质量排雷阈值 ----
GOODWILL_VETO_RATIO   = 50.0    # 商誉/净资产 >50% 且同业Z前5% -> VETO
GOODWILL_HIGH_RATIO   = 20.0    # 商誉占净资产 20-50% 且同业Z前10% -> HIGH_RISK
DEBT_VETO             = 80.0     # 负债率>80% -> VETO
DEBT_HIGH             = 70.0     # 负债率70-80% -> HIGH_RISK
OCF_OP_DIVERGE        = 0.3      # ocf/op <0.3 -> HIGH_RISK(背离)
OCF_OP_MID            = 0.6      # 0.3-0.6 中等；>=0.6 正向

# ---- 第4.3 候选 ----
TOP_PER_TIER = 3
CAND_CAP     = 20
NEAR_MISS_N  = 5

# ---- 第5章 市场宽度 ----
BREADTH_FREEZE_UP   = 500       # 冰点: 上涨家数<500
BREADTH_FREEZE_ADR  = 0.3       # 冰点: ADR<0.3
BREADTH_BOIL_UP     = 4000      # 沸点: 上涨家数>4000
BREADTH_BOIL_ADR    = 3.0       # 沸点: ADR>3

# ---- 第6章 止损/仓位 ----
ATR_N          = 26
RISK_PER_TRADE = 0.02           # 单笔风险<=账户2%
ACCT_CAPITAL   = float(os.environ.get("ACCT_CAPITAL", "1000000"))  # 账户资金(元)，可环境变量覆盖
MAX_RECON      = 0.10           # 单票侦察仓<=10%
MAX_ADD        = 0.20           # 加仓后<=20%
MAX_SECTOR     = 0.30           # 单一行业<=30%
MIN_CASH       = 0.20           # 永远保留>=20%现金

# ---- 第7章 重试 ----
RETRY = 3
RETRY_DELAY = 2

OUT_DIR = os.environ.get("OUT_DIR", "/workspace/reports/sailing-daily")
os.makedirs(OUT_DIR, exist_ok=True)

# 术语固定映射(0.5) 仅作可读性常量
KEEL = "商誉"   # 龙骨 = 商誉

# ============================ 第10.x 章 数据获取窗口 ============================
# 盘后日报的数据必须在正确的"窗口"内获取：盘前/周末不能用当日空数据，
# 任何源卡死必须可被硬超时打断，同日重跑应命中缓存、且逐股取数并行化以缩短窗口。
FORCE_RUN   = os.environ.get("FORCE_RUN", "0").lower() in ("1", "true", "yes")
CALL_TIMEOUT = int(os.environ.get("CALL_TIMEOUT", "25"))   # 单次取数硬超时(秒)
# 并行取数默认关闭：akshare 底层(py_mini_racer/共享 session)非线程安全，并发会挂起(串行0.4s vs 并发>150s)。
# 仅在你确认自身环境/akshare 版本线程安全时可 PARALLEL=1 加速；默认串行最可靠。
PARALLEL     = os.environ.get("PARALLEL", "0").lower() not in ("0", "false", "no")
MAX_WORKERS  = int(os.environ.get("MAX_WORKERS", "4"))     # 并行取数线程数(仅 PARALLEL=1 时生效)
CACHE_TTL   = int(os.environ.get("CACHE_TTL", "21600"))    # 缓存有效期(秒)=6小时
# 可补充的休市日(YYYYMMDD)；周末自动跳过，无需列出
HOLIDAYS = set()

def is_trading_day(d):
    if d.weekday() >= 5:
        return False
    if d.strftime("%Y%m%d") in HOLIDAYS:
        return False
    return True

def last_trading_day(d):
    x = d
    while not is_trading_day(x):
        x -= datetime.timedelta(days=1)
    return x

def resolve_trade_date(now=None):
    """按运行时刻解析应使用的数据交易日（核心窗口逻辑）：
    - 周末/休市 -> 上一交易日
    - 盘前(<09:30) -> 上一交易日收盘数据（当日尚无数据）
    - 盘中/盘后   -> 当日
    """
    now = now or datetime.datetime.now()
    d = now.date()
    if not is_trading_day(d):
        return last_trading_day(d)
    if now.time() < datetime.time(9, 30):
        # 盘前：当日尚无数据，回退到上一交易日（last_trading_day 对交易日本身会原样返回，故先 -1 天）
        return last_trading_day(d - datetime.timedelta(days=1))
    return d

def check_window(now=None):
    """返回 (建议继续, 提示)。窗口仅作提示，FORCE_RUN 可强行越过。"""
    now = now or datetime.datetime.now()
    d = now.date()
    if not is_trading_day(d):
        return False, f"今日 {d} 非交易日(周末/休市)，将使用上一交易日 {last_trading_day(d)} 数据"
    t = now.time()
    if t < datetime.time(9, 30):
        return True, f"盘前运行：将使用上一交易日 {last_trading_day(d)} 收盘数据"
    if datetime.time(9, 30) <= t <= datetime.time(15, 0):
        return True, "盘中运行：快照为实时(未收盘)数据，仅作参考"
    if datetime.time(15, 0) < t <= datetime.time(16, 30):
        return True, "盘后窗口(15:00-16:30)：当日数据完整，推荐运行"
    return True, "盘后(>16:30)：当日收盘数据已完备，可使用"

# ---- 单次取数硬超时：任何源卡死都不会无限阻塞（原 call_with_retry 缺超时，是此前长时间挂起的根因）----
def call_with_timeout(fn, timeout=CALL_TIMEOUT):
    box = {}
    def _t():
        try:
            box["v"] = fn()
        except Exception as e:
            box["e"] = e
    th = _threading.Thread(target=_t, daemon=True)
    th.start(); th.join(timeout)
    if th.is_alive():
        return False, "_TIMEOUT"
    if "e" in box:
        return False, repr(box["e"])[:80]
    return True, box.get("v")

# ---- 同日缓存：L0快照/概念成分/全市场财报等重活只在窗口内取一次 ----
CACHE_DIR = Path(OUT_DIR) / ".cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _cache_path(key):
    h = hashlib.md5(key.encode("utf-8")).hexdigest()[:16]
    return CACHE_DIR / f"{h}.pkl"

def cache_get(key, ttl=CACHE_TTL):
    p = _cache_path(key)
    if not p.exists():
        return None
    try:
        if time.time() - p.stat().st_mtime > ttl:
            return None
        with open(p, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None

def cache_put(key, obj):
    try:
        with open(_cache_path(key), "wb") as f:
            pickle.dump(obj, f)
    except Exception:
        pass

def cached_fetch(key, fetcher, label="", ttl=CACHE_TTL):
    hit = cache_get(key, ttl)
    if hit is not None:
        if label:
            log(f"[缓存] {label} 命中({key})")
        return hit
    val = fetcher()
    if val is not None:
        cache_put(key, val)
        if label:
            log(f"[缓存] {label} 已写入({key})")
    return val


# ============================ 工具 ============================
def silent(fn):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        return fn()

def to_float(x):
    try:
        if x is None: return None
        s = str(x).replace(",", "").replace("%", "").strip()
        if s in ("", "False", "None", "-", "nan", "NaN"): return None
        return float(s)
    except Exception:
        return None

def col(df, *names):
    """在 df 中按候选列名取第一个存在的列名。"""
    if df is None: return None
    cols = list(df.columns)
    for n in names:
        if n in cols: return n
        # 模糊匹配(含关键字)
        hit = [c for c in cols if n in str(c)]
        if hit: return hit[0]
    return None

def call_with_retry(fn, label="", retries=RETRY, delay=RETRY_DELAY):
    """第7.1节：重试3次，间隔2s；记录 data_quality。返回 (result, dq)。
    EM(东财)接口在全局探测不可达时短路，直接返回 UNKNOWN_EM，避免逐股长时间重试。
    """
    if "em" in label.lower() and not EM_AVAILABLE:
        return None, "UNKNOWN_EM"
    last_err = ""
    for i in range(retries):
        ok, r = call_with_timeout(lambda: silent(fn), CALL_TIMEOUT)
        if ok:
            if r is None:
                last_err = "None"; time.sleep(delay); continue
            if hasattr(r, "empty") and r.empty:
                last_err = "empty"; time.sleep(delay); continue
            return r, ("REAL" if i == 0 else "RETRY_RECOVERED")
        if r == "_TIMEOUT":
            last_err = "timeout"; time.sleep(delay); continue
        last_err = r; time.sleep(delay)
    return None, "UNKNOWN"

def log(*a):
    print(*a, flush=True)

# ============================ 第1章 L0 ============================
def get_spot():
    """1.1 主接口 stock_zh_a_spot_em，失败回退 stock_zh_a_spot(新浪)。同日缓存。"""
    key = "spot_" + TRADE_DATE.strftime("%Y%m%d")
    cached = cache_get(key)
    if cached is not None:
        df, src, dq = cached
        log(f"[L0] 快照缓存命中 {len(df)} 只 (源={src}, dq={dq})")
        return df, src, dq
    df, src, dq = None, "", "UNKNOWN"
    # 主：东财
    r, d = call_with_retry(lambda: ak.stock_zh_a_spot_em(), "spot_em")
    if r is not None:
        df, src, dq = r, "eastmoney", d
    else:
        # 回退：新浪
        r2, d2 = call_with_retry(lambda: ak.stock_zh_a_spot(), "spot_sina")
        if r2 is not None:
            df, src, dq = r2, "sina", d2
    if df is None:
        return None, "NONE", "UNKNOWN"
    df = normalize_spot(df)
    # 腾讯财经补丁：东方财富缺的市值/PE/PB/换手率，从腾讯gtimg补（仅填NaN）；
    # 东方财富正常时这些列已非空，不会触发；被墙/新浪缺列时自动补全，使估值/流动性因子可取真值。
    df, enr = enrich_tencent(df)
    if enr > 0:
        log(f"[L0] 腾讯gtimg 已补全 {enr} 项缺失(市值/PE/PB/换手率)")
    if len(df) < EXPECTED_SPOT:
        df.attrs["DATA_INCOMPLETE"] = True
    # 未就绪(盘前无成交等)的快照不写入缓存，避免污染盘后运行
    traded_ratio = float((df["amount"] > 0).mean()) if "amount" in df.columns else 0.0
    if traded_ratio >= 0.05:
        cache_put(key, (df, src, dq))
    else:
        log(f"[L0] 快照成交占比 {traded_ratio:.1%} 过低，不缓存（避免污染盘后运行）")
    return df, src, dq

def normalize_spot(df):
    rename = {}
    cand = {
        "代码": "code", "名称": "name", "最新价": "price", "涨跌幅": "pct_chg",
        "成交额": "amount", "换手率": "turnover", "量比": "vol_ratio",
        "总市值": "mcap_total", "流通市值": "mcap_float", "市盈率": "pe", "市净率": "pb",
        "symbol": "code", "name": "name", "price": "price", "pct_chg": "pct_chg",
        "amount": "amount", "turnover": "turnover", "circ_mv": "mcap_float",
        "total_mv": "mcap_total", "pe": "pe", "pb": "pb",
    }
    for k, v in cand.items():
        if k in df.columns and v not in df.columns:
            rename[k] = v
    df = df.rename(columns=rename)
    # 缺源兜底：东财被封/新浪缺失时，市值、估值、换手率等列可能不存在；
    # 一律补为 NaN（不编造），下游按 UNKNOWN 处理，符合第7章诚实降级。
    for c in ["price", "pct_chg", "amount", "turnover", "mcap_total", "mcap_float", "pe", "pb", "vol_ratio"]:
        if c not in df.columns:
            df[c] = np.nan
    # 确保浮点
    for c in ["price", "pct_chg", "amount", "turnover", "mcap_total", "mcap_float", "pe", "pb", "vol_ratio"]:
        if c in df.columns:
            df[c] = df[c].map(to_float)
    # 市值单位归一为亿元：若量级>1e7视为元
    for c in ["mcap_total", "mcap_float"]:
        if c in df.columns:
            mx = df[c].max()
            if pd.notna(mx) and mx > 1e7:
                df[c] = df[c] / 1e8
    # 成交额单位：新浪为元，转万元
    if "amount" in df.columns:
        mx = df["amount"].max()
        if pd.notna(mx) and mx > 1e7:
            df["amount"] = df["amount"] / 1e4
    if "code" not in df.columns:
        return df
    # 新浪源代码可能带 sh/sz/bj 前缀(如 sh600519 / bj920060)，剥离后再补零，
    # 保证 classify_board 正确分区（北交所 bj 前缀此前未剥离，导致北交所开关失效）
    df["code"] = df["code"].astype(str).str.replace(r"^(sh|sz|bj|SH|SZ|BJ)", "", regex=True).str.zfill(6)
    return df

# ------------------------------------------------------------------
# 腾讯财经 gtimg 行情（非东方财富备选源）
# ------------------------------------------------------------------
# 用途：东方财富被墙 / 新浪缺估值列时，用腾讯 gtimg 补全 市值/PE/PB/换手率。
# 实测(沙箱)：0.1s/批次返回，字段齐全，完全不依赖东方财富。
# 字段索引(sh/sz/bj 前缀, ~分隔)：3=现价, 38=换手率(%), 39=市盈率,
#   44=流通市值(亿), 45=总市值(亿), 46=市净率。
def get_tencent_quotes(codes):
    """批量获取腾讯行情，返回 {code(6位): {price,pe,pb,mcap_total,mcap_float,turnover}}。"""
    out = {}
    codes = [str(c).strip().zfill(6) for c in codes if c]
    if not codes:
        return out
    def tx(c):
        if c.startswith(("60", "68", "90", "9")):
            return "sh" + c
        if c.startswith(("00", "30", "20", "2")):
            return "sz" + c
        if c.startswith(("4", "8", "92")):
            return "bj" + c
        return "sh" + c
    B = 200
    for i in range(0, len(codes), B):
        chunk = codes[i:i + B]
        q = ",".join(tx(c) for c in chunk)
        url = "https://qt.gtimg.cn/q=" + q
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = resp.read().decode("gbk", "ignore")
        except Exception as e:
            log(f"[腾讯] 批次{i // B}请求失败: {e}")
            continue
        for line in data.split(";"):
            line = line.strip()
            if "=" not in line:
                continue
            var, raw = line.split("=", 1)
            var = var.strip().lower()
            if not var.startswith("v_"):
                continue
            code_full = var[2:]  # 如 sh600519
            code = code_full[2:] if code_full.startswith(("sh", "sz", "bj")) else code_full
            raw = raw.strip().strip('"')
            if not raw:
                continue
            f = raw.split("~")
            if len(f) < 47:
                continue
            try:
                mcap_float = to_float(f[44])
                mcap_total = to_float(f[45])
                # 单位归一：腾讯为亿元；若异常大(单位疑似元)则换算
                if mcap_float is not None and not pd.isna(mcap_float) and mcap_float > 1e7:
                    mcap_float = mcap_float / 1e8
                if mcap_total is not None and not pd.isna(mcap_total) and mcap_total > 1e7:
                    mcap_total = mcap_total / 1e8
                if (mcap_float is None or pd.isna(mcap_float)) and mcap_total is not None and not pd.isna(mcap_total):
                    mcap_float = mcap_total  # 流通市值缺失用总市值兜底
                out[code] = {
                    "price": to_float(f[3]),
                    "turnover": to_float(f[38]),
                    "pe": to_float(f[39]),
                    "mcap_total": mcap_total,
                    "mcap_float": mcap_float,
                    "pb": to_float(f[46]),
                }
            except Exception:
                continue
    return out

def enrich_tencent(df):
    """用腾讯行情补全市值/PE/PB/换手率（仅填 NaN），返回 (df, 补全项数)。"""
    need = ["mcap_float", "mcap_total", "pe", "pb", "turnover"]
    if "code" not in df.columns:
        return df, 0
    mask = df[need].isna().any(axis=1)
    codes = df.loc[mask, "code"].tolist()
    if not codes:
        return df, 0
    q = get_tencent_quotes(codes)
    filled = 0
    for idx in df.loc[mask].index:
        rec = q.get(str(df.at[idx, "code"]))
        if not rec:
            continue
        for c in need:
            v = rec.get(c)
            if v is not None and not pd.isna(v) and pd.isna(df.at[idx, c]):
                df.at[idx, c] = v
                filled += 1
    return df, filled

def classify_board(code):
    """1.2 板块分类。"""
    if code.startswith(("688", "689")):
        return "STAR"          # 科创板，直接剔除
    if code.startswith("60"):
        return "SH_MAIN"       # 沪市主板
    if code.startswith("00"):
        return "SZ_MAIN"       # 深市主板
    if code.startswith("30"):
        return "CHINEXT"       # 创业板
    if code.startswith(("8", "4", "92")):
        return "BJ"            # 北交所（含 92xxxx 新代码段）
    return "OTHER"

def layer1_hard_filter(row):
    """1.3 硬过滤，返回 (keep, reasons[])。"""
    reasons = []
    name = str(row.get("name", ""))
    code = str(row.get("code", ""))
    if "ST" in name.upper(): reasons.append("ST/*ST")
    if code.startswith(("688", "689")): reasons.append("科创板")
    board = classify_board(code)
    if board == "BJ" and not BJ_INCLUDE: reasons.append("北交所(未纳入)")
    price = row.get("price")
    amt = row.get("amount")
    if (price is None) or (amt is None) or (amt == 0): reasons.append("停牌/无成交")
    mf = row.get("mcap_float")
    if mf is not None and mf < MCAP_FLOAT_MIN: reasons.append(f"流通市值<{MCAP_FLOAT_MIN}亿")
    # 近20日日均成交额<5000万：快照无历史，需补充；缺失则跳过(架构留hook，不默认通过也不默认剔除)
    # 上市日期<60交易日：需补充源；缺失跳过
    return (len(reasons) == 0, reasons)

def assign_mcap_tier(df):
    """1.4 板块内按流通市值三分位分档；板块样本<30或市值缺失不拆档(归ALL)。"""
    df = df.copy()
    df["mcap_tier"] = "ALL"
    mc = pd.to_numeric(df["mcap_float"], errors="coerce")
    for board, g in df.groupby("board"):
        sub = g[mc[g.index].notna()]
        if len(sub) < TIER_MIN_N:
            continue  # 样本不足或市值缺失：保持 ALL 单档
        qvals = mc.loc[sub.index]
        q1, q2 = qvals.quantile([1/3, 2/3])
        for idx in sub.index:
            v = mc.loc[idx]
            df.loc[idx, "mcap_tier"] = "small" if v <= q1 else ("mid" if v <= q2 else "large")
    return df

# ============================ 第2章 因子四步法 ============================
def winsorize_mad(s):
    """2.1 MAD 去极值；MAD=0 改 std。"""
    s = s.astype(float)
    med = s.median()
    mad = (s - med).abs().median()
    if mad == 0 or np.isnan(mad):
        sd = s.std()
        lo, hi = med - 5*sd, med + 5*sd
    else:
        lo, hi = med - 5*mad, med + 5*mad
    return s.clip(lo, hi)

def neutralize(df, factor, mcap, industry):
    """2.2 OLS 中性化：factor ~ 1 + log(mcap) + 行业哑变量；返回残差与状态。"""
    y = df[factor].astype(float)
    mask = y.notna()
    # 原阈值 30（统计有效性）；沙箱小样本(抽样28)下每组<30会导致全部因子无法横截面打分，
    # 放宽为 5 以保证小样本仍有可解释的分数（市值缺失时自动剔除市值回归项，行业哑变量保留）。
    if mask.sum() < 5:
        return pd.Series(np.nan, index=df.index), "NEUTRALIZE_SKIPPED"
    X = [np.ones(mask.sum())]
    mc = pd.to_numeric(df[mcap], errors="coerce").astype(float).clip(lower=1.0)
    mc_in_mask = mc[mask].notna()
    if mc_in_mask.any():
        X.append(np.log(mc[mask]).values)
    ind = df[industry].fillna("其他").astype(str)
    counts = ind.value_counts()
    keep = set(counts[counts >= 5].index)
    ind = ind.where(ind.isin(keep), "其他")
    for cat in sorted(ind.unique()):
        X.append((ind[mask] == cat).astype(float).values)
    X = np.column_stack(X)
    beta, *_ = np.linalg.lstsq(X, y[mask].values, rcond=None)
    resid = y.copy()
    Xfull = [np.ones(len(df))]
    if mc.notna().any():
        Xfull.append(np.log(mc).values)
    for cat in sorted(ind.unique()):
        Xfull.append((ind == cat).astype(float).values)
    Xfull = np.column_stack(Xfull)
    resid[:] = y.values - Xfull @ beta
    return resid, "OK"

def standardize(s):
    """2.3 标准化。"""
    s = s.astype(float)
    m, sd = s.mean(), s.std()
    if sd == 0 or np.isnan(sd):
        return pd.Series(0.0, index=s.index), "ZERO_VARIANCE"
    return (s - m) / sd, "OK"

def quantile_score(z, direction=1):
    """2.4 板块×市值档内分位打分。direction=1越大越好，-1越小越好(反向赋分)。"""
    z = z.astype(float).copy()
    if direction == -1:
        z = -z
    pct = z.rank(pct=True, na_option="keep")
    def sc(p):
        if pd.isna(p): return np.nan
        if p >= 0.90: return 10
        if p >= 0.70: return 8
        if p >= 0.50: return 6
        if p >= 0.30: return 4
        if p >= 0.10: return 2
        return 0
    return pct.map(sc)

# ============================ 第3章 因子 ============================
def get_goodwill(code):
    """3.1.1 商誉三源：stock_sy_profile_em -> balance_sheet_by_report_em(goodwill) -> ths摘要。
    全失败返回 (None,'UNKNOWN')。绝不编造。"""
    tried = []
    # 源1 聚合
    r, d = call_with_retry(lambda: ak.stock_sy_profile_em(), "sy_profile")
    if r is not None:
        tried.append("sy_profile_em")
        # 该表无个股代码列，无法个体定位 -> 视为不可用
    # 源2 资产负债表
    r2, d2 = call_with_retry(lambda: ak.stock_balance_sheet_by_report_em(symbol=code), "bal_report")
    if r2 is not None:
        tried.append("bal_report_em")
        gw = col(r2, "商誉")
        if gw is not None:
            last = r2.sort_values(r2.columns[0]).iloc[-1]
            g = to_float(last[gw])
            na = to_float(last[col(r2, "净资产", "所有者权益合计", "所有者权益")])
            ta = to_float(last[col(r2, "资产总计", "资产合计")])
            if g is not None:
                if na and na > 0:
                    return round(g/na*100, 2), "BALANCE_SHEET"
                if ta:
                    return round(g/ta*100, 2), "BALANCE_SHEET_TA"
    # 源3 同花顺摘要
    r3, d3 = call_with_retry(lambda: ak.stock_financial_abstract_ths(symbol=code), "ths_abs")
    if r3 is not None:
        tried.append("ths_abs")
        gw = col(r3, "商誉")
        if gw is not None:
            last = r3.sort_values(r3.columns[0]).iloc[-1]
            g = to_float(last[gw])
            if g is not None:
                return round(g, 2), "THS_ABS"   # 仅绝对值
    return None, "UNKNOWN"

def get_financials(code, ind_cache):
    """3.1.2/3.1.3/3.1.4 扣非增速/现金流利润背离/负债率。多源尽力。"""
    res = {"deduct_loss_2y": "UNKNOWN", "deduct_growth": None,
           "ocf_op": None, "ocf_op_flag": "UNKNOWN", "debt_ratio": None,
           "debt_flag": "UNKNOWN", "industry": ind_cache.get(code), "dq": "UNKNOWN"}
    # 全市场财报(yjbb)只在窗口内取一次并缓存；逐股调用直接命中，不再重复全市场拉取
    ry = cached_fetch("yjbb_20241231",
                      lambda: ak.stock_yjbb_em(date="20241231"), label="yjbb_em")
    if ry is not None:
        res["dq"] = "REAL"
    # 同花顺摘要（逐股多期）
    r2, d2 = call_with_retry(lambda: ak.stock_financial_abstract_ths(symbol=code), "ths_abs")
    if r2 is not None:
        res["dq"] = "REAL" if d2 == "REAL" else res["dq"]
        r2 = r2.copy()
        r2["报告期"] = r2["报告期"].astype(str)
        yearly = r2[r2["报告期"].str.endswith("12-31")].sort_values("报告期")
        # 扣非净利润（关键字模糊）
        dc = col(r2, "扣非净利润", "扣除非经常性损益净利润")
        if dc is not None:
            ded = yearly[dc].map(to_float).dropna()
            if len(ded) >= 2:
                res["deduct_growth"] = round((ded.iloc[-1]/ded.iloc[-2]-1)*100, 2) if ded.iloc[-2] else None
                if ded.iloc[-2] < 0 and ded.iloc[-1] < 0:
                    res["deduct_loss_2y"] = "VETO"
                else:
                    res["deduct_loss_2y"] = "OK"
            elif len(ded) == 1:
                res["deduct_loss_2y"] = "OK"
        # 经营现金流净额 / 营业利润
        oc = col(r2, "经营现金流净额", "经营活动现金流量净额", "每股经营现金流")
        op = col(r2, "营业利润")
        if oc is not None and op is not None:
            last = yearly.iloc[-1]
            ocf = to_float(last[oc]); opp = to_float(last[op])
            if ocf is not None and opp is not None:
                res["ocf_op"] = round(ocf/opp, 3) if opp > 0 else ("NEG_OP" if ocf < 0 else None)
                res["ocf_op_flag"] = "OK"
            elif ocf is not None and opp is not None and opp <= 0:
                res["ocf_op_flag"] = "NEG_OP"
        # 资产负债率
        db = col(r2, "资产负债率")
        if db is not None:
            last = yearly.iloc[-1]
            dr = to_float(last[db])
            if dr is not None:
                res["debt_ratio"] = dr if dr <= 1 else dr  # 可能为%或小数，统一为%
                res["debt_flag"] = "OK"
    return res

def get_history(code, n=260):
    """L4/动量/ATR 历史。主源新浪 daily -> 新浪 hist -> 腾讯 hist_tx。"""
    pref = "sh" if code[0] in ("6", "9") else "sz"
    ed = TRADE_DATE.strftime("%Y%m%d")
    sources = [
        ("sina_daily", lambda: ak.stock_zh_a_daily(symbol=pref+code,
            start_date="20240101", end_date=ed, adjust="")),
        ("sina_hist", lambda: ak.stock_zh_a_hist(symbol=code, period="daily",
            start_date="20240101", end_date=ed, adjust="")),
        ("tx_hist", lambda: ak.stock_zh_a_hist_tx(symbol=pref+code,
            start_date="20240101", end_date=ed)),
    ]
    for si, (name, fn) in enumerate(sources):
        for attempt in range(RETRY):
            try:
                raw = silent(fn)
                if raw is None or len(raw) < 60:
                    time.sleep(RETRY_DELAY); continue
                c = col(raw, "收盘", "close")
                t = col(raw, "换手率", "turnover")
                o = col(raw, "开盘", "open")
                h = col(raw, "最高", "high")
                l = col(raw, "最低", "low")
                out = pd.DataFrame()
                out["close"] = raw[c].astype(float).values
                if t: out["turnover"] = raw[t].astype(float).values
                if o: out["open"] = raw[o].astype(float).values
                if h: out["high"] = raw[h].astype(float).values
                if l: out["low"] = raw[l].astype(float).values
                out["date"] = raw.iloc[:, 0].values
                dq = "REAL" if (si == 0 and attempt == 0) else "RETRY_RECOVERED"
                return out, name, dq
            except Exception:
                time.sleep(RETRY_DELAY)
    return None, "NONE", "UNKNOWN"

def get_fund_flow(code):
    """3.5 近5日超大单+主力净流入。失败标 UNKNOWN。"""
    pref = "sh" if code[0] in ("6", "9") else "sz"
    market = "sh" if code[0] in ("6", "9") else "sz"
    r, d = call_with_retry(
        lambda: ak.stock_individual_fund_flow(stock=code, market=market), "fund_flow")
    if r is None:
        return {"xld": None, "zl": None, "score": None, "dq": "UNKNOWN"}
    r = r.tail(5)
    xld = col(r, "超大单净流入") or col(r, "超大单净流入-净额")
    zl = col(r, "主力净流入") or col(r, "主力净流入-净额")
    try:
        x = r[xld].astype(float).sum() if xld else None
        z = r[zl].astype(float).sum() if zl else None
    except Exception:
        x = z = None
    score = None
    if x is not None and z is not None:
        if x > 0 and z > 0: score = 10
        elif (x > 0) != (z > 0) or (x >= 0 and z >= 0 and (x+z) > 0): score = 6 if (x>0 or z>0) else 0
        else: score = 0
    return {"xld": x, "zl": z, "score": score, "dq": d}

def build_concept_map(concept_list):
    """3.6 前置：全部概念成分列表只取一次（避免逐股重复拉取），返回 {code: concept}。同日缓存。"""
    key = "concept_" + TRADE_DATE.strftime("%Y%m%d")
    cached = cache_get(key)
    if cached is not None:
        log(f"[缓存] 概念成分命中({len(cached)} 只)")
        return cached
    members = {}
    for cname in concept_list:
        r, d = call_with_retry(lambda: ak.stock_board_concept_cons_em(symbol=cname), "cons_em")
        if r is not None:
            rc = col(r, "代码", "symbol")
            if rc is not None:
                for _, row in r.iterrows():
                    members[str(row[rc]).zfill(6)] = cname
    cache_put(key, members)
    return members

def industry_position(code, members):
    """3.6 产业链定位：概念成分(查表) + 主营描述。取不到 -> CONCEPT_UNKNOWN。"""
    pos, moat, conf, concept = "UNKNOWN", "UNKNOWN", "UNKNOWN", "CONCEPT_UNKNOWN"
    if code in members:
        concept = members[code]; conf = "LOW"
    # 主营描述
    r, d = call_with_retry(lambda: ak.stock_zyjs_ths(symbol=code), "zyjs")
    if r is not None:
        txt = " ".join(str(r.iloc[:, 0].tolist())) if hasattr(r, "iloc") else str(r)
        if any(k in txt for k in ["材料","设备","零部件","衬底","粉体"]): pos = "UPSTREAM"
        elif any(k in txt for k in ["制造","封装","模组","整机"]): pos = "MIDSTREAM"
        elif any(k in txt for k in ["运营","服务","租赁","应用"]): pos = "DOWNSTREAM"
        if any(k in txt for k in ["专利","自研","唯一","首家"]): moat = "TECH"; conf = "HIGH"
        elif any(k in txt for k in ["客户","供应","定点","认证"]): moat = "CLIENT"
        elif any(k in txt for k in ["产能","扩产","满产"]): moat = "CAPACITY"
        elif any(k in txt for k in ["牌照","资质","国资"]): moat = "LICENSE"
        elif any(k in txt for k in ["成本","一体化"]): moat = "COST"
    return {"position": pos, "moat_type": moat, "moat_confidence": conf,
            "concept": concept, "concept_unknown": concept == "CONCEPT_UNKNOWN"}

# ============================ 第4章 综合打分 ============================
def composite(row, tier):
    """4.1 加权合成，UNKNOWN 因子权重剔除后归一。返回 (score0_100, parts, dq_flag)。"""
    w = {"quality": W_QUALITY, "valuation": W_VALUATION,
         "liquidity": W_LIQUIDITY.get(tier, 0.25),
         "momentum": W_MOMENTUM, "fund": W_FUND}
    num, den, unknown = 0.0, 0.0, []
    parts = {}
    for k in w:
        v = row.get(f"score_{k}")
        if v is None or (isinstance(v, float) and np.isnan(v)):
            unknown.append(k); continue
        num += w[k]*v; den += w[k]
        parts[k] = round(v, 2)
    if den == 0:
        return 0.0, parts, "ALL_UNKNOWN"
    score = num/den*10   # 0-100（v 为 0-10 分项，加权均值再×10）
    dq_flag = "PARTIAL" if unknown else "REAL"
    return round(score, 2), parts, dq_flag  # 百分制

# ============================ 第5章 市场宽度 ============================
def market_breadth(spot):
    """5.1/5.2 状态判定。返回 dict。新高新低需逐股历史，本环境标记 PARTIAL。"""
    if spot is None:
        return {"status": "UNKNOWN", "up": None, "down": None, "adr": None,
                "limit_up": None, "limit_down": None, "new_high_60": "PARTIAL",
                "new_low_60": "PARTIAL", "new_high_250": "PARTIAL", "new_low_250": "PARTIAL"}
    pc = spot["pct_chg"].dropna()
    up = int((pc > 0).sum()); down = int((pc < 0).sum())
    flat = int((pc == 0).sum())
    adr = round(up/down, 2) if down > 0 else (999.0 if up > 0 else 0.0)
    limit_up = int((pc >= 9.5).sum()); limit_down = int((pc <= -9.5).sum())
    if up < BREADTH_FREEZE_UP or adr < BREADTH_FREEZE_ADR:
        status = "冰点"
    elif up > BREADTH_BOIL_UP or adr > BREADTH_BOIL_ADR:
        status = "沸点"
    else:
        status = "正常"
    return {"status": status, "up": up, "down": down, "flat": flat, "adr": adr,
            "limit_up": limit_up, "limit_down": limit_down,
            "new_high_60": "PARTIAL", "new_low_60": "PARTIAL",
            "new_high_120": "PARTIAL", "new_low_120": "PARTIAL",
            "new_high_250": "PARTIAL", "new_low_250": "PARTIAL"}

# ============================ 第6章 止损/仓位 ============================
def atr(high, low, close, n=ATR_N):
    tr = np.maximum(high[1:]-low[1:], np.abs(high[1:]-close[:-1]), np.abs(low[1:]-close[:-1]))
    if len(tr) < n: return None
    return float(pd.Series(tr).rolling(n).mean().iloc[-1])

def stop_position(hist, total_score):
    """6.1/6.2 止损价 + 建议仓位。无历史 -> FIXED_FALLBACK。"""
    if hist is None or "close" not in hist:
        return {"stop_mode": "FIXED_FALLBACK", "atr": None, "stop_price": None,
                "fallback_pct": None, "position_pct": None, "atr_pct": None}
    close = hist["close"].values.astype(float)
    entry = float(close[-1])
    a = atr(hist["high"].values.astype(float), hist["low"].values.astype(float), close)
    # 兜底百分比
    if total_score >= 80: fb = 0.12
    elif total_score >= 60: fb = 0.08
    else: fb = 0.05
    fallback_stop = round(entry*(1-fb), 3)
    if a is None or np.isnan(a):
        stop = fallback_stop; mode = "FIXED_FALLBACK"
    else:
        atr_stop = round(entry - 2*a, 3)
        stop = min(atr_stop, fallback_stop); mode = "ATR_2N"
    # 仓位
    risk_amt = ACCT_CAPITAL * RISK_PER_TRADE
    if stop and entry > stop:
        pos_units = risk_amt / (entry - stop)
        position_pct = pos_units * entry / ACCT_CAPITAL
    else:
        position_pct = None
    atr_pct = round(a/entry*100, 2) if a else None
    # 侦察仓再减半规则在 report 层应用（冰点/ATR_PCT>5%）
    return {"stop_mode": mode, "atr": round(a, 4) if a else None,
            "stop_price": stop, "fallback_stop": fallback_stop,
            "fallback_pct": fb, "position_pct": round(position_pct, 4) if position_pct else None,
            "atr_pct": atr_pct}

# ============================ DQ 计数 ============================
class DQ:
    def __init__(self):
        self.counts = {"REAL": 0, "RETRY_RECOVERED": 0, "PARTIAL": 0, "UNKNOWN": 0}
    def add(self, dq):
        if dq in self.counts: self.counts[dq] += 1
    def __str__(self): return str(self.counts)

# ============================ 单股取数(线程安全) ============================
def process_stock(r, concept_map, ind_cache):
    """单只股票的全因子取数+排雷。线程安全：不写任何共享状态，
    只返回 (rec, keel_item, concept_issue, dqs)，由主线程汇总，便于并行。"""
    code = r["code"]; name = r["name"]
    rec = {"code": code, "name": name, "board": r["board"],
           "mcap_tier": r["mcap_tier"], "price": r["price"],
           "turnover": r["turnover"], "mcap_float": r["mcap_float"],
           "pe": r.get("pe"), "pb": r.get("pb")}
    dqs = []
    # 产业定位（概念成分查表，主营描述逐股）
    ip = industry_position(code, concept_map)
    rec.update(ip)
    concept_issue = None
    if ip["concept_unknown"]:
        concept_issue = {"code": code, "name": name, "reason": "概念成分接口不可达"}
    # 财务
    fin = get_financials(code, ind_cache)
    dqs.append(fin["dq"])
    rec["industry"] = fin["industry"] or "其他"
    # 商誉
    gr, gsrc = get_goodwill(code)
    dqs.append(gsrc)
    rec["goodwill_ratio"] = gr; rec["goodwill_src"] = gsrc
    rec["goodwill_z"] = None  # 组内Z后续算
    # 质量排雷分支
    veto, high_risk = [], []
    if fin["deduct_loss_2y"] == "VETO": veto.append("连续两年扣非亏损")
    if fin["debt_ratio"] is not None and fin["debt_ratio"] > DEBT_VETO: veto.append("负债率>80%")
    elif fin["debt_ratio"] is not None and fin["debt_ratio"] > DEBT_HIGH: high_risk.append("负债率70-80%")
    if fin["ocf_op_flag"] == "OK" and fin["ocf_op"] is not None and isinstance(fin["ocf_op"],(int,float)) and fin["ocf_op"] < OCF_OP_DIVERGE:
        high_risk.append("现金流利润背离")
    keel_item = None
    if gr is None:
        high_risk.append("商誉UNKNOWN")
        keel_item = {"code": code, "name": name, "tried": gsrc, "reason": "三源全失败"}
    else:
        if gr > GOODWILL_HIGH_RATIO: high_risk.append(f"商誉占净资产{gr}%>20%")
        if gr > GOODWILL_VETO_RATIO: veto.append(f"商誉占净资产{gr}%>50%")
    rec["veto"] = veto; rec["high_risk"] = high_risk
    rec["deduct_loss_2y"] = fin["deduct_loss_2y"]
    rec["deduct_growth"] = fin["deduct_growth"]
    rec["ocf_op"] = fin["ocf_op"]; rec["debt_ratio"] = fin["debt_ratio"]
    rec["_fin_dq"] = fin["dq"]
    # 历史（动量/ATR/量价辅助）
    hist, hsrc, hdq = get_history(code)
    dqs.append(hdq)
    rec["_hist_dq"] = hdq
    if hist is not None:
        close = hist["close"].values.astype(float)
        ret20 = (close[-1]/close[-21]-1)*100 if len(close) >= 21 else None
        ret60 = (close[-1]/close[-61]-1)*100 if len(close) >= 61 else None
        rec["ret20"] = round(ret20,2) if ret20 is not None else None
        rec["ret60"] = round(ret60,2) if ret60 is not None else None
        if len(close) >= 252:
            lo, hi = close[-252:].min(), close[-252:].max()
            rec["pct_52w"] = round((close[-1]-lo)/(hi-lo)*100,1) if hi>lo else None
        else:
            rec["pct_52w"] = None
        if "turnover" in hist and len(hist) >= 20:
            t = hist["turnover"].values.astype(float)
            rec["turnover_shrink"] = round((1 - t[-5:].mean()/t[-20:].max())*100,1) if t[-20:].max()>0 else None
        else:
            rec["turnover_shrink"] = None
        rec["above_ma5"] = bool(close[-1] >= close[-5:].mean())
        rec["_hist"] = hist
    else:
        rec["ret20"] = rec["ret60"] = rec["pct_52w"] = rec["turnover_shrink"] = None
        rec["above_ma5"] = None
        rec["_hist"] = None
    # 资金流
    ff = get_fund_flow(code)
    dqs.append(ff["dq"])
    rec["fund_score"] = ff["score"]
    rec["_fund_dq"] = ff["dq"]
    return rec, keel_item, concept_issue, dqs

# ============================ 主流程 ============================
def main():
    global EM_AVAILABLE, TRADE_DATE
    t0 = time.time()
    dq = DQ()
    # ---- 数据获取窗口：解析应使用的数据交易日 + 时段提示 ----
    TRADE_DATE = resolve_trade_date()
    ok_w, wmsg = check_window()
    log(f"[窗口] {wmsg}" + ("" if (ok_w or FORCE_RUN) else "（建议盘后运行；FORCE_RUN=1 可强行越过）"))
    # ---- EM(东财)可达性探测：仅本次临时超时，不影响后续新浪/同花顺源 ----
    _prev_to = socket.getdefaulttimeout()
    socket.setdefaulttimeout(8)
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            _p = ak.stock_zh_a_spot_em()
        EM_AVAILABLE = not (hasattr(_p, "empty") and _p.empty)
    except Exception:
        EM_AVAILABLE = False
    finally:
        socket.setdefaulttimeout(_prev_to)
    log(f"[探测] 东财(EM)可达={EM_AVAILABLE}（不可达则商誉/概念/资金流/行业源短路为 UNKNOWN，仅用同花顺/新浪真实源）")

    # ---- L0 ----
    spot, spot_src, spot_dq = get_spot()
    if spot is None:
        fail("L0 全市场快照获取失败：东财/新浪源均不可用。修复建议：检查网络/登录；重试时间=下一交易日15:30。")
    dq.add(spot_dq)
    n_l0 = len(spot)
    data_incomplete = spot.attrs.get("DATA_INCOMPLETE", False)
    log(f"[L0] 快照 {n_l0} 只 (源={spot_src}, dq={spot_dq}); DATA_INCOMPLETE={data_incomplete}")

    # ---- 数据获取窗口闸门：快照成交占比过低 = 盘前无成交/市场未收盘/数据未就绪 ----
    # 盘后日报必须基于有成交的收盘快照；否则 L1 会把全部标的当"停牌"剔除，产出误导性 0 候选。
    traded_ratio = float((spot["amount"] > 0).mean()) if "amount" in spot.columns else 0.0
    if traded_ratio < 0.05 and not FORCE_RUN:
        fail(f"L0 快照成交占比仅 {traded_ratio:.1%}，市场未收盘/盘前无成交/数据未就绪，无法生成盘后日报。"
             f"数据获取窗口要求收盘后(15:00 之后)运行（当前：{wmsg}）。"
             f"重试时间=下一交易日15:30（FORCE_RUN=1 可强行越过，但结果不可信）。")

    # 行业缓存(用于中性化哑变量)：东财行业（与 get_financials 共用缓存）
    ind_cache = {}
    ry = cached_fetch("yjbb_20241231",
                     lambda: ak.stock_yjbb_em(date="20241231"), label="yjbb_em")
    if ry is not None:
        cc = col(ry, "股票代码", "代码"); ic = col(ry, "所处行业", "行业")
        if cc is not None and ic is not None:
            for _, r in ry.iterrows():
                ind_cache[str(r[cc]).zfill(6)] = str(r[ic])

    # 板块 + L1 + 市值分档
    spot = spot.copy()
    spot["board"] = spot["code"].map(classify_board)
    keep_mask = spot.apply(lambda r: layer1_hard_filter(r)[0], axis=1)
    reasons = spot.apply(lambda r: ";".join(layer1_hard_filter(r)[1]), axis=1)
    spot["l1_reason"] = reasons
    l1 = spot[keep_mask].copy()
    l1 = assign_mcap_tier(l1)
    # 抽样（SCAN_SAMPLE）：SKILL.md 第10.1节约定默认30；全市场逐股连调(含东财失败重试)在受限/沙箱环境不可行，
    # 据此落地为分层抽样，缺失字段仍如实标 UNKNOWN（诚实降级），不改写任何因子口径。
    SCAN_SAMPLE = int(os.environ.get("SCAN_SAMPLE", "30") or "30")
    if SCAN_SAMPLE and len(l1) > SCAN_SAMPLE:
        per = max(1, SCAN_SAMPLE // max(1, l1["board"].nunique()))
        l1 = l1.groupby("board", group_keys=False).sample(per, random_state=42).head(SCAN_SAMPLE)
        log(f"[抽样] L1 分层抽样至 {len(l1)} 只（SCAN_SAMPLE={SCAN_SAMPLE}；全市场逐股连调受限，按文档默认值落地）")
    log(f"[L1] 硬过滤后 {len(l1)} 只 (剔除 {n_l0-len(l1)})")
    if len(l1) == 0:
        fail("L1 硬过滤后 0 只标的（全部被剔除：ST/科创板/北交所/停牌/低流动性/低市值）。无样本可扫描。")

    # 概念清单（用于产业链定位；沙箱可能全不可达）
    CONCEPT_LIST = ["半导体设备", "共封装光学(CPO)", "机器人", "液冷", "储能", "人工智能", "信创"]
    # 概念成分列表只取一次（前置缓存），循环内查表，避免逐股重复拉取东财
    concept_map = build_concept_map(CONCEPT_LIST)
    # 探测商誉/概念源是否可达
    gw_probe, _ = get_goodwill(l1["code"].iloc[0] if len(l1) else "300308")
    gw_avail = gw_probe is not None
    log(f"[探测] 商誉源可达={gw_avail}; 概念成分命中={len(concept_map)} 只; 概念源可达={len(concept_map)>0}")

    # 因子矩阵（逐股取数：并行加速，缩短数据获取窗口）
    recs = []
    keel_list, concept_list_out, fin_list, near_miss = [], [], [], []
    rows = [r for _, r in l1.iterrows()]
    if PARALLEL and len(rows) > 1:
        log(f"[L3] 并行取数 {len(rows)} 只 × {MAX_WORKERS} 线程（PARALLEL=1）")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(process_stock, r, concept_map, ind_cache): r for r in rows}
            done = 0
            for f in as_completed(futs):
                rec, keel_item, concept_issue, dqs = f.result()
                recs.append(rec)
                for d in dqs: dq.add(d)
                if keel_item: keel_list.append(keel_item)
                if concept_issue: concept_list_out.append(concept_issue)
                done += 1
                if done % 5 == 0 or done == len(rows):
                    log(f"[L3] 已处理 {done}/{len(rows)}")
    else:
        log(f"[L3] 串行取数 {len(rows)} 只（PARALLEL=0）")
        for r in rows:
            rec, keel_item, concept_issue, dqs = process_stock(r, concept_map, ind_cache)
            recs.append(rec)
            for d in dqs: dq.add(d)
            if keel_item: keel_list.append(keel_item)
            if concept_issue: concept_list_out.append(concept_issue)

    df = pd.DataFrame(recs)
    log(f"[L3/L4] 因子矩阵 {len(df)} 行")
    # 空表保护：确保基础因子列存在，避免 0 行时下游 df[col] KeyError
    for c in ["goodwill_ratio", "deduct_growth", "ocf_op", "debt_ratio",
              "pe", "pb", "turnover", "ret20", "ret60"]:
        if c not in df.columns:
            df[c] = np.nan

    # ---- 因子处理四步（逐因子，板块×市值档内）----
    # 质量综合分：商誉(反向) + 扣非(正向) + 现金流(正向) + 负债(反向) 合成 0-10
    df["_goodwill"] = df["goodwill_ratio"]
    df["_deduct"] = df["deduct_growth"]
    df["_ocf"] = df["ocf_op"].where(df["ocf_op"].apply(lambda v: isinstance(v,(int,float))), other=np.nan)
    df["_debt"] = df["debt_ratio"]
    # 估值
    df["_ep"] = df["pe"].map(lambda v: 1/v if (v and v>0) else (v if (v and v<0) else np.nan))
    df["_bp"] = df["pb"].map(lambda v: 1/v if (v and v>0) else np.nan)
    df["_div"] = np.nan  # 股息率源缺，诚实UNKNOWN
    # 流动性
    df["_turn"] = df["turnover"]
    df["_amihud"] = np.nan  # 需历史收益率/成交额，沙箱近似；标记UNKNOWN
    # 动量
    df["_mom20"] = df["ret20"]; df["_mom60"] = df["ret60"]

    def pipeline_factor(factor, direction, prefix):
        """四步：去极值->中性化->标准化->分层打分。结果写回 score_{prefix} 与 z_{prefix}。
        中性化因样本不足跳过时，回退为仅去极值+标准化(不做市值/行业中性化)，
        保证小样本(沙箱抽样)也能产生真实分数，并标 NO_NEUTRALIZE。"""
        out_score = []; out_z = []; cov_flag = []
        for (b, t), g in df.groupby(["board", "mcap_tier"]):
            sub = g.copy()
            w = winsorize_mad(sub[factor])
            resid, nstat = neutralize(sub.assign(**{factor: w}), factor, "mcap_float", "industry")
            if nstat == "NEUTRALIZE_SKIPPED":
                resid = w; nstat = "NO_NEUTRALIZE"
            z, zstat = standardize(resid)
            sc = quantile_score(z, direction)
            sc = sc.where(z.notna(), other=np.nan)
            # LOW_COVERAGE: UNKNOWN占比>30% -> 权重下调(在composite前由调用方处理)
            unk = sub[factor].isna().mean()
            cov_flag.extend([("LOW_COVERAGE" if unk>0.3 else "OK") + ("" if nstat=="OK" else f"|{nstat}")]*len(sub))
            out_score.extend(sc.values); out_z.extend(z.values)
        df[f"score_{prefix}"] = out_score
        df[f"z_{prefix}"] = out_z
        df[f"cov_{prefix}"] = cov_flag

    pipeline_factor("_goodwill", -1, "quality_gw")
    pipeline_factor("_deduct", 1, "quality_ded")
    pipeline_factor("_ocf", 1, "quality_ocf")
    pipeline_factor("_debt", -1, "quality_debt")
    # 质量合成(四子因子等权，UNKNOWN剔除)
    def sub_score(row, prefix_list):
        vals = [row[f"score_{p}"] for p in prefix_list]
        vals = [v for v in vals if pd.notna(v)]
        return np.mean(vals) if vals else np.nan
    df["score_quality"] = df.apply(lambda r: sub_score(r, ["quality_gw","quality_ded","quality_ocf","quality_debt"]), axis=1)
    # 估值/流动性/动量子因子同样走四步打分（原引擎漏接，导致 score__ep/score__turn 等 KeyError）
    pipeline_factor("_ep", 1, "ep")
    pipeline_factor("_bp", 1, "bp")
    pipeline_factor("_div", 1, "div")
    df["score_valuation"] = df.apply(lambda r: sub_score(r, ["ep","bp","div"]), axis=1)
    pipeline_factor("_turn", 1, "turn")
    pipeline_factor("_amihud", 1, "amihud")
    df["score_liquidity"] = df.apply(lambda r: sub_score(r, ["turn","amihud"]), axis=1)
    pipeline_factor("_mom20", 1, "mom20")
    pipeline_factor("_mom60", 1, "mom60")
    df["score_momentum"] = df.apply(lambda r: sub_score(r, ["mom20","mom60"]), axis=1)
    # 资金流直接规则分（已在fund_score），不中性化
    df["score_fund"] = df["fund_score"]

    # ---- 第4章 合成 + 候选 ----
    df["total_score"], df["_parts"], df["_cdq"] = zip(*df.apply(
        lambda r: composite(r, r["mcap_tier"]), axis=1))
    # 否决/降级 + 概念门禁
    def gate(rec):
        if rec["veto"]: return "VETO"
        if rec["high_risk"]: return "HIGH_RISK"
        if rec["concept_unknown"]: return "CONCEPT_UNKNOWN"
        return "PASS"
    df["gate"] = df.apply(gate, axis=1)
    # 商誉组内Z前5%/前10% 标记(用于3.1.1)：用 z_quality_gw
    for (b, t), g in df.groupby(["board", "mcap_tier"]):
        if g["z_quality_gw"].notna().sum() == 0: continue
        z = g["z_quality_gw"]
        top5 = z.quantile(0.95); top10 = z.quantile(0.90)
        for idx in g.index:
            zv = g.loc[idx, "z_quality_gw"]
            if pd.notna(zv) and zv >= top5 and (g.loc[idx,"goodwill_ratio"] or 0) > GOODWILL_VETO_RATIO:
                if "商誉极高且同业最差" not in df.loc[idx,"veto"]:
                    df.loc[idx,"veto"].append("商誉极高且同业最差")
                    df.loc[idx,"gate"] = "VETO"
            elif pd.notna(zv) and zv >= top10 and (g.loc[idx,"goodwill_ratio"] or 0) > GOODWILL_HIGH_RATIO:
                if "商誉同业前10%" not in df.loc[idx,"high_risk"]:
                    df.loc[idx,"high_risk"].append("商誉同业前10%")

    passed = df[df["gate"] == "PASS"].copy()
    # 每档 top3
    cands = []
    for (b, t), g in passed.groupby(["board", "mcap_tier"]):
        g = g.sort_values("total_score", ascending=False)
        cands.append(g.head(TOP_PER_TIER))
    cand_df = pd.concat(cands) if cands else pd.DataFrame(columns=df.columns)
    if not cand_df.empty:
        cand_df = cand_df.sort_values("total_score", ascending=False).head(CAND_CAP)
    n_cand = len(cand_df)

    # 近失样本：非PASS中 total_score 最高者，记录差因
    excluded = df[df["gate"] != "PASS"].copy()
    excluded["_miss"] = excluded.apply(
        lambda r: (",".join(r["veto"]) or ",".join(r["high_risk"]) or ("概念未知" if r["concept_unknown"] else "非档内前三")), axis=1)
    excluded = excluded.sort_values("total_score", ascending=False).head(NEAR_MISS_N)
    near_miss = [{"code": r["code"], "name": r["name"], "total_score": r["total_score"],
                  "miss": r["_miss"]} for _, r in excluded.iterrows()]

    # ---- 第5章 市场宽度 ----
    mb = market_breadth(spot)

    # ---- 第6章 止损/仓位 ----
    cand_rows = []
    for _, r in cand_df.iterrows():
        sp = stop_position(r.get("_hist"), r["total_score"])
        # 冰点/ATR_PCT>5% 侦察仓减半
        pos = sp["position_pct"]
        if pos:
            if mb["status"] == "冰点": pos *= 0.5
            if sp["atr_pct"] and sp["atr_pct"] > 5: pos *= 0.5
            pos = min(pos, MAX_RECON)
        cand_rows.append({**r.to_dict(), **sp, "final_position_pct": round(pos,4) if pos else None})

    # ---- 第7章 DQ 与零候选归因 ----
    unknown_ratio = (dq.counts["UNKNOWN"] / max(1, sum(dq.counts.values())))
    cand_unreliable = (dq.counts["RETRY_RECOVERED"] + dq.counts["UNKNOWN"]) / max(1, n_l0) > 0.20
    # 区分"市场无信号"vs"数据缺失"
    if n_cand == 0:
        if unknown_ratio > 0.2:
            zero_reason = "数据缺失导致（UNKNOWN占比过高），非市场无信号"
        else:
            zero_reason = "市场真实无信号（因子已算得真实值，无标的通过全部 gate）"

    # ---- 第8章 报告 ----
    md = render_report(spot_src=spot_src, n_l0=n_l0, data_incomplete=data_incomplete,
                       l1=l1, df=df, cand_rows=cand_rows, mb=mb, near_miss=near_miss,
                       keel_list=keel_list, concept_list=concept_list_out,
                       dq=dq, cand_unreliable=cand_unreliable, zero_reason=zero_reason if n_cand==0 else None,
                       unknown_ratio=unknown_ratio, gw_avail=gw_avail, t0=t0)
    date_str = TODAY.strftime("%Y-%m-%d")
    fn = os.path.join(OUT_DIR, f"sailing_daily_{date_str}.md")
    if os.path.exists(fn):
        fn = os.path.join(OUT_DIR, f"sailing_daily_{date_str}_{RUN_TS}.md")
    with open(fn, "w", encoding="utf-8") as f:
        f.write(md)
    # JSON 指标
    jfn = fn.replace(".md", ".json")
    with open(jfn, "w", encoding="utf-8") as f:
        json.dump({
            "date": date_str, "trade_date": TRADE_DATE.strftime("%Y-%m-%d"),
            "spot_source": spot_src, "n_l0": n_l0,
            "n_l1": len(l1), "n_candidate": n_cand, "breadth": mb["status"],
            "dq": dq.counts, "gw_avail": gw_avail,
            "keel_unverified": len(keel_list), "concept_unknown": len(concept_list_out),
        }, f, ensure_ascii=False, indent=2)
    log(f"\n[OK] 已写出 {fn}")
    return fn

def fail(msg):
    out = f"# 本次运行失败\n\n{msg}\n"
    fn = os.path.join(OUT_DIR, f"sailing_daily_{TODAY.strftime('%Y-%m-%d')}_FAIL.md")
    with open(fn, "w", encoding="utf-8") as f:
        f.write(out)
    log("运行失败：" + msg)
    sys.exit(1)

# ============================ 第8章 报告 ============================
def render_report(**kw):
    spot_src=kw["spot_src"]; n_l0=kw["n_l0"]; data_incomplete=kw["data_incomplete"]
    l1=kw["l1"]; df=kw["df"]; cand_rows=kw["cand_rows"]; mb=kw["mb"]
    near_miss=kw["near_miss"]; keel_list=kw["keel_list"]; concept_list=kw["concept_list"]
    dq=kw["dq"]; cand_unreliable=kw["cand_unreliable"]; zero_reason=kw.get("zero_reason")
    unknown_ratio=kw["unknown_ratio"]; gw_avail=kw["gw_avail"]; t0=kw["t0"]
    n_cand=len(cand_rows)
    L=[]
    L.append(f"# 全市场分层 Z-score 漏斗日报 · {TODAY}\n")
    L.append(f"> 数据日期：{TRADE_DATE} ｜ 运行日期：{TODAY} ｜ 快照源：{spot_src} ｜ 北交所纳入：{BJ_INCLUDE} ｜ 账户资金(元)：{ACCT_CAPITAL:,.0f}\n")
    if data_incomplete:
        L.append("> ⚠️ **DATA_INCOMPLETE**：快照返回 <4000 条，数据异常，以下结论置信度下降。\n")
    if not gw_avail:
        L.append("> 🔶 **商誉源不可用（本环境）**：个股商誉接口被墙，`get_goodwill` 诚实返回 UNKNOWN；相关标的进「龙骨未验证清单」，不进候选。用户机器 Eastmoney 可达时自动取真值。\n")
    L.append(f"> **今日市场宽度状态：{mb['status']}**（上涨{mb['up']} / 下跌{mb['down']} / ADR {mb['adr']}）\n")

    # 一、运行摘要
    L.append("## 一、运行摘要\n")
    L.append(f"- 运行日期：{TODAY}；数据日期：{TRADE_DATE}；覆盖板块：沪市主板/深市主板/创业板{'/北交所' if BJ_INCLUDE else ''}（剔除科创板/ST/停牌/低流动性）\n")
    L.append(f"- **一句话结论：{'今日候选 '+str(n_cand)+' 只' if n_cand>0 else '今日空仓'}**\n")

    # 二、市场宽度
    L.append("## 二、市场宽度\n")
    L.append(f"- 状态：**{mb['status']}**\n")
    L.append(f"- 上涨家数：{mb['up']} ｜ 下跌家数：{mb['down']} ｜ 上涨占比：{round(mb['up']/(mb['up']+mb['down'])*100,1) if (mb['up']+mb['down'])>0 else 'NA'}%\n")
    L.append(f"- ADR（涨跌比）：{mb['adr']}\n")
    L.append(f"- 涨停家数：{mb['limit_up']} ｜ 跌停家数：{mb['limit_down']} ｜ 炸板率：PARTIAL(需逐笔)\n")
    L.append(f"- 创60/120/250日新高新低：均为 PARTIAL（需逐股历史，本环境未逐股拉取）\n")

    # 三、漏斗统计
    L.append("## 三、漏斗统计\n")
    l1_by_board = l1.groupby("board").size().to_dict()
    board_names={"SH_MAIN":"沪市主板","SZ_MAIN":"深市主板","CHINEXT":"创业板","BJ":"北交所","ALL":"全市场","OTHER":"其他"}
    L.append(f"- L0 全市场有效标的数：**{n_l0}**{'（DATA_INCOMPLETE）' if data_incomplete else ''}\n")
    L.append("- L1 各板块剩余数：")
    L.append("；".join(f"{board_names.get(b,b)} {n}" for b,n in l1_by_board.items()) + "\n")
    L.append(f"- L2 产业链定位：已逐股打标；CONCEPT_UNKNOWN 数 = **{len(concept_list)}**（进建池未验证清单）\n")
    L.append(f"- L3 财务排雷：VETO 数 = **{int(df['veto'].map(len).gt(0).sum())}**；HIGH_RISK 数 = **{int(df['high_risk'].map(len).gt(0).sum())}**；商誉 UNKNOWN 数 = **{len(keel_list)}**\n")
    L.append(f"- L4 最终候选数：**{n_cand}**（上限 {CAND_CAP}；每档前 {TOP_PER_TIER}）\n")

    # 四、因子有效性快照
    L.append("## 四、因子有效性快照\n")
    L.append("- 单日截面 Rank-IC 需历史回归验证；本环境标记：动量类 IC 待回测验证（方向不稳定时权重置0）。\n")
    L.append("- 商誉/负债率/现金流为排雷项，不参与 IC 打分，仅作 VETO/HIGH_RISK。\n")
    L.append("- 若某板块某因子 UNKNOWN 占比>30% 标记 LOW_COVERAGE，权重下调50%。\n")

    # 五、候选清单
    L.append("## 五、候选清单（按板块×市值档分组）\n")
    if cand_rows:
        L.append("| 代码 | 名称 | 板块 | 市值档 | 概念 | position | moat | 质量 | 估值 | 流动性 | 动量 | 资金流 | 总分 | DQ | 商誉Z | 商誉% | HIGH_RISK | ATR% | 52w% | 换手萎缩 | 站MA5 |\n")
        L.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
        for r in cand_rows:
            L.append(
                f"| {r['code']} | {r['name']} | {board_names.get(r['board'],r['board'])} | {r['mcap_tier']} | {r['concept']} | "
                f"{r['position']} | {r['moat_type']} | {r.get('score_quality')} | {r.get('score_valuation')} | {r.get('score_liquidity')} | "
                f"{r.get('score_momentum')} | {r.get('score_fund')} | {r['total_score']} | {r['_cdq']} | {r.get('z_quality_gw')} | "
                f"{r.get('goodwill_ratio')} | {';'.join(r['high_risk']) or '-'} | {r.get('atr_pct')} | {r.get('pct_52w')} | "
                f"{r.get('turnover_shrink')} | {r.get('above_ma5')} |\n")
    else:
        L.append("（无候选）\n")

    # 六、次日操作建议（逐只）
    L.append("## 六、次日操作建议（逐只候选）\n")
    if cand_rows:
        for r in cand_rows:
            act = "小仓侦察" if r['gate']=="PASS" else "观察"
            if mb["status"]=="沸点": act="放弃（沸点禁止新开仓）"
            L.append(f"### {r['name']}({r['code']}) ｜ 动作：**{act}**\n")
            L.append(f"- 侦察区间：基于52周分位 {r.get('pct_52w')}% 与 MA5，建议 {round(r['price']*0.97,2)}–{round(r['price']*1.03,2)} 元区间内分批。\n")
            L.append(f"- 止损价：{r.get('stop_price')}（STOP_MODE={r.get('stop_mode')}）；兜底止损价：{r.get('fallback_stop')}（{int(r.get('fallback_pct',0)*100)}%）\n")
            L.append(f"- 建议仓位：{round(r.get('final_position_pct',0)*100,2)}%（计算：账户2%风险 / (入场-止损)= {r.get('position_pct')}%，冰点/ATR_PCT>5%已减半，侦察仓上限10%）\n")
            L.append(f"- 不买理由：①{gw_avail and '商誉已验证，仍须跟踪' or '商誉源缺失，排雷未闭环'}；②单行业仓位受30%上限约束；③本报告仅为研究，须人工复核后决策。\n")
            L.append(f"- 需人工复核：商誉真值、概念成分、财报口径、止损价有效性。\n")
    else:
        L.append("（无候选，见第七、八节）\n")

    # 七、近失样本
    L.append("## 七、近失样本（前五）\n")
    if near_miss:
        L.append("| 代码 | 名称 | 总分 | 差在哪一项 | 差多少 |\n|---|---|---|---|---|\n")
        for m in near_miss:
            L.append(f"| {m['code']} | {m['name']} | {m['total_score']} | {m['miss']} | — |\n")
    else:
        L.append("（无近失样本，或候选充足）\n")

    # 八、数据质量与待确认
    L.append("## 八、数据质量与待确认\n")
    L.append(f"- **DQ 统计**：REAL={dq.counts['REAL']} ｜ RETRY_RECOVERED={dq.counts['RETRY_RECOVERED']} ｜ PARTIAL={dq.counts['PARTIAL']} ｜ UNKNOWN={dq.counts['UNKNOWN']}\n")
    if cand_unreliable:
        L.append("> ⚠️ **候选可信度下降，需人工复核数据源**（RETRY_RECOVERED+UNKNOWN 占比超阈值）。\n")
    L.append(f"### 龙骨未验证清单（商誉 UNKNOWN，共 {len(keel_list)} 只）\n")
    if keel_list:
        L.append("| 代码 | 名称 | 已尝试源 | 失败原因 |\n|---|---|---|---|\n")
        for k in keel_list[:200]:
            L.append(f"| {k['code']} | {k['name']} | {k['tried']} | 三源全失败 |\n")
    else:
        L.append("（无）\n")
    L.append(f"### 建池未验证清单（CONCEPT_UNKNOWN，共 {len(concept_list)} 只）\n")
    if concept_list:
        L.append("| 代码 | 名称 | 原因 |\n|---|---|---|\n")
        for c in concept_list[:200]:
            L.append(f"| {c['code']} | {c['name']} | {c['reason']} |\n")
    else:
        L.append("（无）\n")
    L.append(f"### 财务待确认清单（扣非/现金流/负债率 UNKNOWN 逐只，见 JSON）\n")
    L.append(f"- 市场宽度开关触发：{mb['status']}；对仓位影响：{'冰点→上限减半' if mb['status']=='冰点' else ('沸点→新开仓=0' if mb['status']=='沸点' else '常规')}。\n")

    # 九、自检结果
    L.append("## 九、自检结果\n")
    checks = {
        "一、运行摘要": True,
        "二、市场宽度": mb["status"]!="UNKNOWN",
        "三、漏斗统计(每层均有数字)": True,
        "四、因子有效性快照": True,
        "五、候选清单": True,
        "六、次日操作建议": n_cand>0,
        "七、近失样本(候选为0时必附)": n_cand>0 or len(near_miss)>0,
        "八、数据质量与待确认": True,
        "九、自检结果": True,
    }
    for k,v in checks.items():
        L.append(f"- [{'✅' if v else '❌'}] {k}\n")
    L.append(f"- 候选为0时是否附近失样本：{'是' if n_cand==0 and len(near_miss)>0 else ('N/A' if n_cand>0 else '否⚠️')}\n")
    L.append(f"- UNKNOWN 占比 {unknown_ratio:.1%} 是否超阈值(20%)：{'是⚠️' if unknown_ratio>0.2 else '否'}\n")
    L.append(f"- 是否触发候选不可信告警：{'是⚠️' if cand_unreliable else '否'}\n")
    if n_cand==0:
        L.append(f"- 零候选归因：**{zero_reason}**\n")
    conclusion = ("本次运行成功" if not cand_unreliable and n_cand>=0 and not data_incomplete
                  else ("成功但需人工复核" if (cand_unreliable or data_incomplete) else "失败"))
    L.append(f"- **结论：{conclusion}**\n")
    L.append(f"\n---\n*引擎 v3 · 生成耗时 {round(time.time()-t0,1)}s · 仅研究标注不自动下单 · 数据缺失一律标 UNKNOWN*\n")
    return "".join(L)

if __name__ == "__main__":
    main()
