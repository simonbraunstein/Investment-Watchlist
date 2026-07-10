#!/usr/bin/env python3
"""
Investment Watchlist - Daily Data Refresh Script
================================================
Runs automatically via GitHub Actions every weekday at 6am UK time.
Can also be triggered manually from the GitHub Actions tab (Run workflow button).

Data pipeline:
  1. Twelve Data  -> Technical indicators + 12M/6M momentum + volume ratio
  2. FMP          -> Analyst targets, ratings, earnings dates, EPS history
  3. ROIC.ai      -> Quality metrics (ROIC, ROE, margins, FCF, debt ratios)
  4. Score engine -> Composite 0-100 score (coefficient-based, market regime aware)
  5. Google Sheets-> Writes all data back to your spreadsheet
  6. Dashboard    -> Generates index.html for GitHub Pages
"""

import os, json, time, math, requests
from functools import wraps

def retry_on_quota(max_retries=5, wait_seconds=30):
    """Decorator that retries a function on Google Sheets 429 quota errors."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if "429" in str(e) or "Quota exceeded" in str(e):
                        wait = wait_seconds * (attempt + 1)
                        print(f"  Rate limit hit, waiting {wait}s before retry {attempt+1}/{max_retries}...", flush=True)
                        time.sleep(wait)
                    else:
                        raise
            raise Exception(f"Max retries exceeded for {func.__name__}")
        return wrapper
    return decorator

# REFRESH_MODE controls what data is fetched:
#   "daily"   = technicals + analyst + scores (fast, ~20 mins)
#   "quality" = quality metrics only via ROIC.ai (~110 mins)
#   unset     = everything (original behaviour, may timeout)
REFRESH_MODE = os.environ.get("REFRESH_MODE", "all")
from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials

# ── Environment variables (set as GitHub Secrets) ──────────────────────────────
FMP_KEY  = os.environ["FMP_API_KEY"]
TD_KEY   = os.environ["TWELVEDATA_API_KEY"]
ROIC_KEY = os.environ["ROIC_API_KEY"]
SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
SA_JSON  = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

FMP_BASE  = "https://financialmodelingprep.com"
TD_BASE   = "https://api.twelvedata.com"
ROIC_BASE = "https://api.roic.ai/v1"

SHEET_ID_CONST    = "1I3exhLlocMvFFlnzv2JYoxcxF5NBXg2RIhqQTcWifWI"
PAGES_URL         = "https://simonbraunstein.github.io/Investment-Watchlist"
GOOGLE_SHEET_URL  = f"https://docs.google.com/spreadsheets/d/{SHEET_ID_CONST}"

# Sheet tab names - must match exactly
TAB_PRICES  = "📈 Live Prices"
TAB_ANALYST = "🎯 Analyst & Ratings"
TAB_TECH    = "📊 Technicals"
TAB_SCORES  = "🏆 Score Breakdown"

# Column positions (1-indexed)
LP_CAT=2; LP_TICKER=3; LP_COMPANY=4; LP_PRICE=6
LP_HIGH52=10; LP_LOW52=11; LP_PE=13; LP_BETA=14; LP_MKTCAP=15
LP_ATH=16; LP_RULE40=17; LP_REGIME=18; LP_BAND=19; LP_COMPOSITE=20

TC_SMA20=5;  TC_SMA50=6;   TC_SMA200=7; TC_EMA12=8; TC_EMA26=9; TC_RSI=10
TC_MACD=11;  TC_MACD_SIG=12; TC_MACD_HIST=13
TC_BB_UP=14; TC_BB_MID=15; TC_BB_LOW=16; TC_ADX=17; TC_ATR=18
TC_RET12=20; TC_RET6=21;   TC_VOL=22;   TC_RS_SPY=23
TC_ROIC=25;  TC_ROE=26;    TC_GM=27;    TC_EBITDA_M=28; TC_NET_M=29
TC_FCF=30;   TC_DEBT_EBITDA=31; TC_INT_COV=32
TC_FWD_PE=33; TC_EV_EBITDA=34; TC_PEGY=35
TC_SCORE_MOM=36; TC_SCORE_QUAL=37; TC_SCORE_EARN=38
TC_SCORE_ANAL=39; TC_SCORE_RS=40; TC_SCORE_VAL=41; TC_SCORE_VOL=42; TC_RULE40=43

AN_STRONG_BUY=11; AN_BUY=12; AN_HOLD=13; AN_SELL=14; AN_STRONG_SELL=15
AN_CONSENSUS=16; AN_EPS_ACT=17; AN_EPS_EST=18; AN_STREAK=20
AN_NEXT_EARN=21; AN_DAYS_EARN=22

# ── Utility functions ──────────────────────────────────────────────────────────
def safe_float(val, default=None):
    try:
        f = float(val)
        return f if not math.isnan(f) else default
    except (TypeError, ValueError):
        return default

def col_letter(n):
    """Convert 1-indexed column number to letter(s). e.g. 1->A, 27->AA"""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result

def fmp_get(endpoint, params=None):
    p = dict(params or {})
    p["apikey"] = FMP_KEY
    for attempt in range(3):
        try:
            r = requests.get(f"{FMP_BASE}{endpoint}", params=p, timeout=20)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            if attempt == 2:
                print(f"  FMP error {endpoint}: {e}")
        time.sleep(1)
    return []

def td_get(endpoint, params=None):
    p = dict(params or {})
    p["apikey"] = TD_KEY
    for attempt in range(3):
        try:
            r = requests.get(f"{TD_BASE}/{endpoint}", params=p, timeout=20)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            if attempt == 2:
                print(f"  TD error {endpoint}: {e}")
        time.sleep(0.5)
    return {}

def roic_get(path):
    for attempt in range(3):
        try:
            r = requests.get(f"{ROIC_BASE}{path}",
                             headers={"x-api-key": ROIC_KEY}, timeout=20)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            if attempt == 2:
                print(f"  ROIC error {path}: {e}")
        time.sleep(1)
    return []

# ── Google Sheets connection ───────────────────────────────────────────────────
def connect_sheets():
    import socket
    socket.setdefaulttimeout(30)  # 30 second timeout on all connections
    sa_info = json.loads(SA_JSON)
    scopes  = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds  = Credentials.from_service_account_info(sa_info, scopes=scopes)
    # Use requests session with timeout
    import google.auth.transport.requests
    authed_session = google.auth.transport.requests.AuthorizedSession(creds)
    authed_session.timeout = 30
    client = gspread.Client(auth=creds, session=authed_session)
    client.timeout = 30
    return client.open_by_key(SHEET_ID)

# ── Read tickers ───────────────────────────────────────────────────────────────
def get_tickers(ws_prices):
    all_vals = ws_prices.get_all_values()
    tickers  = []
    for i, row in enumerate(all_vals):
        if i < 4:
            continue
        if len(row) < LP_TICKER:
            continue
        raw = str(row[LP_TICKER - 1]).strip()
        if not raw or "TOTAL" in raw or raw == "—":
            continue
        # Strip emoji prefixes to get clean ticker
        clean = raw
        for prefix in ["🌟 ", "🏦 ", "⚠️ ", "⛔ "]:
            clean = clean.replace(prefix, "")
        clean = clean.strip()
        if not clean:
            continue
        # Skip non-US tickers (contain colon or dot)
        if ":" in clean or "." in clean:
            continue
        # Skip closed positions
        if raw.startswith("⛔"):
            continue
        tickers.append({
            "row":       i + 1,
            "ticker":    clean,
            "raw":       raw,
            "category":  str(row[LP_CAT - 1]).strip() if len(row) >= LP_CAT else "",
            "company":   str(row[LP_COMPANY - 1]).strip() if len(row) >= LP_COMPANY else "",
            "holding":   "🏦" in raw,
            "conviction":"🌟" in raw,
        })
    print(f"  Found {len(tickers)} US tickers to process")
    return tickers

# ── Fetch SPY regime ───────────────────────────────────────────────────────────
def fetch_spy_regime():
    print("  Fetching SPY data for market regime...")
    data = td_get("time_series", {"symbol": "SPY", "interval": "1day", "outputsize": 253})
    vals = data.get("values", [])
    spy_ret12 = 0.0
    regime    = "BULL"
    if len(vals) >= 252:
        latest   = safe_float(vals[0].get("close"),   1)
        ago12m   = safe_float(vals[251].get("close"),  1)
        spy_ret12 = (latest - ago12m) / ago12m
        closes200 = [safe_float(v.get("close"), 0) for v in vals[:200]]
        sma200    = sum(closes200) / 200
        regime    = "BULL" if latest > sma200 else "BEAR"
    print(f"  SPY 12M: {spy_ret12*100:.1f}% | Regime: {regime}")
    return spy_ret12, regime

# ── Fetch technicals (Twelve Data, batched) ────────────────────────────────────
def fetch_technicals(tickers):
    print(f"  Fetching technical indicators for {len(tickers)} stocks (batched)...")
    BATCH    = 8
    symbols  = [t["ticker"] for t in tickers]
    results  = {s: {} for s in symbols}

    from concurrent.futures import ThreadPoolExecutor, as_completed as asc

    def batch_call(endpoint, params, val_key, res_key):
        for i in range(0, len(symbols), BATCH):
            batch = symbols[i:i+BATCH]
            syms  = ",".join(batch)
            p     = {"symbol": syms, "interval": "1day", "outputsize": 1, **params}
            data  = td_get(endpoint, p)
            for sym in batch:
                d    = data.get(sym, data) if len(batch) > 1 else data
                vals = d.get("values", [])
                if vals:
                    v = safe_float(vals[0].get(val_key))
                    if v is not None:
                        results[sym][res_key] = v
            time.sleep(0.2)

    # Run all indicator batch calls in parallel
    indicator_calls = [
        ("rsi",   {"time_period": 14},  "rsi",  "rsi"),
        ("sma",   {"time_period": 20},  "sma",  "sma20"),
        ("sma",   {"time_period": 50},  "sma",  "sma50"),
        ("sma",   {"time_period": 200}, "sma",  "sma200"),
        ("ema",   {"time_period": 12},  "ema",  "ema12"),
        ("ema",   {"time_period": 26},  "ema",  "ema26"),
        ("adx",   {"time_period": 14},  "adx",  "adx"),
        ("atr",   {"time_period": 14},  "atr",  "atr"),
    ]
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(batch_call, ep, p, vk, rk) for ep,p,vk,rk in indicator_calls]
        for f in asc(futs): f.result()

    def fetch_macd():
        for i in range(0, len(symbols), BATCH):
            batch = symbols[i:i+BATCH]
            syms  = ",".join(batch)
            data  = td_get("macd", {"symbol": syms, "interval": "1day", "outputsize": 1,
                                    "fast_period": 12, "slow_period": 26, "signal_period": 9})
            for sym in batch:
                d    = data.get(sym, data) if len(batch) > 1 else data
                vals = d.get("values", [])
                if vals:
                    results[sym]["macd"]      = safe_float(vals[0].get("macd"))
                    results[sym]["macd_sig"]  = safe_float(vals[0].get("macd_signal"))
                    results[sym]["macd_hist"] = safe_float(vals[0].get("macd_hist"))
            time.sleep(0.2)

    def fetch_bbands():
        for i in range(0, len(symbols), BATCH):
            batch = symbols[i:i+BATCH]
            syms  = ",".join(batch)
            data  = td_get("bbands", {"symbol": syms, "interval": "1day", "outputsize": 1,
                                      "time_period": 20, "sd": 2})
            for sym in batch:
                d    = data.get(sym, data) if len(batch) > 1 else data
                vals = d.get("values", [])
                if vals:
                    results[sym]["bb_upper"]  = safe_float(vals[0].get("upper_band"))
                    results[sym]["bb_middle"] = safe_float(vals[0].get("middle_band"))
                    results[sym]["bb_lower"]  = safe_float(vals[0].get("lower_band"))
            time.sleep(0.2)

    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = [ex.submit(fetch_macd), ex.submit(fetch_bbands)]
        for f in asc(futs): f.result()

    # 12M/6M returns and volume ratio (individual calls - needs history)
    print(f"  Fetching price history for momentum + volume...")
    for sym in symbols:
        data = td_get("time_series", {"symbol": sym, "interval": "1day", "outputsize": 253})
        vals = data.get("values", [])
        if len(vals) >= 126:
            latest   = safe_float(vals[0].get("close"))
            ago6m    = safe_float(vals[125].get("close"))
            ago12m   = safe_float(vals[min(251, len(vals)-1)].get("close"))
            if latest and ago6m:
                results[sym]["ret6m"]  = (latest - ago6m)  / ago6m
            if latest and ago12m:
                results[sym]["ret12m"] = (latest - ago12m) / ago12m
            vols    = [safe_float(v.get("volume"), 0) for v in vals[:20]]
            vol_avg = sum(vols) / len(vols) if vols else 0
            vol_now = safe_float(vals[0].get("volume"), 0)
            if vol_avg > 0:
                results[sym]["vol_ratio"] = vol_now / vol_avg
        time.sleep(0.3)

    print(f"  Technicals complete for {len(results)} stocks")
    return results

# ── Fetch analyst data (FMP) ───────────────────────────────────────────────────
def fetch_one_analyst(sym):
    """Fetch all analyst data for a single ticker - runs in thread pool."""
    result = {}
    try:
        pt = fmp_get("/stable/price-target-summary", {"symbol": sym})
        if pt:
            result["target_avg"]  = safe_float(pt[0].get("targetConsensus"))
            result["target_high"] = safe_float(pt[0].get("targetHigh"))
            result["target_low"]  = safe_float(pt[0].get("targetLow"))
    except Exception: pass

    try:
        gr = fmp_get("/stable/grades-summary", {"symbol": sym})
        if gr:
            g  = gr[0]
            sB = int(g.get("strongBuy",  0) or 0)
            b  = int(g.get("buy",        0) or 0)
            h  = int(g.get("hold",       0) or 0)
            s  = int(g.get("sell",       0) or 0)
            sS = int(g.get("strongSell", 0) or 0)
            result.update({"strong_buy": sB, "buy": b, "hold": h,
                           "sell": s, "strong_sell": sS})
            tot  = sB + b + h + s + sS
            bull = (sB + b) / tot if tot > 0 else 0
            bear = (s + sS) / tot if tot > 0 else 0
            result["bull_pct"] = bull
            if   bull >= 0.70: result["consensus"] = "🟢 Strong Buy"
            elif bull >= 0.50: result["consensus"] = "🟩 Buy"
            elif bear >= 0.50: result["consensus"] = "🔴 Sell"
            elif bear >= 0.30: result["consensus"] = "🟥 Weak Sell"
            else:              result["consensus"] = "🟡 Hold"
    except Exception: pass

    try:
        es = fmp_get("/stable/earnings-surprises", {"symbol": sym, "limit": 4})
        if es:
            result["eps_actual"]   = safe_float(es[0].get("actualEarningResult"))
            result["eps_estimate"] = safe_float(es[0].get("estimatedEarning"))
            streak = 0
            for e in es:
                a  = safe_float(e.get("actualEarningResult"))
                e2 = safe_float(e.get("estimatedEarning"))
                if a is not None and e2 is not None and a > e2:
                    streak += 1
                else:
                    break
            result["eps_streak"] = streak
    except Exception: pass

    try:
        ec = fmp_get("/stable/earnings-calendar", {"symbol": sym})
        if ec:
            nd = ec[0].get("date", "")
            result["next_earnings"] = nd
            if nd:
                try:
                    result["days_to_earnings"] = (
                        datetime.strptime(nd, "%Y-%m-%d").date() - date.today()
                    ).days
                except Exception:
                    pass
    except Exception: pass

    return sym, result


def fetch_analyst_data(tickers):
    """Fetch analyst data for all tickers using thread pool (5 concurrent)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    print(f"  Fetching analyst data for {len(tickers)} stocks (FMP, 5 concurrent)...")
    results  = {}
    symbols  = [t["ticker"] for t in tickers]
    done     = 0
    # 5 concurrent threads - stays well within FMP rate limits
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_one_analyst, sym): sym for sym in symbols}
        for future in as_completed(futures):
            try:
                sym, result = future.result()
                results[sym] = result
                done += 1
                if done % 10 == 0 or done == len(symbols):
                    print(f"    [{done}/{len(symbols)}] completed", flush=True)
            except Exception as e:
                sym = futures[future]
                print(f"    Error {sym}: {e}", flush=True)
                results[sym] = {}
    print(f"  Analyst data complete for {len(results)} stocks")
    return results

# ── Fetch quality metrics (ROIC.ai) ───────────────────────────────────────────
def fetch_quality_data(tickers):
    print(f"  Fetching quality metrics for {len(tickers)} stocks (ROIC.ai - 5/min)...")
    results = {}
    for i, t in enumerate(tickers):
        sym    = t["ticker"]
        result = {}
        print(f"    [{i+1}/{len(tickers)}] {sym}")

        prof = roic_get(f"/financial/ratios/profitability?ticker={sym}&period=annual&limit=1")
        if prof:
            p = prof[0]
            result["roic"]     = safe_float(p.get("return_on_inv_capital"))
            result["roe"]      = safe_float(p.get("return_com_eqy"))
            result["gm"]       = safe_float(p.get("gross_margin"))
            result["ebitda_m"] = safe_float(p.get("ebitda_margin"))
            result["net_m"]    = safe_float(p.get("profit_margin"))
        time.sleep(12)

        cred = roic_get(f"/financial/ratios/credit?ticker={sym}&period=annual&limit=1")
        if cred:
            c = cred[0]
            result["debt_ebitda"] = safe_float(c.get("tot_debt_to_ebitda"))
            result["int_cov"]     = safe_float(c.get("ebit_to_int_exp"))
            result["fcf_yield"]   = safe_float(c.get("free_cash_flow_yield"))
        time.sleep(12)

        val = roic_get(f"/financial/ratios/valuation?ticker={sym}&period=annual&limit=1")
        if val:
            v = val[0]
            result["fwd_pe"]     = safe_float(v.get("pe_ratio"))
            result["ev_ebitda"]  = safe_float(v.get("ev_to_ebitda"))
            result["rev_growth"] = safe_float(v.get("revenue_growth"))
        time.sleep(12)

        # Rule of 40 = Revenue Growth % + Gross Margin %
        gm  = result.get("gm")
        rg  = result.get("rev_growth")
        if gm is not None and rg is not None:
            result["rule40"] = round(gm + rg, 1)

        results[sym] = result

    print(f"  Quality data complete")
    return results

# ── Composite scoring ──────────────────────────────────────────────────────────
def compute_score(tech, analyst, quality, price, spy_ret12, regime):
    bear_mult = 0.5 if regime == "BEAR" else 1.0

    # MOMENTUM (32 pts max — halved in bear market)
    r12 = tech.get("ret12m")
    m1  = min(15, max(0, 15*(max(-0.30, min(0.50, r12))+0.30)/0.80)) if r12 is not None else 0
    r6  = tech.get("ret6m")
    m2  = min(10, max(0, 10*(max(-0.20, min(0.40, r6))+0.20)/0.60))  if r6  is not None else 0
    p       = price or 0
    sma50   = tech.get("sma50",  0) or 0
    sma200  = tech.get("sma200", 0) or 0
    m3      = 5 if p>sma50>sma200>0 else (3 if p>sma200>0 else (2 if sma50>sma200>0 else 0))
    macd    = tech.get("macd");   msig = tech.get("macd_sig"); mhist = tech.get("macd_hist")
    m4      = 2 if (macd and msig and macd>msig and mhist and mhist>0) else (1 if (macd and msig and macd>msig) else 0)
    momentum = (m1 + m2 + m3 + m4) * bear_mult

    # QUALITY (22 pts)
    roic = quality.get("roic")
    q1   = min(8, max(0, 8 * max(0, roic) / 30)) if roic is not None else 0
    gm   = quality.get("gm")
    q2   = min(7, max(0, 7 * max(0, gm)   / 60)) if gm   is not None else 0
    fcf  = quality.get("fcf_yield")
    q3   = min(4, 4 * fcf / 5)  if (fcf is not None and fcf > 0) else 0
    de   = quality.get("debt_ebitda")
    q4   = max(0, 3 * (1 - de / 4)) if de is not None else 1.5
    quality_score = q1 + q2 + q3 + q4

    # EARNINGS (13 pts)
    ea   = analyst.get("eps_actual");   ee = analyst.get("eps_estimate")
    e1   = 0
    if ea is not None and ee is not None and ee != 0:
        surp = (ea - ee) / abs(ee) * 100
        e1   = min(7, 7 * min(surp, 20) / 20) if surp > 0 else 0
    streak = analyst.get("eps_streak", 0) or 0
    e2   = min(3, 3 * streak / 4)
    r40  = quality.get("rule40")
    e3   = 3 if (r40 and r40 >= 40) else (1.5 if (r40 and r40 >= 20) else 0)
    earnings = e1 + e2 + e3

    # ANALYST CONSENSUS (13 pts)
    avg_tgt = analyst.get("target_avg")
    a1 = min(9, max(0, 9 * (avg_tgt - p) / p / 0.40)) if (avg_tgt and p and p > 0) else 0
    bull_pct = analyst.get("bull_pct", 0) or 0
    a2 = 4 if bull_pct >= 0.70 else (2.5 if bull_pct >= 0.50 else 0)
    analyst_score = a1 + a2

    # RELATIVE STRENGTH (10 pts)
    rs   = (r12 - spy_ret12) if r12 is not None else None
    rs1  = min(7, max(0, 7*(max(-0.20, min(0.30, rs))+0.20)/0.50)) if rs is not None else 0
    rel_str = rs1

    # VALUE / PEGY (6 pts)
    pegy = quality.get("pegy")
    v1   = min(6, max(0, 6*(3.0 - min(pegy, 4))/3.0)) if (pegy and pegy > 0) else 0

    # VOLUME CONFIRMATION (4 pts)
    vr  = tech.get("vol_ratio")
    vc1 = 4 if (vr and vr > 1.5) else (2 if (vr and vr > 0.8) else 1)

    composite = round(min(100, max(0,
        momentum + quality_score + earnings + analyst_score + rel_str + v1 + vc1
    )), 1)

    band = ("🚀 EXCEPTIONAL" if composite >= 80 else
            "🟢 STRONG BUY"  if composite >= 65 else
            "🟩 POSITIVE"    if composite >= 50 else
            "🟡 NEUTRAL"     if composite >= 35 else
            "🟥 WEAK"        if composite >= 20 else
            "🔴 POOR")

    return {
        "composite": composite, "band": band,
        "momentum":  round(momentum,      1),
        "quality":   round(quality_score, 1),
        "earnings":  round(earnings,      1),
        "analyst":   round(analyst_score, 1),
        "rel_str":   round(rel_str,       1),
        "value":     round(v1,            1),
        "volume":    round(vc1,           1),
        "rule40":    r40,
        "rs_spy":    round((rs or 0) * 100, 1),
    }

# ── Write to Google Sheets (batch updates) ────────────────────────────────────
def write_to_sheets(wb, tickers, tech_res, anal_res, qual_res, scores, spy_ret12, regime):
    # Wait 65 seconds for Google Sheets read quota to reset before writing
    # (quota is 60 reads/min per user — parallel API calls may have used it up)
    print("  Waiting 65 seconds for Google Sheets quota to reset...", flush=True)
    time.sleep(65)
    print("  Opening worksheets...", flush=True)
    all_sheets = wb.worksheets()
    sheet_map  = {ws.title: ws for ws in all_sheets}
    ws_p = sheet_map.get(TAB_PRICES)
    ws_t = sheet_map.get(TAB_TECH)
    ws_a = sheet_map.get(TAB_ANALYST)
    ws_s = sheet_map.get(TAB_SCORES)

    if not all([ws_p, ws_t, ws_a, ws_s]):
        missing = [n for n,w in [(TAB_PRICES,ws_p),(TAB_TECH,ws_t),(TAB_ANALYST,ws_a),(TAB_SCORES,ws_s)] if not w]
        raise Exception(f"Missing worksheets: {missing}")

    regime_label = "✅ BULL" if regime == "BULL" else "⚠️ BEAR"

    p_updates = []
    t_updates = []
    a_updates = []

    def pu(col, row, val):
        if val is not None:
            p_updates.append({"range": f"{col_letter(col)}{row}", "values": [[val]]})

    def tu(col, row, val):
        if val is not None:
            v = round(val, 4) if isinstance(val, float) else val
            t_updates.append({"range": f"{col_letter(col)}{row}", "values": [[v]]})

    def au(col, row, val):
        if val is not None:
            a_updates.append({"range": f"{col_letter(col)}{row}", "values": [[val]]})

    for t in tickers:
        row  = t["row"]
        sym  = t["ticker"]
        tech = tech_res.get(sym, {})
        anal = anal_res.get(sym, {})
        qual = qual_res.get(sym, {})
        sc   = scores.get(sym, {})

        r12  = tech.get("ret12m")
        r6   = tech.get("ret6m")
        vr   = tech.get("vol_ratio")
        rs   = ((r12 or 0) - spy_ret12) * 100

        # Live Prices
        pu(LP_RULE40,    row, sc.get("rule40"))
        pu(LP_REGIME,    row, regime_label)
        pu(LP_BAND,      row, sc.get("band"))
        pu(LP_COMPOSITE, row, sc.get("composite"))

        # Technicals
        tu(TC_SMA20,   row, tech.get("sma20"));   tu(TC_SMA50,   row, tech.get("sma50"))
        tu(TC_SMA200,  row, tech.get("sma200"));  tu(TC_EMA12,   row, tech.get("ema12"))
        tu(TC_EMA26,   row, tech.get("ema26"));   tu(TC_RSI,     row, tech.get("rsi"))
        tu(TC_MACD,    row, tech.get("macd"));    tu(TC_MACD_SIG,row, tech.get("macd_sig"))
        tu(TC_MACD_HIST,row,tech.get("macd_hist"))
        tu(TC_BB_UP,   row, tech.get("bb_upper")); tu(TC_BB_MID, row, tech.get("bb_middle"))
        tu(TC_BB_LOW,  row, tech.get("bb_lower")); tu(TC_ADX,    row, tech.get("adx"))
        tu(TC_ATR,     row, tech.get("atr"))
        if r12 is not None: tu(TC_RET12,   row, round(r12 * 100, 1))
        if r6  is not None: tu(TC_RET6,    row, round(r6  * 100, 1))
        if vr  is not None: tu(TC_VOL,     row, round(vr, 3))
        tu(TC_RS_SPY,  row, round(rs, 1))
        tu(TC_ROIC,    row, qual.get("roic"));     tu(TC_ROE,       row, qual.get("roe"))
        tu(TC_GM,      row, qual.get("gm"));       tu(TC_EBITDA_M,  row, qual.get("ebitda_m"))
        tu(TC_NET_M,   row, qual.get("net_m"));    tu(TC_FCF,       row, qual.get("fcf_yield"))
        tu(TC_DEBT_EBITDA, row, qual.get("debt_ebitda"))
        tu(TC_INT_COV, row, qual.get("int_cov"));  tu(TC_FWD_PE,    row, qual.get("fwd_pe"))
        tu(TC_EV_EBITDA,row,qual.get("ev_ebitda")); tu(TC_RULE40,   row, qual.get("rule40"))
        tu(TC_SCORE_MOM,  row, sc.get("momentum")); tu(TC_SCORE_QUAL,row, sc.get("quality"))
        tu(TC_SCORE_EARN, row, sc.get("earnings")); tu(TC_SCORE_ANAL,row, sc.get("analyst"))
        tu(TC_SCORE_RS,   row, sc.get("rel_str"));  tu(TC_SCORE_VAL, row, sc.get("value"))
        tu(TC_SCORE_VOL,  row, sc.get("volume"))

        # Analyst
        au(5,  row, anal.get("target_avg"));    au(6,  row, anal.get("target_high"))
        au(7,  row, anal.get("target_low"));    au(AN_STRONG_BUY, row, anal.get("strong_buy"))
        au(AN_BUY,  row, anal.get("buy"));      au(AN_HOLD, row, anal.get("hold"))
        au(AN_SELL, row, anal.get("sell"));     au(AN_STRONG_SELL, row, anal.get("strong_sell"))
        au(AN_CONSENSUS, row, anal.get("consensus"))
        au(AN_EPS_ACT, row, anal.get("eps_actual"))
        au(AN_EPS_EST, row, anal.get("eps_estimate"))
        au(AN_STREAK,  row, anal.get("eps_streak"))
        au(AN_NEXT_EARN, row, anal.get("next_earnings"))
        au(AN_DAYS_EARN, row, anal.get("days_to_earnings"))

    def batch_write(ws, updates, label):
        print(f"  Writing {len(updates)} cells to {label}...")
        for i in range(0, len(updates), 500):
            ws.batch_update(updates[i:i+500])
            time.sleep(1)

    batch_write(ws_p, p_updates, TAB_PRICES)
    time.sleep(10)  # pause between sheets to avoid quota
    batch_write(ws_t, t_updates, TAB_TECH)
    time.sleep(10)
    batch_write(ws_a, a_updates, TAB_ANALYST)
    time.sleep(10)

    # Score Breakdown — sorted by composite
    ticker_map   = {t["ticker"]: t for t in tickers}
    sorted_scores = sorted(
        [(sym, sc) for sym, sc in scores.items()],
        key=lambda x: x[1].get("composite", 0), reverse=True
    )
    score_rows = []
    for rank, (sym, sc) in enumerate(sorted_scores, 1):
        t   = ticker_map.get(sym, {})
        a   = anal_res.get(sym, {})
        q   = qual_res.get(sym, {})
        score_rows.append([
            rank,
            t.get("category", ""),
            t.get("raw", sym),
            t.get("company", ""),
            sc.get("composite", "—"),
            sc.get("band", "—"),
            sc.get("momentum", "—"),
            sc.get("quality",  "—"),
            sc.get("earnings", "—"),
            sc.get("analyst",  "—"),
            sc.get("rel_str",  "—"),
            sc.get("value",    "—"),
            sc.get("volume",   "—"),
            q.get("rule40",    "—"),
            a.get("next_earnings",    "—"),
            a.get("days_to_earnings", "—"),
            regime_label,
            rank
        ])

    if score_rows:
        for attempt in range(5):
            try:
                ws_s.update(range_name=f"A5:R{4+len(score_rows)}", values=score_rows)
                print(f"  Score Breakdown updated: {len(score_rows)} stocks ranked")
                break
            except Exception as e:
                if "429" in str(e) or "Quota" in str(e):
                    wait = 30 * (attempt + 1)
                    print(f"  Rate limit on Score Breakdown, waiting {wait}s...", flush=True)
                    time.sleep(wait)
                else:
                    print(f"  Score Breakdown error: {e}", flush=True)
                    break

    print("  All sheets written successfully")

# ── Generate HTML dashboard ────────────────────────────────────────────────────
def generate_dashboard(tickers, scores, anal_res, qual_res, tech_res,
                        regime, spy_ret12, run_time):
    print("  Generating HTML dashboard...")

    # Build sorted stock list
    sorted_stocks = sorted(
        tickers,
        key=lambda t: scores.get(t["ticker"], {}).get("composite", 0),
        reverse=True
    )

    categories = sorted(set(t.get("category","") for t in tickers if t.get("category")))

    band_colors = {
        "🚀 EXCEPTIONAL": "#00c853",
        "🟢 STRONG BUY":  "#43a047",
        "🟩 POSITIVE":    "#81c784",
        "🟡 NEUTRAL":     "#ffd54f",
        "🟥 WEAK":        "#e57373",
        "🔴 POOR":        "#c62828",
    }

    # Build table rows
    rows = []
    for rank, t in enumerate(sorted_stocks, 1):
        sym  = t["ticker"]
        sc   = scores.get(sym,    {})
        a    = anal_res.get(sym,  {})
        q    = qual_res.get(sym,  {})
        tech = tech_res.get(sym,  {})

        comp  = sc.get("composite", "—")
        band  = sc.get("band", "—")
        bc    = band_colors.get(band, "#aaa")
        r40   = q.get("rule40")
        dte   = a.get("days_to_earnings")
        cons  = a.get("consensus", "—")
        tgt   = a.get("target_avg")
        price = None  # live price comes from Google Sheet

        hold = "🏦 " if t.get("holding")   else ""
        conv = "🌟 " if t.get("conviction") else ""

        r40_bg  = ("#c8e6c9" if isinstance(r40, (int,float)) and r40 >= 40
                   else "#fff9c4" if isinstance(r40, (int,float)) and r40 >= 20
                   else "#ffcdd2" if isinstance(r40, (int,float))
                   else "transparent")
        dte_bg  = ("#ffcdd2" if isinstance(dte, int) and dte <= 5
                   else "#fff9c4" if isinstance(dte, int) and dte <= 20
                   else "transparent")

        r40_str = f"{r40:.0f}" if isinstance(r40, (int,float)) else "—"
        dte_str = str(dte) if dte is not None else "—"

        rows.append(f"""<tr>
  <td>{rank}</td>
  <td style="background:{bc};font-weight:bold">{comp}</td>
  <td style="background:{bc};font-size:11px">{band}</td>
  <td><b>{conv}{hold}{sym}</b></td>
  <td style="font-size:11px;color:#444">{t.get('company','')[:30]}</td>
  <td style="font-size:11px;color:#666">{t.get('category','')[:22]}</td>
  <td>{sc.get('momentum','—')}</td>
  <td>{sc.get('quality','—')}</td>
  <td>{sc.get('earnings','—')}</td>
  <td>{sc.get('analyst','—')}</td>
  <td>{sc.get('rel_str','—')}</td>
  <td>{sc.get('value','—')}</td>
  <td style="background:{r40_bg}">{r40_str}</td>
  <td style="font-size:11px">{cons}</td>
  <td style="background:{dte_bg}">{dte_str}</td>
</tr>""")

    rows_html   = "\n".join(rows)
    regime_label = "✅ BULL MARKET" if regime == "BULL" else "⚠️ BEAR MARKET — Momentum scores halved"
    regime_bg    = "#e8f5e9" if regime == "BULL" else "#ffebee"
    cat_options  = "\n".join(f'<option value="{c}">{c}</option>' for c in categories)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Investment Watchlist — Score Dashboard</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:Arial,sans-serif;font-size:13px;background:#f4f6f8;color:#222}}
.hdr{{background:#1a1a2e;color:#fff;padding:14px 20px}}
.hdr h1{{font-size:18px;margin-bottom:3px}}
.hdr .sub{{font-size:11px;color:#aaa}}
.hdr a{{color:#90caf9}}
.meta{{padding:10px 20px;background:{regime_bg};border-bottom:1px solid #ddd;display:flex;gap:20px;flex-wrap:wrap;align-items:center;font-size:12px}}
.meta b{{font-weight:700}}
.toolbar{{padding:10px 20px;background:#fff;border-bottom:1px solid #eee;display:flex;gap:8px;flex-wrap:wrap;align-items:center}}
.toolbar input,.toolbar select{{padding:7px 10px;border:1px solid #ccc;border-radius:5px;font-size:12px}}
.toolbar input{{width:220px}}
.wrap{{overflow-x:auto;padding:14px 20px}}
table{{border-collapse:collapse;width:100%;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.12)}}
th{{background:#0f3460;color:#fff;padding:7px 9px;font-size:11px;white-space:nowrap;position:sticky;top:0;text-align:center}}
td{{padding:6px 9px;border-bottom:1px solid #f0f0f0;text-align:center;white-space:nowrap}}
tr:hover td{{background:#f8f9ff}}
tr.hidden{{display:none}}
.legend{{padding:10px 20px;display:flex;gap:10px;flex-wrap:wrap;font-size:11px;align-items:center}}
.ld{{display:flex;align-items:center;gap:5px}}
.lc{{width:12px;height:12px;border-radius:2px;display:inline-block}}
.footer{{padding:12px 20px;color:#999;font-size:11px;text-align:center}}
</style>
</head>
<body>
<div class="hdr">
  <h1>📊 Investment Watchlist — Composite Score Dashboard</h1>
  <div class="sub">
    Auto-refreshed weekdays at 6am UK time via GitHub Actions &nbsp;|&nbsp;
    <a href="{GOOGLE_SHEET_URL}" target="_blank">Open Google Sheet ↗</a>
    &nbsp;|&nbsp;
    <a href="https://github.com/simonbraunstein/Investment-Watchlist/actions" target="_blank" style="color:#90caf9">Run manual refresh ↗</a>
  </div>
</div>

<div class="meta">
  <b>🕐 Updated: {run_time} UTC</b>
  <span style="padding:3px 10px;border-radius:4px;background:{regime_bg};border:1px solid #ccc"><b>{regime_label}</b></span>
  <span>SPY 12M: {spy_ret12*100:.1f}%</span>
  <span>{len(tickers)} stocks tracked</span>
</div>

<div class="toolbar">
  <input type="text" id="srch" placeholder="Search ticker or company..." oninput="ft()">
  <select id="catF" onchange="ft()">
    <option value="">All categories</option>
    {cat_options}
  </select>
  <select id="bandF" onchange="ft()">
    <option value="">All signals</option>
    <option value="EXCEPTIONAL">🚀 Exceptional (80+)</option>
    <option value="STRONG BUY">🟢 Strong Buy (65+)</option>
    <option value="POSITIVE">🟩 Positive (50+)</option>
    <option value="NEUTRAL">🟡 Neutral (35+)</option>
    <option value="WEAK">🟥 Weak (20+)</option>
    <option value="POOR">🔴 Poor (&lt;20)</option>
  </select>
  <select id="holdF" onchange="ft()">
    <option value="">All stocks</option>
    <option value="🏦">🏦 Holdings only</option>
    <option value="🌟">🌟 Conviction only</option>
  </select>
</div>

<div class="wrap">
<table id="tbl">
<thead><tr>
  <th>#</th><th>SCORE/100</th><th>SIGNAL</th><th>TICKER</th><th>COMPANY</th>
  <th>CATEGORY</th><th>MOM/32</th><th>QUAL/22</th><th>EARN/13</th>
  <th>ANAL/13</th><th>RS/10</th><th>VAL/6</th>
  <th>RULE 40</th><th>CONSENSUS</th><th>DAYS TO EARN</th>
</tr></thead>
<tbody>
{rows_html}
</tbody>
</table>
</div>

<div class="legend">
  <b>Signals:</b>
  <div class="ld"><span class="lc" style="background:#00c853"></span>80–100 Exceptional</div>
  <div class="ld"><span class="lc" style="background:#43a047"></span>65–79 Strong Buy</div>
  <div class="ld"><span class="lc" style="background:#81c784"></span>50–64 Positive</div>
  <div class="ld"><span class="lc" style="background:#ffd54f"></span>35–49 Neutral</div>
  <div class="ld"><span class="lc" style="background:#e57373"></span>20–34 Weak</div>
  <div class="ld"><span class="lc" style="background:#c62828"></span>0–19 Poor</div>
  &nbsp;
  <b>Rule of 40:</b>
  <div class="ld"><span class="lc" style="background:#c8e6c9"></span>≥40</div>
  <div class="ld"><span class="lc" style="background:#fff9c4"></span>20–39</div>
  <div class="ld"><span class="lc" style="background:#ffcdd2"></span>&lt;20</div>
</div>

<div class="footer">
  Scoring: Momentum(32) + Quality(22) + Earnings(13) + Analyst(13) + Rel.Strength(10) + Value(6) + Volume(4) = 100 &nbsp;|&nbsp;
  Sources: Twelve Data · FMP · ROIC.ai · Google Finance
</div>

<script>
function ft(){{
  const s=document.getElementById('srch').value.toLowerCase();
  const c=document.getElementById('catF').value.toLowerCase();
  const b=document.getElementById('bandF').value.toLowerCase();
  const h=document.getElementById('holdF').value;
  document.querySelectorAll('#tbl tbody tr').forEach(r=>{{
    const tx=r.textContent.toLowerCase();
    r.classList.toggle('hidden',
      !(!s||tx.includes(s)) ||
      !(!c||tx.includes(c)) ||
      !(!b||tx.includes(b)) ||
      !(!h||tx.includes(h))
    );
  }});
}}
</script>
</body>
</html>"""

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html)

    # Save data.json for future use
    data = {
        "updated":   run_time,
        "regime":    regime,
        "spy_ret12": round(spy_ret12 * 100, 1),
        "stocks": [
            {
                "rank":      i+1,
                "ticker":    t["ticker"],
                "company":   t["company"],
                "category":  t["category"],
                "holding":   t["holding"],
                "conviction":t["conviction"],
                **scores.get(t["ticker"], {}),
                "rule40":    qual_res.get(t["ticker"], {}).get("rule40"),
                "consensus": anal_res.get(t["ticker"], {}).get("consensus"),
                "days_to_earnings": anal_res.get(t["ticker"], {}).get("days_to_earnings"),
                "next_earnings":    anal_res.get(t["ticker"], {}).get("next_earnings"),
            }
            for i, t in enumerate(sorted_stocks)
        ]
    }
    with open("data.json", "w") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"  Dashboard generated: {len(sorted_stocks)} stocks")

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    run_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    print(f"\n{'='*60}")
    print(f"Investment Watchlist Refresh — {run_time} UTC")
    print(f"{'='*60}\n")

    import sys
    print("Checking environment variables...", flush=True)
    # Verify all required environment variables are present
    required_vars = ["FMP_API_KEY","TWELVEDATA_API_KEY","ROIC_API_KEY",
                     "GOOGLE_SHEET_ID","GOOGLE_SERVICE_ACCOUNT_JSON"]
    for var in required_vars:
        val = os.environ.get(var,"")
        if not val:
            print(f"ERROR: Missing environment variable: {var}", flush=True)
            sys.exit(1)
        print(f"  {var}: {'*'*8} (length={len(val)})", flush=True)
    print("All environment variables present.", flush=True)

    print("\nSTEP 1: Connecting to Google Sheets...", flush=True)
    # Validate JSON before attempting connection
    print("Validating service account JSON...", flush=True)
    try:
        sa_test = json.loads(SA_JSON)
        print(f"  JSON valid. Type: {sa_test.get('type','unknown')}", flush=True)
        print(f"  Project: {sa_test.get('project_id','unknown')}", flush=True)
        print(f"  Client email: {sa_test.get('client_email','unknown')}", flush=True)
    except json.JSONDecodeError as e:
        print(f"  ERROR: GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON: {e}", flush=True)
        print(f"  Make sure you copied the entire .json file contents into the secret", flush=True)
        sys.exit(1)

    try:
        print("Connecting to Google Sheets...", flush=True)
        wb = connect_sheets()
        print(f"  Connected successfully", flush=True)
        ws_test = wb.worksheet(TAB_PRICES)
        print(f"  Sheet '{TAB_PRICES}' found OK", flush=True)
    except Exception as e:
        print(f"  FAILED to connect to Google Sheets: {e}", flush=True)
        print(f"  Check that:", flush=True)
        print(f"  1. GOOGLE_SERVICE_ACCOUNT_JSON contains valid JSON", flush=True)
        print(f"  2. Service account {sa_test.get('client_email','')} has Editor access", flush=True)
        print(f"  3. Google Sheets API is enabled in Google Cloud Console", flush=True)
        print(f"  4. Sheet ID {SHEET_ID} is correct", flush=True)
        sys.exit(1)
    ws_prices = wb.worksheet(TAB_PRICES)
    tickers   = get_tickers(ws_prices)

    print("\nSTEP 2: Market regime (SPY)...")
    spy_ret12, regime = fetch_spy_regime()

    print("\nSTEP 3: Technical indicators (Twelve Data)...")
    if REFRESH_MODE == "quality":
        print("  SKIPPING technicals (quality mode)")
        tech_res = {}
    else:
        tech_res = fetch_technicals(tickers)

    print("\nSTEP 4: Analyst data (FMP)...")
    if REFRESH_MODE == "quality":
        print("  SKIPPING analyst data (quality mode)")
        anal_res = {}
    else:
        anal_res = fetch_analyst_data(tickers)

    print("\nSTEP 5: Quality metrics (ROIC.ai — ~34 mins)...")
    if REFRESH_MODE == "daily":
        print("  SKIPPING quality data (daily mode — runs separately on Sundays)")
        # Load any previously cached quality data from data.json
        qual_res = {}
        try:
            with open("data.json") as f:
                cached = json.load(f)
            for s in cached.get("stocks", []):
                sym = s.get("ticker","")
                if sym:
                    qual_res[sym] = {
                        "roic":        s.get("q_roic"),
                        "roe":         s.get("q_roe"),
                        "gm":          s.get("q_gm"),
                        "ebitda_m":    s.get("q_ebitda_m"),
                        "net_m":       s.get("q_net_m"),
                        "fcf_yield":   s.get("q_fcf_yield"),
                        "debt_ebitda": s.get("q_debt_ebitda"),
                        "int_cov":     s.get("q_int_cov"),
                        "fwd_pe":      s.get("q_fwd_pe"),
                        "ev_ebitda":   s.get("q_ev_ebitda"),
                        "rev_growth":  s.get("q_rev_growth"),
                        "rule40":      s.get("q_rule40"),
                    }
            print(f"  Loaded cached quality data for {len(qual_res)} stocks from data.json")
        except Exception as e:
            print(f"  No cached quality data available ({e}) — quality scores will be partial")
    elif REFRESH_MODE == "quality":
        qual_res = fetch_quality_data(tickers)
        # In quality-only mode, also load cached tech/analyst for score computation
        print("  Loading cached technicals and analyst data for score computation...")
        tech_res_cached = {}
        anal_res_cached = {}
        try:
            with open("data.json") as f:
                cached = json.load(f)
            for s in cached.get("stocks", []):
                sym = s.get("ticker","")
                if sym:
                    tech_res_cached[sym] = {
                        k.replace("t_",""): v
                        for k,v in s.items() if k.startswith("t_")
                    }
                    anal_res_cached[sym] = {
                        k.replace("a_",""): v
                        for k,v in s.items() if k.startswith("a_")
                    }
        except Exception as e:
            print(f"  Could not load cache: {e}")
        tech_res = tech_res_cached or tech_res
        anal_res = anal_res_cached or anal_res
    else:
        qual_res = fetch_quality_data(tickers)

    print("\nSTEP 6: Computing composite scores...")
    scores = {}
    for t in tickers:
        price = None
        try:
            row_data = ws_prices.row_values(t["row"])
            price = safe_float(row_data[LP_PRICE - 1]) if len(row_data) >= LP_PRICE else None
        except Exception:
            pass
        scores[t["ticker"]] = compute_score(
            tech_res.get(t["ticker"], {}),
            anal_res.get(t["ticker"], {}),
            qual_res.get(t["ticker"], {}),
            price, spy_ret12, regime
        )
    print(f"  Scores computed for {len(scores)} stocks")

    print("\nSTEP 7: Writing to Google Sheets...")
    write_to_sheets(wb, tickers, tech_res, anal_res, qual_res,
                    scores, spy_ret12, regime)

    print("\nSTEP 8: Generating dashboard...")
    generate_dashboard(tickers, scores, anal_res, qual_res, tech_res,
                       regime, spy_ret12, run_time)

    print(f"\n{'='*60}")
    print(f"Refresh complete — {run_time} UTC")
    top5 = sorted(scores.items(), key=lambda x: x[1].get("composite",0), reverse=True)[:5]
    print("Top 5 stocks by composite score:")
    for sym, sc in top5:
        print(f"  {sym}: {sc['composite']}/100 — {sc['band']}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
