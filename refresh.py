#!/usr/bin/env python3
"""
Investment Watchlist - Daily Data Refresh Script (v19)
======================================================
Runs automatically via GitHub Actions every day at 5am UTC (6am UK).
Can also be triggered manually from the GitHub Actions tab (Run workflow button).

v11 ARCHITECTURE CHANGES (fixes the empty-data bugs):
  1. Price history: bulk yfinance download (Yahoo 'chart' endpoint — tolerant of
     GitHub runner IPs), with Twelve Data time_series as per-symbol FALLBACK only,
     correctly throttled to the free tier's 8 credits/min.
     ALL technical indicators (SMA/EMA/RSI/MACD/BBands/ATR/ADX/returns/volume)
     are now computed LOCALLY in pandas — zero indicator API credits needed.
     (v10 burned ~1,850 Twelve Data credits/run vs the 800/day free cap, and the
     API returns HTTP 200 with an error body, so every failure was silent.)
  2. Analyst data: ONE combined Yahoo quoteSummary request per stock
     (financialData + recommendationTrend + earningsHistory + calendarEvents)
     instead of 4 separate requests — 168 calls instead of 672 — sequential with
     exponential backoff on 429.
  3. Google Sheets: cached prices reused in scoring (v10 made 167 read calls in
     Step 6, blowing the 60 reads/min quota). Analyst/Technicals rows are mapped
     by scanning each tab's ticker column rather than assuming Live Prices rows.
  4. ROIC.ai       -> Quality metrics (unchanged; weekly, cached in data.json)
  5. Score engine  -> Composite 0-100 (unchanged)
  6. Dashboard     -> index.html for GitHub Pages (unchanged)
"""

import os, json, time, math, requests
from functools import wraps
from datetime import datetime, date, timedelta
try:
    import yfinance as yf
except ImportError:
    yf = None

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
VERSION = "v19"  # printed at startup so the running version is never ambiguous
# deep = scheduled overnight runs (generous repair budgets, slow is fine)
# fast = manual runs (quick retries only, target <20 min end-to-end)
REFRESH_DEPTH = os.environ.get("REFRESH_DEPTH", "deep").strip().lower()
# Optional: free Finnhub key unlocks an independent analyst-data source for the
# repair stage (ratings + EPS + earnings dates; price targets are premium there)
FINNHUB_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()
REFRESH_MODE = os.environ.get("REFRESH_MODE", "all")
from datetime import datetime, date, timedelta
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
# Risk metrics (v12) — new columns on 📊 Technicals
TC_BETA=45; TC_VOL_ANN=46; TC_MAX_DD=47; TC_SHARPE=48; TC_52W=49

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
            if r.status_code in (401, 402, 403, 404):
                # Not on this plan / bad symbol — retrying won't change that
                return []
        except Exception as e:
            if attempt == 2:
                print(f"  FMP error {endpoint}: {e}")
        time.sleep(1)
    return []

def td_get(endpoint, params=None):
    """Twelve Data GET. IMPORTANT: TD returns HTTP 200 with an error JSON body
    (e.g. {"code":429,...}) when you exceed the 8 credits/min or 800/day free
    limits — so we must inspect the body, not just the HTTP status."""
    p = dict(params or {})
    p["apikey"] = TD_KEY
    for attempt in range(3):
        try:
            r = requests.get(f"{TD_BASE}/{endpoint}", params=p, timeout=20)
            if r.status_code == 200:
                data = r.json()
                # Single-symbol error body
                if isinstance(data, dict) and data.get("status") == "error":
                    code = data.get("code")
                    if code == 429:
                        print(f"  TD rate/credit limit hit on {endpoint} — waiting 62s...", flush=True)
                        time.sleep(62)
                        continue
                    print(f"  TD error {endpoint}: code={code} {str(data.get('message',''))[:120]}", flush=True)
                    return {}
                return data
        except Exception as e:
            if attempt == 2:
                print(f"  TD error {endpoint}: {e}")
        time.sleep(0.5)
    return {}

# ── ROIC.ai client (v13: self-diagnosing) ─────────────────────────────────────
# The v12 quality run completed 168 stocks yet produced zero data, because
# roic_get silently swallowed every non-200 response. We cannot reproduce the
# API's behaviour from CI logs alone, so this client PROBES on its first call:
# it tries each plausible base-URL + auth-scheme combination, locks in the
# first one that returns real data, and prints the status/body of every probe.
# The next run will therefore either work, or say exactly why it can't.
_ROIC = {"base": None, "auth": None, "probed": False,
         "errors_logged": 0, "fail_all": False}
# Endpoint structure verified against https://www.roic.ai/api/docs (July 2026):
# GET https://api.roic.ai/v2/fundamental/ratios/profitability/{ticker}?apikey=KEY
# The old v1 /financial/ratios/...?ticker= paths no longer exist (hence the 404s).
_ROIC_SCHEMES = [
    ("https://api.roic.ai/v2", "param"),    # documented auth style
    ("https://api.roic.ai/v2", "header"),   # defensive backup
]

def _roic_request(base, auth, path):
    headers, params = {}, {}
    if auth == "header":
        headers["x-api-key"] = ROIC_KEY
    else:
        params["apikey"] = ROIC_KEY
    return requests.get(f"{base}{path}", headers=headers, params=params, timeout=25)

def _roic_probe(path):
    print("  Probing ROIC.ai endpoint/auth schemes (first call)...", flush=True)
    for base, auth in _ROIC_SCHEMES:
        try:
            r = _roic_request(base, auth, path)
            body = (r.text or "")[:120].replace("\n", " ")
            print(f"    {base} [{auth}] -> HTTP {r.status_code}: {body}", flush=True)
            if r.status_code == 200:
                j = r.json()
                if j:  # non-empty data = winner
                    _ROIC.update({"base": base, "auth": auth})
                    print(f"    ✅ Locked in {base} with {auth} auth", flush=True)
                    return j if isinstance(j, list) else [j]
        except Exception as e:
            print(f"    {base} [{auth}] -> error: {str(e)[:100]}", flush=True)
        time.sleep(2)
    _ROIC["fail_all"] = True
    print("  ❌ ALL ROIC.ai schemes failed — check the API key, plan, or their "
          "docs for endpoint changes. Falling back to FMP for quality metrics "
          "(best-effort, budget-limited).", flush=True)
    return []

def roic_get(path):
    """ROIC.ai GET with loud failures. Never silently swallows a non-200."""
    if _ROIC["fail_all"]:
        return []
    if not _ROIC["probed"]:
        _ROIC["probed"] = True
        return _roic_probe(path)
    for attempt in range(3):
        try:
            r = _roic_request(_ROIC["base"], _ROIC["auth"], path)
            if r.status_code == 200:
                j = r.json()
                return j if isinstance(j, list) else ([j] if j else [])
            if r.status_code == 429:  # free tier = 5 requests/min
                time.sleep(61)
                continue
            if _ROIC["errors_logged"] < 8:
                _ROIC["errors_logged"] += 1
                print(f"    ROIC HTTP {r.status_code} on {path}: "
                      f"{(r.text or '')[:120]}", flush=True)
            return []
        except Exception as e:
            if attempt == 2:
                print(f"  ROIC error {path}: {e}", flush=True)
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
        # NOTE: Do NOT skip closed positions (⛔) — include them for tracking
        # They will be scored but marked as closed in the dashboard

        # Duplicate guard: the sheet contains 168 rows but only 167 unique
        # tickers ever get scored/written — a duplicate row silently shadows
        # its twin. Keep the first occurrence and say so loudly.
        if any(x["ticker"] == clean for x in tickers):
            print(f"  ⚠️ Duplicate ticker {clean} at sheet row {i+1} — keeping the "
                  f"first occurrence only (delete the duplicate row to silence this)", flush=True)
            continue

        # Cache the live price from col F so we don't need to re-read later
        cached_price = None
        if len(row) >= LP_PRICE:
            cached_price = safe_float(row[LP_PRICE - 1])

        tickers.append({
            "row":       i + 1,
            "ticker":    clean,
            "raw":       raw,
            "category":  str(row[LP_CAT - 1]).strip() if len(row) >= LP_CAT else "",
            "company":   str(row[LP_COMPANY - 1]).strip() if len(row) >= LP_COMPANY else "",
            "holding":   "🏦" in raw,
            "conviction":"🌟" in raw,
            "closed":    raw.startswith("⛔"),
            "price":     cached_price,
        })
    print(f"  Found {len(tickers)} US tickers to process")
    return tickers

# ── Price history + technicals (computed locally — v11) ───────────────────────
#
# v10 requested ~1,850 Twelve Data credits per run against a free-tier cap of
# 800/day (and 8/min), so nearly every indicator call silently returned empty
# and momentum/SMA data never populated (the "score = 1.5" bug).
#
# v11: download raw OHLCV history in bulk from Yahoo's chart endpoint via
# yfinance (this endpoint tolerates GitHub runner IPs, unlike quoteSummary),
# then compute every indicator locally in pandas. Twelve Data is used ONLY as
# a per-symbol fallback for tickers Yahoo fails to return, throttled properly.

def _td_history_fallback(missing_symbols, history):
    """Fetch daily OHLCV from Twelve Data for symbols Yahoo missed.
    Multi-symbol requests cost 1 credit per symbol; free tier = 8 credits/min,
    so we send batches of 8 and wait 62s between batches."""
    import pandas as pd
    if not missing_symbols:
        return
    print(f"  Twelve Data fallback for {len(missing_symbols)} symbols "
          f"(8/min throttle — ~{len(missing_symbols)//8 + 1} min)...", flush=True)
    for i in range(0, len(missing_symbols), 8):
        batch = missing_symbols[i:i+8]
        data  = td_get("time_series", {"symbol": ",".join(batch),
                                       "interval": "1day", "outputsize": 300})
        for sym in batch:
            d    = data.get(sym, data) if len(batch) > 1 else data
            vals = (d or {}).get("values", []) if isinstance(d, dict) else []
            if not vals:
                continue
            rows = []
            for v in reversed(vals):  # TD returns newest-first; we want oldest-first
                rows.append({
                    "datetime": v.get("datetime"),
                    "Open":   safe_float(v.get("open")),
                    "High":   safe_float(v.get("high")),
                    "Low":    safe_float(v.get("low")),
                    "Close":  safe_float(v.get("close")),
                    "Volume": safe_float(v.get("volume"), 0),
                })
            df = pd.DataFrame(rows).set_index("datetime")
            df = df.dropna(subset=["Close"])
            if len(df) < 30:
                continue
            try:
                df.index = pd.to_datetime(df.index)
            except Exception:
                pass
            history[sym] = df
            print(f"    TD fallback OK: {sym} ({len(df)} bars)", flush=True)
        if i + 8 < len(missing_symbols):
            time.sleep(62)  # respect 8 credits/min


def fetch_all_history(tickers):
    """Bulk-download ~15 months of daily OHLCV for all tickers + SPY.
    Returns dict: symbol -> pandas DataFrame (Open/High/Low/Close/Volume,
    oldest-first)."""
    import pandas as pd
    import yfinance as yf

    symbols = [t["ticker"] for t in tickers]
    want    = symbols + ["SPY"]
    history = {}

    print(f"  Bulk-downloading price history for {len(want)} symbols via yfinance...", flush=True)
    CHUNK = 50
    for i in range(0, len(want), CHUNK):
        chunk = want[i:i+CHUNK]
        try:
            df = yf.download(
                tickers=chunk, period="15mo", interval="1d",
                group_by="ticker", auto_adjust=True,
                threads=True, progress=False,
            )
        except Exception as e:
            print(f"    yf.download chunk error: {e}", flush=True)
            df = None
        if df is not None and not df.empty:
            for sym in chunk:
                try:
                    sub = df[sym] if isinstance(df.columns, pd.MultiIndex) else df
                    sub = sub.dropna(subset=["Close"])
                    if len(sub) >= 30:
                        try:
                            sub.index = pd.to_datetime(sub.index).tz_localize(None)
                        except Exception:
                            pass
                        history[sym] = sub
                except Exception:
                    pass
        got = len([s for s in chunk if s in history])
        print(f"    [{min(i+CHUNK, len(want))}/{len(want)}] chunk done — {got}/{len(chunk)} symbols OK", flush=True)
        time.sleep(2)

    missing = [s for s in want if s not in history]
    if missing:
        print(f"  {len(missing)} symbols missing from Yahoo: {missing[:10]}", flush=True)
        _td_history_fallback(missing, history)

    still_missing = [s for s in want if s not in history]
    if still_missing:
        print(f"  ⚠️ No history available for {len(still_missing)} symbols: {still_missing[:10]}", flush=True)
    print(f"  History loaded for {len(history)}/{len(want)} symbols", flush=True)
    return history


def compute_regime_from_history(history):
    spy_ret12, regime = 0.0, "BULL"
    spy = history.get("SPY")
    if spy is not None and len(spy) >= 200:
        close  = spy["Close"]
        latest = float(close.iloc[-1])
        if len(close) >= 252:
            spy_ret12 = latest / float(close.iloc[-252]) - 1
        else:
            spy_ret12 = latest / float(close.iloc[0]) - 1
        sma200 = float(close.iloc[-200:].mean())
        regime = "BULL" if latest > sma200 else "BEAR"
    else:
        print("  ⚠️ SPY history unavailable — defaulting to BULL regime, RS scores will be off", flush=True)
    print(f"  SPY 12M: {spy_ret12*100:.1f}% | Regime: {regime}")
    return spy_ret12, regime


def compute_technicals(history, tickers):
    """Compute all indicators locally from OHLCV history (no API calls)."""
    import pandas as pd
    print(f"  Computing technical indicators locally for {len(tickers)} stocks...", flush=True)
    results = {}
    n_full  = 0

    # Pre-compute SPY daily returns once (for beta)
    spy_rets = None
    spy_df = history.get("SPY")
    if spy_df is not None and len(spy_df) >= 100:
        spy_rets = spy_df["Close"].pct_change().dropna()

    RISK_FREE = 0.04  # assumed annual risk-free rate for Sharpe (documented)

    for t in tickers:
        sym = t["ticker"]
        results[sym] = {}
        df = history.get(sym)
        if df is None or len(df) < 30:
            continue
        try:
            close, high, low = df["Close"], df["High"], df["Low"]
            vol = df["Volume"] if "Volume" in df else None
            r   = results[sym]
            last = float(close.iloc[-1])
            r["last_close"] = last

            # SMAs / EMAs
            if len(close) >= 20:  r["sma20"]  = float(close.iloc[-20:].mean())
            if len(close) >= 50:  r["sma50"]  = float(close.iloc[-50:].mean())
            if len(close) >= 200: r["sma200"] = float(close.iloc[-200:].mean())
            ema12 = close.ewm(span=12, adjust=False).mean()
            ema26 = close.ewm(span=26, adjust=False).mean()
            r["ema12"], r["ema26"] = float(ema12.iloc[-1]), float(ema26.iloc[-1])

            # RSI (Wilder, 14) — zero-average-loss convention: RSI=100 when
            # only gains, 50 when flat (avoids NaN from division by zero)
            delta = close.diff()
            gain  = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
            loss  = (-delta.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
            avg_g, avg_l = float(gain.iloc[-1]), float(loss.iloc[-1])
            if avg_l == 0:
                r["rsi"] = 100.0 if avg_g > 0 else 50.0
            else:
                r["rsi"] = 100 - 100 / (1 + avg_g / avg_l)

            # MACD (12,26,9)
            macd_line = ema12 - ema26
            macd_sig  = macd_line.ewm(span=9, adjust=False).mean()
            r["macd"]      = float(macd_line.iloc[-1])
            r["macd_sig"]  = float(macd_sig.iloc[-1])
            r["macd_hist"] = r["macd"] - r["macd_sig"]

            # Bollinger (20, 2)
            if len(close) >= 20:
                mid = float(close.iloc[-20:].mean())
                sd  = float(close.iloc[-20:].std(ddof=0))
                r["bb_middle"], r["bb_upper"], r["bb_lower"] = mid, mid + 2*sd, mid - 2*sd

            # ATR (Wilder, 14)
            prev_close = close.shift(1)
            tr = (high - low).combine((high - prev_close).abs(), max).combine(
                 (low - prev_close).abs(), max)
            atr = tr.ewm(alpha=1/14, adjust=False).mean()
            r["atr"] = float(atr.iloc[-1])

            # ADX (Wilder, 14)
            up_move   = high.diff()
            down_move = -low.diff()
            plus_dm   = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
            minus_dm  = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
            atr_s     = tr.ewm(alpha=1/14, adjust=False).mean()
            plus_di   = 100 * plus_dm.ewm(alpha=1/14, adjust=False).mean() / atr_s
            minus_di  = 100 * minus_dm.ewm(alpha=1/14, adjust=False).mean() / atr_s
            dx        = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
            adx       = dx.ewm(alpha=1/14, adjust=False).mean()
            if not math.isnan(float(adx.iloc[-1])):
                r["adx"] = float(adx.iloc[-1])

            # Momentum returns
            if len(close) >= 252:
                r["ret12m"] = last / float(close.iloc[-252]) - 1
            elif len(close) >= 200:
                r["ret12m"] = last / float(close.iloc[0]) - 1
            if len(close) >= 126:
                r["ret6m"]  = last / float(close.iloc[-126]) - 1

            # Volume ratio (today vs 20-day avg)
            if vol is not None and len(vol) >= 20:
                v20 = float(vol.iloc[-20:].mean())
                if v20 > 0:
                    r["vol_ratio"] = float(vol.iloc[-1]) / v20

            # ── Risk metrics (v12) ────────────────────────────────────────
            rets = close.pct_change().dropna()

            # Annualised volatility (%)
            if len(rets) >= 60:
                r["vol_ann"] = float(rets.std(ddof=0)) * math.sqrt(252) * 100

            # Beta vs SPY (date-aligned daily returns, min 100 overlapping days)
            if spy_rets is not None and len(rets) >= 100:
                try:
                    joined = pd.concat([rets, spy_rets], axis=1, join="inner").dropna()
                    if len(joined) >= 100:
                        s_r, m_r = joined.iloc[:, 0], joined.iloc[:, 1]
                        var_m = float(m_r.var())  # ddof=1, consistent with .cov()
                        if var_m > 0:
                            r["beta"] = float(s_r.cov(m_r)) / var_m
                except Exception:
                    pass

            # Max drawdown over the loaded window (%)
            dd = close / close.cummax() - 1
            _mdd = float(dd.min())
            if not math.isnan(_mdd):
                r["max_dd"] = _mdd * 100

            # Sharpe (12M return minus assumed 4% risk-free, over annualised vol)
            if "ret12m" in r and r.get("vol_ann"):
                r["sharpe"] = (r["ret12m"] - RISK_FREE) / (r["vol_ann"] / 100)

            # % from 52-week high
            if len(high) >= 60:
                hi52 = float(high.iloc[-min(252, len(high)):].max())
                if hi52 > 0:
                    r["pct_52w"] = (last / hi52 - 1) * 100

            if "ret12m" in r and "sma200" in r:
                n_full += 1
        except Exception as e:
            print(f"    Indicator computation failed for {sym}: {e}", flush=True)

    print(f"  Technicals computed: {n_full}/{len(tickers)} stocks with full momentum data", flush=True)
    return results

# ── Finnhub (optional, free tier: 60 calls/min) ───────────────────────────────
# Independent analyst source for the repair stage. Free tier covers ratings
# (recommendation trends) and EPS surprises; price targets are premium there,
# so targets still come from Yahoo/FMP/cache. Activates only when the
# FINNHUB_API_KEY secret is configured; probe-gated like the other fallbacks.
_FINNHUB_STATS = {"probed": False, "healthy": False, "disabled": False,
                  "last_http": None, "hard_errors": 0, "errors_logged": 0}
# Names virtually guaranteed to have analyst coverage — used to test whether
# the SOURCE works, so obscure small-caps can't be mistaken for a dead API.
_LIQUID_PROBES = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "AMD",
                  "TSM", "AVGO", "CSCO", "INTC", "QCOM", "MU", "CRWD", "DDOG"]

def _finnhub_get(path, params):
    if not FINNHUB_KEY:
        return None
    p = dict(params or {}); p["token"] = FINNHUB_KEY
    for attempt in range(2):
        try:
            r = requests.get(f"https://finnhub.io/api/v1{path}", params=p, timeout=15)
            _FINNHUB_STATS["last_http"] = r.status_code
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429 and attempt == 0:
                time.sleep(31)
                continue
            if _FINNHUB_STATS["errors_logged"] < 6:
                _FINNHUB_STATS["errors_logged"] += 1
                print(f"    Finnhub HTTP {r.status_code} on {path}: "
                      f"{(r.text or '')[:100]}", flush=True)
            return None
        except Exception as e:
            _FINNHUB_STATS["last_http"] = "exception"
            if _FINNHUB_STATS["errors_logged"] < 6:
                _FINNHUB_STATS["errors_logged"] += 1
                print(f"    Finnhub error on {path}: {str(e)[:100]}", flush=True)
            return None
    return None

_FINNHUB_PROBE_CACHE = {}

def _finnhub_probe(missing):
    """Decide ONCE whether Finnhub is usable, by testing names that must have
    analyst coverage. Fixes the v17 flaw where a handful of obscure small-caps
    returning legitimately-empty data tripped the kill-switch and abandoned
    the whole pass (including AAPL/MSFT further down the list)."""
    _FINNHUB_STATS["probed"] = True
    cands = [s for s in _LIQUID_PROBES if s in missing][:3] or list(missing)[:3]
    print(f"    Finnhub health probe using: {cands}", flush=True)
    for sym in cands:
        got = _finnhub_analyst(sym, _probe=True)
        if got:
            _FINNHUB_PROBE_CACHE[sym] = got
            _FINNHUB_STATS["healthy"] = True
            print("    ✅ Finnhub healthy — sweeping remaining stocks (empty "
                  "results for individual small-caps = no coverage, not an error)",
                  flush=True)
            return True
        time.sleep(1.1)
    _FINNHUB_STATS["disabled"] = True
    http = _FINNHUB_STATS["last_http"]
    if http in (401, 403):
        why = (f"HTTP {http} — the FINNHUB_API_KEY secret looks invalid, or these "
               f"endpoints aren't included in the plan")
    elif http == 200:
        why = "HTTP 200 but empty even for mega-caps — plan restriction likely"
    else:
        why = f"no usable response (last: {http})"
    print(f"    ❌ Finnhub unusable: {why}. Skipping Finnhub this run.", flush=True)
    return False

def _finnhub_analyst(sym, _probe=False):
    """Ratings + EPS + next earnings date from Finnhub (no price targets)."""
    if _FINNHUB_STATS["disabled"] and not _probe:
        return {}
    out = {}
    rec = _finnhub_get("/stock/recommendation", {"symbol": sym})
    if not _probe:
        if rec is None:
            _FINNHUB_STATS["hard_errors"] += 1
            if _FINNHUB_STATS["hard_errors"] >= 5:  # repeated HTTP failures mid-run
                _FINNHUB_STATS["disabled"] = True
                print(f"    Finnhub: 5 consecutive hard errors (last HTTP "
                      f"{_FINNHUB_STATS['last_http']}) — stopping this pass", flush=True)
            return {}
        _FINNHUB_STATS["hard_errors"] = 0
    if isinstance(rec, list) and rec:
        c   = rec[0]  # latest month first
        sB  = int(c.get("strongBuy") or 0); b = int(c.get("buy") or 0)
        h   = int(c.get("hold") or 0); s = int(c.get("sell") or 0)
        sS  = int(c.get("strongSell") or 0)
        tot = sB + b + h + s + sS
        if tot > 0:
            out.update({"strong_buy": sB, "buy": b, "hold": h, "sell": s,
                        "strong_sell": sS})
            bull = (sB + b) / tot; bear = (s + sS) / tot
            out["bull_pct"] = bull
            if   bull >= 0.70: out["consensus"] = "🟢 Strong Buy"
            elif bull >= 0.50: out["consensus"] = "🟩 Buy"
            elif bear >= 0.50: out["consensus"] = "🔴 Sell"
            elif bear >= 0.30: out["consensus"] = "🟥 Weak Sell"
            else:              out["consensus"] = "🟡 Hold"
    ern = _finnhub_get("/stock/earnings", {"symbol": sym})
    if isinstance(ern, list) and ern:
        out["eps_actual"]   = safe_float(ern[0].get("actual"))
        out["eps_estimate"] = safe_float(ern[0].get("estimate"))
        streak = 0
        for q in ern:
            a, e = safe_float(q.get("actual")), safe_float(q.get("estimate"))
            if a is not None and e is not None and a > e:
                streak += 1
            else:
                break
        out["eps_streak"] = streak
    cal = _finnhub_get("/calendar/earnings",
                       {"symbol": sym,
                        "from": date.today().isoformat(),
                        "to": (date.today() + timedelta(days=120)).isoformat()})
    events = (cal or {}).get("earningsCalendar") or []
    dates  = sorted(e.get("date") for e in events if e.get("date"))
    if dates:
        try:
            nd = datetime.strptime(dates[0], "%Y-%m-%d").date()
            out["next_earnings"]    = nd.strftime("%Y-%m-%d")
            out["days_to_earnings"] = (nd - date.today()).days
        except Exception:
            pass
    return {k: v for k, v in out.items() if v is not None}


# ── STEP 6a: completeness audit + self-healing repair (v16) ───────────────────
def audit_completeness(tickers, tech_res, anal_res, qual_res, label=""):
    tech_missing = [t["ticker"] for t in tickers
                    if tech_res.get(t["ticker"], {}).get("ret12m") is None
                    and tech_res.get(t["ticker"], {}).get("sma200") is None]
    anal_missing = [t["ticker"] for t in tickers if not anal_res.get(t["ticker"])]
    qual_missing = [t["ticker"] for t in tickers if not qual_res.get(t["ticker"])]
    n = len(tickers)
    print(f"  Completeness{label}: technicals {n-len(tech_missing)}/{n} | "
          f"analyst {n-len(anal_missing)}/{n} | quality {n-len(qual_missing)}/{n}", flush=True)
    for name, lst in (("technicals", tech_missing), ("analyst", anal_missing),
                      ("quality", qual_missing)):
        if lst:
            print(f"    missing {name}: {lst[:8]}{' ...' if len(lst) > 8 else ''}", flush=True)
    return {"tech": tech_missing, "anal": anal_missing, "qual": qual_missing}


def repair_data(tickers, history, tech_res, anal_res, qual_res, depth):
    """Retry every missing item against the same source, then alternates.
    deep = generous budgets (overnight); fast = quick retries only (manual)."""
    import yfinance as yf
    gaps = audit_completeness(tickers, tech_res, anal_res, qual_res, " (pre-repair)")
    if not any(gaps.values()):
        print("  Nothing to repair — all categories complete", flush=True)
        return

    # ---- Technicals: yfinance single-ticker retry, then Twelve Data ----
    if gaps["tech"]:
        print(f"  Repairing technicals for {len(gaps['tech'])} stocks...", flush=True)
        still = []
        for sym in gaps["tech"]:
            try:
                df = yf.download(sym, period="15mo", interval="1d",
                                 auto_adjust=True, progress=False)
                if df is not None and len(df.dropna(subset=["Close"])) >= 30:
                    import pandas as pd
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.get_level_values(0)
                    df.index = __import__("pandas").to_datetime(df.index).tz_localize(None)
                    history[sym] = df.dropna(subset=["Close"])
                else:
                    still.append(sym)
            except Exception:
                still.append(sym)
            time.sleep(1.0)
        if still:
            _td_history_fallback(still, history)
        repaired = [t for t in tickers if t["ticker"] in gaps["tech"]
                    and t["ticker"] in history]
        if repaired:
            fresh = compute_technicals(history, repaired)
            for sym, vals in fresh.items():
                if vals:
                    tech_res[sym] = vals
            print(f"    technicals recovered for {len([s for s,v in fresh.items() if v])} stocks", flush=True)

    # ---- Analyst: Yahoo second pass -> FMP re-probe -> Finnhub ----
    if gaps["anal"]:
        missing = [s for s in gaps["anal"] if not anal_res.get(s)]
        budget_n   = len(missing) if depth == "deep" else min(12, len(missing))
        budget_sec = 20 * 60 if depth == "deep" else 3 * 60
        breaker    = 15 if depth == "deep" else 5
        print(f"  Repairing analyst data for {len(missing)} stocks "
              f"(Yahoo pass: up to {budget_n}, {budget_sec//60} min budget)...", flush=True)
        from yfinance.data import YfData
        yfd = YfData(session=requests.Session())  # fresh session/cookies
        t0, fails, got_yahoo = time.time(), 0, 0
        for sym in missing[:budget_n]:
            if time.time() - t0 > budget_sec or fails >= breaker:
                print("    Yahoo repair budget exhausted — moving to alternates", flush=True)
                break
            try:
                j = yfd.get_raw_json(f"{_YF_QS_URL}/{sym}",
                                     params={"modules": _QS_MODULES,
                                             "corsDomain": "finance.yahoo.com",
                                             "formatted": "false", "symbol": sym})
                blocks = ((j.get("quoteSummary") or {}).get("result") or [])
                if blocks:
                    parsed = _parse_quote_summary(blocks[0])
                    if parsed:
                        anal_res[sym] = parsed; got_yahoo += 1; fails = 0
            except Exception as e:
                fails += 1 if ("429" in str(e) or "Too Many" in str(e)) else 0
            time.sleep(2.0)
        # FMP: allow a fresh 3-attempt probe in the repair pass
        _FMP_STATS.update({"disabled": False, "tried": max(0, _FMP_STATS["tried"] - 3)})
        got_fmp = 0
        for sym in [s for s in missing if not anal_res.get(s)]:
            r2 = _fmp_analyst_fallback(sym)
            if r2:
                anal_res[sym] = r2; got_fmp += 1
            if _FMP_STATS["disabled"] or _FMP_BUDGET["remaining"] < 2:
                break
        # Finnhub (independent source; only if key configured)
        got_fh = 0
        if not FINNHUB_KEY:
            print("    Finnhub: FINNHUB_API_KEY is NOT present in this workflow's "
                  "env — add it to the env block of refresh.yml (the quality "
                  "workflow having it does not cover the daily one)", flush=True)
        else:
            print(f"    Finnhub: key present (length {len(FINNHUB_KEY)})", flush=True)
            fh_list = [s for s in missing if not anal_res.get(s)]
            # Probe with heavily-covered mega-caps FIRST so the self-disable
            # gate can't be tripped by five obscure micro-caps in a row.
            LIQUID = ["AAPL","MSFT","NVDA","GOOGL","AMZN","META","TSM","AVGO",
                      "AMD","INTC","CSCO","QCOM","MU","CRWD","ANET"]
            fh_list = ([s for s in LIQUID if s in fh_list]
                       + [s for s in fh_list if s not in LIQUID])
            if depth != "deep":
                fh_list = fh_list[:40]
            if fh_list and _finnhub_probe(fh_list):
                # probe already recovered its hit(s); harvest them
                for sym in [s for s in fh_list if s in _FINNHUB_PROBE_CACHE]:
                    anal_res[sym] = _FINNHUB_PROBE_CACHE[sym]; got_fh += 1
                remaining = [s for s in fh_list if not anal_res.get(s)]
                print(f"    Finnhub sweep: {len(remaining)} stocks "
                      f"(~{len(remaining)*3//60+1} min)...", flush=True)
                for i, sym in enumerate(remaining, 1):
                    r3 = _finnhub_analyst(sym)
                    if r3:
                        anal_res[sym] = r3; got_fh += 1
                    if _FINNHUB_STATS["disabled"]:
                        break
                    if i % 25 == 0:
                        print(f"      ... {i}/{len(remaining)} swept, "
                              f"{got_fh} recovered", flush=True)
                    time.sleep(1.1)  # 3 calls/stock within 60/min
        print(f"    analyst recovered: yahoo={got_yahoo} fmp={got_fmp} finnhub={got_fh}", flush=True)

    # ---- Quality: bounded ROIC.ai top-up (deep runs only — 5/min is slow) ----
    if gaps["qual"] and depth == "deep" and REFRESH_MODE != "quality":
        topup = gaps["qual"][:10]
        print(f"  Repairing quality for {len(topup)} stocks via ROIC.ai "
              f"(bounded; full coverage comes from the weekly run)...", flush=True)
        sub = [t for t in tickers if t["ticker"] in topup]
        fresh_q = fetch_quality_data(sub)
        for sym, vals in fresh_q.items():
            if vals:
                qual_res[sym] = vals

    audit_completeness(tickers, tech_res, anal_res, qual_res, " (post-repair)")


# ── Fetch analyst data (Yahoo quoteSummary, combined modules — v11) ────────────
#
# v10 made 4 separate quoteSummary requests per stock (672 total). Yahoo
# aggressively rate-limits datacenter IPs (GitHub runners) on this endpoint,
# so nearly all requests got 429 and analyst data came back empty — which is
# also why "Writing 0 cells to 🎯 Analyst & Ratings" appeared (nothing to
# write, not a row-mapping bug).
#
# v11 makes ONE request per stock with all 4 modules combined, sequentially,
# with exponential backoff on 429, reusing yfinance's cookie/crumb machinery.

_YF_QS_URL = "https://query2.finance.yahoo.com/v10/finance/quoteSummary"
_QS_MODULES = "financialData,recommendationTrend,earningsHistory,calendarEvents"

def _raw(v):
    """Yahoo sometimes wraps numbers as {'raw': x, 'fmt': '...'}."""
    if isinstance(v, dict):
        return v.get("raw")
    return v

def _parse_quote_summary(res):
    """Parse a combined quoteSummary result block into our analyst dict."""
    out = {}
    fd = res.get("financialData") or {}
    out["target_avg"]  = safe_float(_raw(fd.get("targetMeanPrice")))
    out["target_high"] = safe_float(_raw(fd.get("targetHighPrice")))
    out["target_low"]  = safe_float(_raw(fd.get("targetLowPrice")))

    trend = ((res.get("recommendationTrend") or {}).get("trend") or [])
    cur = next((x for x in trend if x.get("period") == "0m"), trend[0] if trend else None)
    if cur:
        sB = int(_raw(cur.get("strongBuy"))  or 0)
        b  = int(_raw(cur.get("buy"))        or 0)
        h  = int(_raw(cur.get("hold"))       or 0)
        s  = int(_raw(cur.get("sell"))       or 0)
        sS = int(_raw(cur.get("strongSell")) or 0)
        tot = sB + b + h + s + sS
        if tot > 0:
            out.update({"strong_buy": sB, "buy": b, "hold": h,
                        "sell": s, "strong_sell": sS})
            bull = (sB + b) / tot
            bear = (s + sS) / tot
            out["bull_pct"] = bull
            if   bull >= 0.70: out["consensus"] = "🟢 Strong Buy"
            elif bull >= 0.50: out["consensus"] = "🟩 Buy"
            elif bear >= 0.50: out["consensus"] = "🔴 Sell"
            elif bear >= 0.30: out["consensus"] = "🟥 Weak Sell"
            else:              out["consensus"] = "🟡 Hold"

    hist = ((res.get("earningsHistory") or {}).get("history") or [])
    hist = sorted(hist, key=lambda x: _raw(x.get("quarter")) or 0, reverse=True)
    if hist:
        out["eps_actual"]   = safe_float(_raw(hist[0].get("epsActual")))
        out["eps_estimate"] = safe_float(_raw(hist[0].get("epsEstimate")))
        streak = 0
        for hrow in hist:
            a  = safe_float(_raw(hrow.get("epsActual")))
            e2 = safe_float(_raw(hrow.get("epsEstimate")))
            if a is not None and e2 is not None and a > e2:
                streak += 1
            else:
                break
        out["eps_streak"] = streak

    edates = (((res.get("calendarEvents") or {}).get("earnings") or {})
              .get("earningsDate") or [])
    if edates:
        ts = _raw(edates[0])
        try:
            nd = datetime.utcfromtimestamp(int(ts)).date() if isinstance(ts, (int, float)) \
                 else datetime.strptime(str(ts)[:10], "%Y-%m-%d").date()
            out["next_earnings"]    = nd.strftime("%Y-%m-%d")
            out["days_to_earnings"] = (nd - date.today()).days
        except Exception:
            pass

    return {k: v for k, v in out.items() if v is not None}


_FMP_BUDGET = {"remaining": 240}  # FMP free tier = 250 calls/day; keep headroom
_FMP_STATS  = {"tried": 0, "hits": 0, "disabled": False}

def _fmp_analyst_fallback(sym):
    """Best-effort fallback for price targets + ratings via FMP if Yahoo blocks us.
    Bounded by the free-tier daily budget. Probes first: if the first 5 attempts
    all come back empty (endpoints not on the free plan), it disables itself
    instead of burning ~15 minutes on 160 fruitless calls."""
    out = {}
    if _FMP_STATS["disabled"] or _FMP_BUDGET["remaining"] < 2:
        return out
    if _FMP_STATS["tried"] >= 5 and _FMP_STATS["hits"] == 0:
        _FMP_STATS["disabled"] = True
        print("    FMP fallback returned nothing for 5 probes — likely not on the "
              "free plan; skipping FMP for the remaining stocks", flush=True)
        return out
    _FMP_BUDGET["remaining"] -= 2
    _FMP_STATS["tried"] += 1
    try:
        pt = fmp_get("/stable/price-target-consensus", {"symbol": sym})
        if isinstance(pt, list) and pt:
            out["target_avg"]  = safe_float(pt[0].get("targetConsensus"))
            out["target_high"] = safe_float(pt[0].get("targetHigh"))
            out["target_low"]  = safe_float(pt[0].get("targetLow"))
        gr = fmp_get("/stable/grades-consensus", {"symbol": sym})
        if isinstance(gr, list) and gr:
            g  = gr[0]
            sB = int(g.get("strongBuy") or 0); b = int(g.get("buy") or 0)
            h  = int(g.get("hold") or 0); s = int(g.get("sell") or 0)
            sS = int(g.get("strongSell") or 0)
            tot = sB + b + h + s + sS
            if tot > 0:
                out.update({"strong_buy": sB, "buy": b, "hold": h,
                            "sell": s, "strong_sell": sS,
                            "bull_pct": (sB + b) / tot})
    except Exception:
        pass
    out = {k: v for k, v in out.items() if v is not None}
    if out:
        _FMP_STATS["hits"] += 1
    return out


def fetch_analyst_data(tickers, cached=None):
    """Sequential combined quoteSummary fetch with adaptive backoff."""
    import yfinance  # ensure installed
    from yfinance.data import YfData
    yfd = YfData(session=requests.Session())

    symbols = [t["ticker"] for t in tickers]
    print(f"  Fetching analyst data for {len(symbols)} stocks "
          f"(1 combined Yahoo call each, sequential)...", flush=True)

    results   = {}
    delay     = 0.7
    yahoo_429 = 0
    consec_fail = 0
    cached    = cached or {}

    for i, sym in enumerate(symbols, 1):
        result = {}
        for attempt in range(2):          # one retry only — a blocked IP stays blocked
            try:
                j = yfd.get_raw_json(
                    f"{_YF_QS_URL}/{sym}",
                    params={"modules": _QS_MODULES,
                            "corsDomain": "finance.yahoo.com",
                            "formatted": "false", "symbol": sym},
                )
                blocks = ((j.get("quoteSummary") or {}).get("result") or [])
                if blocks:
                    result = _parse_quote_summary(blocks[0])
                consec_fail = 0
                break
            except Exception as e:
                msg = str(e)
                is_429 = "429" in msg or "Too Many Requests" in msg
                if is_429:
                    yahoo_429 += 1
                    consec_fail += 1
                    delay = min(5.0, delay * 1.3)   # slow the whole loop down
                    if attempt < 1:
                        print(f"    429 on {sym} — backing off 12s (retry 1/1)...", flush=True)
                        time.sleep(12)
                        continue
                else:
                    # 404 / delisted / parse errors: skip quietly
                    break
        if not result:
            result = _fmp_analyst_fallback(sym)
        results[sym] = result
        if i % 20 == 0 or i == len(symbols):
            ok = len([r for r in results.values() if r])
            print(f"    [{i}/{len(symbols)}] done — {ok} with data "
                  f"(429s so far: {yahoo_429})", flush=True)
        if consec_fail >= 10:             # trip fast: ~10 blocked stocks ≈ 2.5 min
            print("  ⚠️ Yahoo appears to be hard-blocking this runner IP. "
                  "Switching remaining stocks to FMP fallback (budget-limited).", flush=True)
            for rest in symbols[i:]:
                results[rest] = _fmp_analyst_fallback(rest)
            break
        time.sleep(delay)

    # Cache fallback: analyst targets/ratings drift slowly, so yesterday's data
    # beats an empty cell. Any run that lands on a Yahoo-friendly runner IP
    # refreshes the cache; blocked runs coast on it.
    used_cache = 0
    for sym in symbols:
        if not results.get(sym) and sym in cached:
            entry = dict(cached[sym])
            ne = entry.get("next_earnings")
            if ne:  # recompute the countdown so staleness doesn't skew it
                try:
                    entry["days_to_earnings"] = (
                        datetime.strptime(str(ne)[:10], "%Y-%m-%d").date() - date.today()
                    ).days
                except Exception:
                    entry.pop("days_to_earnings", None)
            results[sym] = entry
            used_cache += 1
    if used_cache:
        print(f"  Reused cached analyst data for {used_cache} stocks "
              f"(fresh fetch was empty for them)", flush=True)

    ok = len([r for r in results.values() if r])
    print(f"  Analyst data complete: {ok}/{len(symbols)} stocks with data", flush=True)
    sample = [(k, v) for k, v in results.items() if v][:3]
    for sym, d in sample:
        print(f"    {sym}: target={d.get('target_avg')} consensus={d.get('consensus')} "
              f"earnings={d.get('next_earnings')}", flush=True)
    empty = [k for k, v in results.items() if not v]
    if empty:
        print(f"    {len(empty)} stocks returned no analyst data: {empty[:5]}", flush=True)
    return results


def _OLD_fetch_one_analyst_yf(sym):
    """(v10 legacy — kept for reference, no longer called.)"""
    result = {}
    # Add small random delay to avoid thundering herd on Yahoo
    time.sleep(0.5 + (hash(sym) % 10) * 0.1)
    try:
        ticker = yf.Ticker(sym)

        # Price targets
        try:
            apt = ticker.analyst_price_targets
            if apt and isinstance(apt, dict):
                result["target_avg"]  = safe_float(apt.get("mean"))
                result["target_high"] = safe_float(apt.get("high"))
                result["target_low"]  = safe_float(apt.get("low"))
        except Exception: pass

        # Analyst recommendations
        try:
            recs = ticker.recommendations
            if recs is not None and not recs.empty:
                # Get most recent period
                latest = recs.iloc[-1] if len(recs) > 0 else None
                if latest is not None:
                    sB = int(latest.get("strongBuy",  0) or 0)
                    b  = int(latest.get("buy",        0) or 0)
                    h  = int(latest.get("hold",       0) or 0)
                    s  = int(latest.get("sell",       0) or 0)
                    sS = int(latest.get("strongSell", 0) or 0)
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

        # Earnings history (EPS surprise)
        try:
            eh = ticker.earnings_history
            if eh is not None and not eh.empty:
                eh = eh.sort_index(ascending=False)
                first = eh.iloc[0]
                result["eps_actual"]   = safe_float(first.get("epsActual"))
                result["eps_estimate"] = safe_float(first.get("epsEstimate"))
                streak = 0
                for _, row in eh.iterrows():
                    a  = safe_float(row.get("epsActual"))
                    e2 = safe_float(row.get("epsEstimate"))
                    if a is not None and e2 is not None and a > e2:
                        streak += 1
                    else:
                        break
                result["eps_streak"] = streak
        except Exception: pass

        # Earnings calendar (next date)
        try:
            cal = ticker.calendar
            if cal and isinstance(cal, dict):
                nd = cal.get("Earnings Date")
                if nd:
                    # Can be list or single value
                    if isinstance(nd, (list, tuple)):
                        nd = nd[0]
                    if hasattr(nd, "strftime"):
                        nd_str = nd.strftime("%Y-%m-%d")
                    else:
                        nd_str = str(nd)[:10]
                    result["next_earnings"] = nd_str
                    try:
                        result["days_to_earnings"] = (
                            datetime.strptime(nd_str, "%Y-%m-%d").date() - date.today()
                        ).days
                    except Exception:
                        pass
        except Exception: pass

    except Exception as e:
        pass  # silently skip failures

    return sym, result


def _OLD_fetch_analyst_data(tickers):
    """(v10 legacy — kept for reference, no longer called.)"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    print(f"  Fetching analyst data for {len(tickers)} stocks (yfinance, 3 concurrent)...")
    results = {}
    symbols = [t["ticker"] for t in tickers]
    done    = 0
    with ThreadPoolExecutor(max_workers=3) as executor:  # 3 concurrent to avoid Yahoo rate limits
        futures = {executor.submit(_OLD_fetch_one_analyst_yf, sym): sym for sym in symbols}
        for future in as_completed(futures):
            try:
                sym, result = future.result()
                results[sym] = result
                done += 1
                if done % 20 == 0 or done == len(symbols):
                    print(f"    [{done}/{len(symbols)}] completed", flush=True)
            except Exception as e:
                sym = futures[future]
                print(f"    Error {sym}: {e}", flush=True)
                results[sym] = {}
    print(f"  Analyst data complete for {len(results)} stocks")
    # Debug: show sample of what was retrieved
    sample = [(k,v) for k,v in results.items() if v][:3]
    for sym, d in sample:
        print(f"    {sym}: target={d.get('target_avg')} consensus={d.get('consensus')} earnings={d.get('next_earnings')}", flush=True)
    empty = [k for k,v in results.items() if not v]
    if empty:
        print(f"    {len(empty)} stocks returned no analyst data: {empty[:5]}", flush=True)
    return results

# ── Fetch quality metrics (ROIC.ai) ───────────────────────────────────────────
def _pick(d, *candidates):
    """Tolerant field extraction: exact key first, then case-insensitive
    substring match — guards against minor API field renames."""
    for c in candidates:
        if c in d:
            return safe_float(d.get(c))
    lower = {k.lower(): k for k in d}
    for c in candidates:
        cl = c.lower()
        for lk, orig in lower.items():
            if cl in lk:
                return safe_float(d.get(orig))
    return None

_FMP_QUAL_STATS = {"tried": 0, "hits": 0, "disabled": False}

def _fmp_quality_fallback(sym):
    """Best-effort quality metrics via FMP key-metrics-ttm (1 call/stock so 168
    stocks fit the 250/day free budget). Probe-gated: disables itself after 5
    empty responses. Partial fields beat none."""
    if _FMP_QUAL_STATS["disabled"] or _FMP_BUDGET["remaining"] < 1:
        return {}
    if _FMP_QUAL_STATS["tried"] >= 5 and _FMP_QUAL_STATS["hits"] == 0:
        _FMP_QUAL_STATS["disabled"] = True
        print("    FMP quality fallback empty after 5 probes — likely not on "
              "the free plan; skipping for remaining stocks", flush=True)
        return {}
    _FMP_BUDGET["remaining"] -= 1
    _FMP_QUAL_STATS["tried"] += 1
    out = {}
    try:
        km = fmp_get("/stable/key-metrics-ttm", {"symbol": sym})
        if isinstance(km, list) and km:
            k = km[0]
            roic = _pick(k, "returnOnInvestedCapitalTTM", "roicTTM", "roic")
            if roic is not None:
                out["roic"] = roic * 100 if abs(roic) <= 3 else roic  # ratio vs %
            ev = _pick(k, "evToEBITDATTM", "enterpriseValueOverEBITDATTM", "evToEbitda")
            if ev is not None: out["ev_ebitda"] = ev
            fcfy = _pick(k, "freeCashFlowYieldTTM", "freeCashFlowYield")
            if fcfy is not None:
                out["fcf_yield"] = fcfy * 100 if abs(fcfy) <= 3 else fcfy
            de = _pick(k, "netDebtToEBITDATTM", "netDebtToEbitda", "debtToEbitda")
            if de is not None: out["debt_ebitda"] = de
    except Exception:
        pass
    if out:
        _FMP_QUAL_STATS["hits"] += 1
    return out

def fetch_quality_data(tickers):
    # 5 documented v2 endpoints per stock (paths & fields verified against
    # https://www.roic.ai/api/docs, July 2026). Free tier = 5 requests/min,
    # so a full run is ~167 stocks x 5 calls x 12s ≈ 2h45m (weekly).
    print(f"  Fetching quality metrics for {len(tickers)} stocks "
          f"(ROIC.ai v2 — 5/min, ~2h45m for a full run)...", flush=True)
    results = {}
    got_any = 0

    def _throttle():
        if not _ROIC["fail_all"]:
            time.sleep(12)

    for i, t in enumerate(tickers):
        sym    = t["ticker"]
        result = {}
        print(f"    [{i+1}/{len(tickers)}] {sym}", flush=True)

        prof = roic_get(f"/fundamental/ratios/profitability/{sym}?period=annual&limit=1")
        if prof:
            p = prof[0]
            result["roic"]     = _pick(p, "return_on_inv_capital", "return_on_cap")
            result["roe"]      = _pick(p, "return_com_eqy")
            result["gm"]       = _pick(p, "gross_margin")
            result["ebitda_m"] = _pick(p, "ebitda_margin")
            result["net_m"]    = _pick(p, "profit_margin")
        _throttle()

        cred = roic_get(f"/fundamental/ratios/credit/{sym}?period=annual&limit=1")
        if cred:
            c = cred[0]
            result["debt_ebitda"] = _pick(c, "tot_debt_to_ebitda", "net_debt_to_ebitda")
            result["int_cov"]     = _pick(c, "ebit_to_int_exp", "interest_coverage")
        _throttle()

        yld = roic_get(f"/fundamental/ratios/yield-analysis/{sym}?period=annual&limit=1")
        if yld:
            y = yld[0]
            result["fcf_yield"] = _pick(y, "free_cash_flow_yield")
        _throttle()

        mult = roic_get(f"/fundamental/multiples/{sym}?period=annual&limit=1")
        if mult:
            m = mult[0]
            result["fwd_pe"]    = _pick(m, "pe_ratio")
            result["ev_ebitda"] = _pick(m, "ev_to_ttm_ebitda", "ev_to_ebitda")
        _throttle()

        # Revenue growth: no longer a ratio field on v2 — compute it from the
        # last two annual revenues on the income statement (DESC order).
        inc = roic_get(f"/fundamental/income-statement/{sym}?period=annual&limit=2&order=DESC")
        if inc:
            if result.get("gm") is None:
                result["gm"] = _pick(inc[0], "gross_margin")
            if len(inc) >= 2:
                r0 = _pick(inc[0], "is_sales_revenue_turnover", "is_sales_and_services_revenues")
                r1 = _pick(inc[1], "is_sales_revenue_turnover", "is_sales_and_services_revenues")
                if r0 is not None and r1 not in (None, 0):
                    result["rev_growth"] = round((r0 / r1 - 1) * 100, 2)
        _throttle()

        # FMP fallback when ROIC.ai yields nothing for this stock
        result = {k: v for k, v in result.items() if v is not None}
        if not result:
            result = _fmp_quality_fallback(sym)

        # Rule of 40 = Revenue Growth % + Gross Margin %
        gm  = result.get("gm")
        rg  = result.get("rev_growth")
        if gm is not None and rg is not None:
            result["rule40"] = round(gm + rg, 1)

        if result:
            got_any += 1
        results[sym] = result
        if (i + 1) % 20 == 0:
            print(f"      ... {got_any}/{i+1} with data so far", flush=True)

    print(f"  Quality data complete: {got_any}/{len(tickers)} stocks with data", flush=True)
    if got_any == 0:
        print("  ❌ ZERO quality data retrieved — scores will use the neutral "
              "default. Check the ROIC probe output above for the reason.", flush=True)
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
    em   = quality.get("ebitda_m")
    # A negative debt/EBITDA ratio is ambiguous and previously EXPLODED this
    # sub-score (3*(1 - de/4) is unbounded for de<0 — CRWD/WULF/MARA scored
    # 50+ points on a 3-point sub-pillar). Sign-aware handling:
    #   de >= 0            -> normal scaling, naturally capped at 3
    #   de < 0, EBITDA > 0 -> net cash: best case, full 3 points
    #   de < 0, EBITDA <= 0/unknown -> leverage unmeasurable: 0 points
    if de is None:
        q4 = 1.5
    elif de < 0:
        q4 = 3.0 if (em is not None and em > 0) else 0.0
    else:
        q4 = min(3, max(0, 3 * (1 - de / 4)))
    quality_score = min(22, q1 + q2 + q3 + q4)  # belt-and-braces pillar cap

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
    vc1 = 4 if (vr and vr > 1.5) else (2 if (vr and vr > 0.8) else (1 if vr else 0))

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
        # Debug fields
        "_price":    price,
        "_r12":      r12,
        "_r6":       r6,
        "_sma50":    sma50,
        "_sma200":   sma200,
        "_roic":     roic,
        "_gm":       gm,
    }

# ── Write to Google Sheets (batch updates) ────────────────────────────────────
def _strip_ticker(raw):
    clean = str(raw).strip()
    for prefix in ["🌟 ", "🏦 ", "⚠️ ", "⛔ "]:
        clean = clean.replace(prefix, "")
    return clean.strip()

def build_row_map(ws, tickers, label):
    """Scan a worksheet for the column containing tickers and map ticker -> row.
    Returns None (caller falls back to Live Prices row numbers) if the sheet
    doesn't appear to contain the tickers."""
    try:
        vals = ws.get_all_values()
    except Exception as e:
        print(f"  ⚠️ Could not read {label} for row mapping ({e}) — "
              f"falling back to Live Prices rows", flush=True)
        return None
    tick_set = {t["ticker"] for t in tickers}
    best_col, best_hits = None, 0
    max_cols = min(6, max((len(r) for r in vals), default=0))
    for c in range(max_cols):
        hits = sum(1 for r in vals if len(r) > c and _strip_ticker(r[c]) in tick_set)
        if hits > best_hits:
            best_hits, best_col = hits, c
    if best_col is None or best_hits < len(tick_set) * 0.5:
        print(f"  ⚠️ {label}: could not locate ticker column "
              f"(best match {best_hits}/{len(tick_set)}) — "
              f"falling back to Live Prices rows", flush=True)
        return None
    rowmap = {}
    for i, r in enumerate(vals):
        if len(r) > best_col:
            clean = _strip_ticker(r[best_col])
            if clean in tick_set and clean not in rowmap:
                rowmap[clean] = i + 1
    print(f"  {label}: matched {len(rowmap)}/{len(tick_set)} tickers "
          f"in column {col_letter(best_col+1)}", flush=True)
    return rowmap

def write_to_sheets(wb, tickers, tech_res, anal_res, qual_res, scores, spy_ret12, regime):
    # v11: Step 6 no longer makes per-ticker read calls, so only a short pause
    # is needed for the Sheets read quota.
    print("  Waiting 15 seconds for Google Sheets quota headroom...", flush=True)
    time.sleep(15)
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

    # Map tickers to their actual rows on the Technicals and Analyst tabs
    # (defensive: v10 assumed they mirror Live Prices row-for-row).
    tech_map = build_row_map(ws_t, tickers, TAB_TECH)
    anal_map = build_row_map(ws_a, tickers, TAB_ANALYST)

    p_updates = []
    t_updates = []
    a_updates = []

    # v12 writes risk metrics to cols 45-49 (AS-AW). Expand the Technicals
    # grid if the tab is narrower, otherwise batch_update raises
    # "exceeds grid limits" and the whole write step fails.
    try:
        needed = TC_52W  # rightmost new column (49)
        if getattr(ws_t, "col_count", needed) < needed:
            print(f"  Expanding {TAB_TECH} from {ws_t.col_count} to {needed} columns...", flush=True)
            ws_t.add_cols(needed - ws_t.col_count)
    except Exception as e:
        print(f"  ⚠️ Could not verify/expand {TAB_TECH} column count ({e}) — "
              f"risk columns may fail to write if the tab has <49 columns", flush=True)

    # Headers for the new risk-metric columns (row 4 = header row; idempotent)
    for col, hdr in [(TC_BETA, "BETA"), (TC_VOL_ANN, "VOL % (ann)"),
                     (TC_MAX_DD, "MAX DD %"), (TC_SHARPE, "SHARPE"),
                     (TC_52W, "% FROM 52W HIGH")]:
        t_updates.append({"range": f"{col_letter(col)}4", "values": [[hdr]]})

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
        trow = tech_map.get(sym, row) if tech_map else row
        arow = anal_map.get(sym, row) if anal_map else row
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
        tu(TC_SMA20,   trow, tech.get("sma20"));   tu(TC_SMA50,   trow, tech.get("sma50"))
        tu(TC_SMA200,  trow, tech.get("sma200"));  tu(TC_EMA12,   trow, tech.get("ema12"))
        tu(TC_EMA26,   trow, tech.get("ema26"));   tu(TC_RSI,     trow, tech.get("rsi"))
        tu(TC_MACD,    trow, tech.get("macd"));    tu(TC_MACD_SIG,trow, tech.get("macd_sig"))
        tu(TC_MACD_HIST,trow,tech.get("macd_hist"))
        tu(TC_BB_UP,   trow, tech.get("bb_upper")); tu(TC_BB_MID, trow, tech.get("bb_middle"))
        tu(TC_BB_LOW,  trow, tech.get("bb_lower")); tu(TC_ADX,    trow, tech.get("adx"))
        tu(TC_ATR,     trow, tech.get("atr"))
        if r12 is not None: tu(TC_RET12,   trow, round(r12 * 100, 1))
        if r6  is not None: tu(TC_RET6,    trow, round(r6  * 100, 1))
        if vr  is not None: tu(TC_VOL,     trow, round(vr, 3))
        tu(TC_RS_SPY,  trow, round(rs, 1))
        tu(TC_ROIC,    trow, qual.get("roic"));     tu(TC_ROE,       trow, qual.get("roe"))
        tu(TC_GM,      trow, qual.get("gm"));       tu(TC_EBITDA_M,  trow, qual.get("ebitda_m"))
        tu(TC_NET_M,   trow, qual.get("net_m"));    tu(TC_FCF,       trow, qual.get("fcf_yield"))
        tu(TC_DEBT_EBITDA, trow, qual.get("debt_ebitda"))
        tu(TC_INT_COV, trow, qual.get("int_cov"));  tu(TC_FWD_PE,    trow, qual.get("fwd_pe"))
        tu(TC_EV_EBITDA,trow,qual.get("ev_ebitda")); tu(TC_RULE40,   trow, qual.get("rule40"))
        tu(TC_SCORE_MOM,  trow, sc.get("momentum")); tu(TC_SCORE_QUAL,trow, sc.get("quality"))
        tu(TC_SCORE_EARN, trow, sc.get("earnings")); tu(TC_SCORE_ANAL,trow, sc.get("analyst"))
        tu(TC_SCORE_RS,   trow, sc.get("rel_str"));  tu(TC_SCORE_VAL, trow, sc.get("value"))
        tu(TC_SCORE_VOL,  trow, sc.get("volume"))

        # Risk metrics (v12)
        if tech.get("beta")    is not None: tu(TC_BETA,    trow, round(tech["beta"], 2))
        if tech.get("vol_ann") is not None: tu(TC_VOL_ANN, trow, round(tech["vol_ann"], 1))
        if tech.get("max_dd")  is not None: tu(TC_MAX_DD,  trow, round(tech["max_dd"], 1))
        if tech.get("sharpe")  is not None: tu(TC_SHARPE,  trow, round(tech["sharpe"], 2))
        if tech.get("pct_52w") is not None: tu(TC_52W,     trow, round(tech["pct_52w"], 1))

        # Analyst
        au(5,  arow, anal.get("target_avg"));    au(6,  arow, anal.get("target_high"))
        au(7,  arow, anal.get("target_low"));    au(AN_STRONG_BUY, arow, anal.get("strong_buy"))
        au(AN_BUY,  arow, anal.get("buy"));      au(AN_HOLD, arow, anal.get("hold"))
        au(AN_SELL, arow, anal.get("sell"));     au(AN_STRONG_SELL, arow, anal.get("strong_sell"))
        au(AN_CONSENSUS, arow, anal.get("consensus"))
        au(AN_EPS_ACT, arow, anal.get("eps_actual"))
        au(AN_EPS_EST, arow, anal.get("eps_estimate"))
        au(AN_STREAK,  arow, anal.get("eps_streak"))
        au(AN_NEXT_EARN, arow, anal.get("next_earnings"))
        au(AN_DAYS_EARN, arow, anal.get("days_to_earnings"))

    def batch_write(ws, updates, label):
        print(f"  Writing {len(updates)} cells to {label}...")
        for i in range(0, len(updates), 500):
            ws.batch_update(updates[i:i+500])
            time.sleep(1)

    batch_write(ws_p, p_updates, TAB_PRICES)
    print("  Pausing 30s between sheets to avoid quota...", flush=True)
    time.sleep(30)
    batch_write(ws_t, t_updates, TAB_TECH)
    print("  Pausing 30s between sheets to avoid quota...", flush=True)
    time.sleep(30)
    batch_write(ws_a, a_updates, TAB_ANALYST)
    print("  Pausing 30s between sheets to avoid quota...", flush=True)
    time.sleep(30)

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

    # 📖 Score Key tab — same content as the dashboard key, auto-created
    try:
        time.sleep(10)
        key_ws = next((w for w in wb.worksheets() if "Score Key" in w.title), None)
        if key_ws is None:
            key_ws = wb.add_worksheet(title="📖 Score Key", rows=80, cols=6)
            print("  Created new '📖 Score Key' tab", flush=True)
        rows = [["COMPOSITE SCORE /100 — sum of seven pillars. Bands:", "", "", ""]]
        rows += [[b, rng, desc, ""] for b, rng, _c, desc in SCORE_BANDS]
        rows += [["", "", "", ""], ["PILLAR", "MAX PTS", "WHAT IT MEASURES", "NOTES"]]
        rows += [[n, mx, what, note] for n, mx, what, note in SCORE_KEY]
        rows += [["", "", "", ""], ["RISK METRIC", "HOW TO READ IT", "", ""]]
        rows += [[n, what, "", ""] for n, what in RISK_KEY]
        rows += [["", "", "", ""],
                 [f"Auto-generated by refresh {VERSION} — edits here are overwritten each run.", "", "", ""]]
        key_ws.update(range_name=f"A1:D{len(rows)}", values=rows)
        print(f"  📖 Score Key written ({len(rows)} rows)", flush=True)
    except Exception as e:
        print(f"  ⚠️ Could not write Score Key tab: {e}", flush=True)

    print("  All sheets written successfully")

# ── Screening charts template (inserted into the dashboard HTML) ──────────────
# Plain string (not f-string) to avoid brace-escaping; __CHART_DATA__ is
# replaced with the JSON payload at generation time. Threshold zones (green)
# mark the "strongest buy" regions; qualifying tickers are labelled directly
# on each chart.

# ── Scoring key & bands (v17) — single source of truth for dashboard + sheet ──
SCORE_BANDS = [
    ("🚀 EXCEPTIONAL", "80 – 100",  "#7b1fa2", "Rare. Strong on nearly every pillar — verify it isn't a data error, then treat as a priority candidate."),
    ("🟢 STRONG BUY",  "65 – 79.9", "#2e7d32", "Leaders. Momentum + quality confirmed; the primary buy-research list."),
    ("🟩 POSITIVE",    "50 – 64.9", "#66bb6a", "Healthy. Watchlist names — look for an entry trigger or catalyst."),
    ("🟡 NEUTRAL",     "35 – 49.9", "#f9a825", "Mixed signals. Hold if owned with thesis intact; not a fresh-money buy."),
    ("🟥 WEAK",        "20 – 34.9", "#ef6c00", "Deteriorating. Review holdings; tighten stops."),
    ("🔴 POOR",        "0 – 19.9",  "#c62828", "Avoid / exit candidates. Momentum and fundamentals both against you."),
]
SCORE_KEY = [
    ("MOMENTUM", "32", "Price trend strength: 12-month return (15 pts, scales −30%→+50%), 6-month return (10 pts, −20%→+40%), trend stack Price>SMA50>SMA200 (5 pts), MACD above signal with rising histogram (2 pts).", "Halved automatically when SPY is below its 200-day average (bear regime)."),
    ("QUALITY", "22", "Business durability: ROIC (8 pts, full at ≥30%), gross margin (7 pts, full at ≥60%), free-cash-flow yield (4 pts, full at ≥5%), debt/EBITDA (3 pts: net cash = full, ≥4× or negative EBITDA = 0).", "Data from ROIC.ai annual fundamentals; refreshed weekly."),
    ("EARNINGS", "13", "Execution: last EPS surprise vs estimate (7 pts, full at ≥+20% beat), consecutive beat streak (3 pts, full at 4 quarters), Rule of 40 (3 pts if ≥40, 1.5 if ≥20).", "Rule of 40 = revenue growth % + gross margin %."),
    ("ANALYST", "13", "Street view: upside to average price target (9 pts, full at ≥+40%), share of buy-rated analysts (4 pts if ≥70% bullish, 2.5 if ≥50%).", "Zero when analyst data is unavailable — the composite ceiling drops to ≈74 for those stocks."),
    ("REL STRENGTH", "10", "12-month return minus SPY's 12-month return (full marks at ≥+30 points vs the index).", "Currently 7 of the 10 points are implemented (sector-relative leg on the roadmap)."),
    ("VALUE", "6", "PEGY ratio: P/E ÷ (growth + dividend yield); cheaper growth scores higher.", "Currently inactive (0 for all) — awaiting a reliable forward-growth feed. Attainable composite max is therefore ≈91 (≈97 by design)."),
    ("VOLUME", "4", "Conviction check: today's volume ÷ 20-day average (4 pts if >1.5×, 2 if >0.8×, 1 otherwise).", "High volume on up-moves = accumulation."),
]
RISK_KEY = [
    ("BETA (vs SPY)", "Sensitivity to the index: 1.0 moves with SPY; >1.5 amplifies swings both ways; <0.8 defensive."),
    ("VOL % (ann.)", "Annualised daily volatility: <25% calm; 25–50% typical growth stock; >60% speculative — size positions accordingly."),
    ("MAX DD %", "Worst peak-to-trough fall in the loaded window — the pain you'd have taken holding it."),
    ("SHARPE (1Y)", "Return per unit of risk (4% risk-free assumed): <0 lost to cash; 0–1 modest; 1–2 good; >2 excellent."),
    ("% FROM 52W HIGH", "0 to −5% = at highs (strength); −5 to −20% = normal pullback; below −30% = broken trend, needs a reason to own."),
    ("RSI (14)", "Momentum oscillator: >70 overbought (extended), <30 oversold (washed out), 40–60 neutral."),
    ("RULE OF 40", "Growth health: revenue growth % + gross margin % ≥40 = efficient growth; the SaaS/quality-growth yardstick."),
]

CHARTS_TEMPLATE = r"""
<style>
.charts{padding:14px 20px;display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:16px}
.chartcard{background:#fff;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,.12);padding:14px}
.chartcard h3{font-size:14px;margin-bottom:2px;color:#0f3460}
.chartcard .cnote{font-size:11px;color:#2e7d32;margin-bottom:8px;font-weight:bold}
.chartcard .csub{font-size:11px;color:#888;margin-bottom:6px}
.cwrap{position:relative;height:340px}
.zonebar{padding:12px 20px 0;display:flex;flex-wrap:wrap;gap:8px}
.zpill{background:#fff;border-left:4px solid #43a047;border-radius:6px;box-shadow:0 1px 3px rgba(0,0,0,.12);padding:8px 12px;font-size:12px;max-width:340px}
.zpill b{color:#0f3460;display:block;margin-bottom:2px}
.zpill.zempty{border-left-color:#b0bec5;color:#78909c}
.zpill .zt{color:#1b5e20;font-weight:bold}
</style>
__ZONE_SUMMARY__
<div class="charts">
  <div class="chartcard">
    <h3>1️⃣ Composite Score vs Upside to Analyst Target</h3>
    <div class="cnote">🎯 Best-buy zone: Score ≥ 65 AND upside ≥ 20% (shaded green, tickers labelled)</div>
    <div class="csub">Top-right = high conviction + analysts see room. High score at/above target = easy move may be done.</div>
    <div class="cwrap"><canvas id="c1"></canvas></div>
  </div>
  <div class="chartcard">
    <h3>2️⃣ Momentum vs Quality</h3>
    <div class="cnote">🎯 Durable-compounder zone: Momentum ≥ 24/32 AND Quality ≥ 15/22</div>
    <div class="csub">High momentum + low quality = junk running (tighter stops). High both = size with confidence.</div>
    <div class="cwrap"><canvas id="c2"></canvas></div>
  </div>
  <div class="chartcard">
    <h3>3️⃣ Composite Score vs Days to Earnings</h3>
    <div class="cnote">🎯 Catalyst zone: Score ≥ 65 AND earnings within 10 days</div>
    <div class="csub">Near-term catalyst watchlist — also where you may prefer to wait rather than buy into a print.</div>
    <div class="cwrap"><canvas id="c3"></canvas></div>
  </div>
  <div class="chartcard">
    <h3>4️⃣ Top 20 by Composite Score (🏦 holdings in gold)</h3>
    <div class="cnote">🎯 Thresholds: ≥ 65 Strong Buy (shaded) · ≥ 80 Exceptional (dashed line)</div>
    <div class="csub">Exit-discipline view: do your holdings still rank where you think they do?</div>
    <div class="cwrap"><canvas id="c4"></canvas></div>
  </div>
  <div class="chartcard">
    <h3>5️⃣ Rule of 40 vs EV/EBITDA</h3>
    <div class="cnote">🎯 Quality-at-reasonable-price zone: Rule of 40 ≥ 40 AND EV/EBITDA ≤ 15</div>
    <div class="csub">Growth-quality vs price paid — the "am I overpaying for the story" check. Needs weekly quality data.</div>
    <div class="cwrap"><canvas id="c5"></canvas></div>
  </div>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script>
const CD = __CHART_DATA__;
const BAND_COLORS = {"🚀 EXCEPTIONAL":"#00c853","🟢 STRONG BUY":"#43a047","🟩 POSITIVE":"#81c784",
                     "🟡 NEUTRAL":"#ffd54f","🟥 WEAK":"#e57373","🔴 POOR":"#c62828"};
const clamp = (v,lo,hi)=>Math.max(lo,Math.min(hi,v));
const chartMsg = (id,msg)=>{
  const cv=document.getElementById(id); if(!cv) return;
  const d=document.createElement('div');
  d.style.cssText='display:flex;align-items:center;justify-content:center;height:100%;color:#78909c;font:13px Arial;text-align:center;padding:20px';
  d.textContent=msg; cv.parentNode.replaceChild(d, cv);
};
const HAS_CHART = (typeof Chart !== 'undefined');
if(!HAS_CHART){
  ['c1','c2','c3','c4','c5'].forEach(id=>chartMsg(id,
    'Chart library failed to load — hard-refresh (Ctrl+Shift+R) or check ad-blocker/network'));
}

// Shades the "best" zone, draws dashed threshold boundaries + optional hlines
const zonePlugin = {
  id:'zone',
  beforeDraw(chart){
    const z = chart.options.plugins.zone; if(!z) return;
    const {ctx, chartArea:{left,right,top,bottom}, scales:{x,y}} = chart;
    const px = v=>clamp(x.getPixelForValue(v), left, right);
    const py = v=>clamp(y.getPixelForValue(v), top, bottom);
    const x1 = z.xMin!=null ? px(z.xMin) : left,  x2 = z.xMax!=null ? px(z.xMax) : right;
    const y1 = z.yMin!=null ? py(z.yMin) : bottom, y2 = z.yMax!=null ? py(z.yMax) : top;
    ctx.save();
    ctx.fillStyle='rgba(0,200,83,0.10)';
    ctx.fillRect(Math.min(x1,x2), Math.min(y1,y2), Math.abs(x2-x1), Math.abs(y2-y1));
    ctx.strokeStyle='rgba(27,94,32,0.65)'; ctx.setLineDash([6,4]); ctx.lineWidth=1.5;
    if(z.xMin!=null){ctx.beginPath();ctx.moveTo(px(z.xMin),top);ctx.lineTo(px(z.xMin),bottom);ctx.stroke();}
    if(z.xMax!=null){ctx.beginPath();ctx.moveTo(px(z.xMax),top);ctx.lineTo(px(z.xMax),bottom);ctx.stroke();}
    if(z.yMin!=null){ctx.beginPath();ctx.moveTo(left,py(z.yMin));ctx.lineTo(right,py(z.yMin));ctx.stroke();}
    (z.hlines||[]).forEach(h=>{
      ctx.strokeStyle=h.color||'rgba(0,100,0,0.8)';
      ctx.beginPath();ctx.moveTo(left,py(h.v));ctx.lineTo(right,py(h.v));ctx.stroke();
      ctx.setLineDash([6,4]); ctx.font='bold 10px Arial'; ctx.fillStyle=h.color||'#1b5e20';
      ctx.fillText(h.label||'', left+4, py(h.v)-4);
    });
    ctx.restore();
  }
};
// Labels tickers whose points fall inside the zone
const zoneLabels = {
  id:'zoneLabels',
  afterDatasetsDraw(chart){
    const z = chart.options.plugins.zone; if(!z||!z.label) return;
    const meta = chart.getDatasetMeta(0), ds = chart.data.datasets[0];
    const ctx = chart.ctx; ctx.save();
    ctx.font='bold 10px Arial'; ctx.fillStyle='#1b5e20';
    ds.data.forEach((d,i)=>{
      const ok = (z.xMin==null||d.x>=z.xMin)&&(z.xMax==null||d.x<=z.xMax)&&
                 (z.yMin==null||d.y>=z.yMin)&&(z.yMax==null||d.y<=z.yMax);
      if(ok && meta.data[i]) ctx.fillText(d.t, meta.data[i].x+6, meta.data[i].y-6);
    });
    ctx.restore();
  }
};

function scatterPoints(xk, yk, xlo, xhi){
  return CD.filter(d=>d[xk]!=null && d[yk]!=null && d[xk]>=xlo && d[xk]<=xhi)
           .map(d=>({x:d[xk], y:d[yk], t:d.t, hold:d.hold, conv:d.conv, band:d.band}));
}
function styleFor(pts){
  return {
    pointBackgroundColor: pts.map(p=>BAND_COLORS[p.band]||'#90a4ae'),
    pointBorderColor:     pts.map(p=>p.hold?'#b8860b':(p.conv?'#6a1b9a':'#ffffff')),
    pointBorderWidth:     pts.map(p=>(p.hold||p.conv)?2.5:1),
    pointRadius:          pts.map(p=>(p.hold||p.conv)?6:4.5),
  };
}
function mkScatter(id, pts, xTitle, yTitle, zone, emptyMsg){
  if(!HAS_CHART) return;
  if(!pts.length){ chartMsg(id, emptyMsg||'No data for this chart yet'); return; }
  new Chart(document.getElementById(id), {
    type:'scatter',
    data:{datasets:[{data:pts, ...styleFor(pts)}]},
    options:{
      responsive:true, maintainAspectRatio:false,
      plugins:{ legend:{display:false}, zone:zone,
        tooltip:{callbacks:{label:c=>{
          const d=c.raw;
          return `${d.t}${d.hold?' 🏦':''}${d.conv?' 🌟':''}: ${xTitle} ${d.x}, ${yTitle} ${d.y}`;
        }}}},
      scales:{ x:{title:{display:true,text:xTitle}}, y:{title:{display:true,text:yTitle}} }
    },
    plugins:[zonePlugin, zoneLabels]
  });
}

// 1) Score vs Upside — best zone: upside ≥ 20% AND score ≥ 65
mkScatter('c1', scatterPoints('up','c',-50,150), '% upside to avg target', 'Composite /100',
          {xMin:20, yMin:65, label:true},
          'Awaiting analyst price-target data — populates once an analyst fetch succeeds (Yahoo currently rate-limiting; cached data will accumulate over the next daily runs)');
// 2) Momentum vs Quality — zone: qual ≥ 15/22 AND mom ≥ 24/32
mkScatter('c2', scatterPoints('q','m',0,22), 'Quality /22', 'Momentum /32',
          {xMin:15, yMin:24, label:true});
// 3) Score vs Days to Earnings — zone: 0–10 days AND score ≥ 65
mkScatter('c3', scatterPoints('dte','c',0,90), 'Days to next earnings', 'Composite /100',
          {xMin:0, xMax:10, yMin:65, label:true},
          'Awaiting earnings-calendar data — arrives with analyst data');
// 5) Rule of 40 vs EV/EBITDA — zone: EV/EBITDA ≤ 15 AND Rule40 ≥ 40
mkScatter('c5', scatterPoints('ev','r40',0,60), 'EV/EBITDA', 'Rule of 40',
          {xMax:15, yMin:40, label:true},
          'Awaiting quality data — populates after a successful weekly quality refresh');

// 4) Top-20 composite bar, holdings gold, thresholds 65 & 80
const top20 = CD.filter(d=>d.c!=null).sort((a,b)=>b.c-a.c).slice(0,20);
if(HAS_CHART && !top20.length) chartMsg('c4','No composite scores yet');
if(HAS_CHART && top20.length) new Chart(document.getElementById('c4'), {
  type:'bar',
  data:{ labels: top20.map(d=>(d.hold?'🏦':'')+(d.conv?'🌟':'')+d.t),
         datasets:[{ data: top20.map(d=>d.c),
                     backgroundColor: top20.map(d=>d.hold?'#f9a825':(BAND_COLORS[d.band]||'#90a4ae')) }]},
  options:{ responsive:true, maintainAspectRatio:false,
    plugins:{ legend:{display:false},
              zone:{yMin:65, hlines:[{v:80,label:'80 · Exceptional',color:'#00695c'}]},
              tooltip:{callbacks:{label:c=>`Score ${c.raw}/100`}}},
    scales:{ y:{min:0, max:100, title:{display:true,text:'Composite /100'}},
             x:{ticks:{autoSkip:false, maxRotation:70, minRotation:45}} }},
  plugins:[zonePlugin]
});
</script>
<script>
// Click-to-sort on every column (numeric-aware, ▲/▼ toggle)
(function(){
  const tbl=document.getElementById('tbl'); if(!tbl||!tbl.tBodies||!tbl.tBodies.length) return;
  const ths=tbl.querySelectorAll('thead th');
  const num=s=>{const v=parseFloat(String(s).replace(/[%,$,]/g,'').replace(/[^0-9eE+.\-]/g,''));return isNaN(v)?null:v;};
  ths.forEach((th,ci)=>{
    th.style.cursor='pointer'; th.title='Click to sort';
    th.addEventListener('click',()=>{
      const dir = th.dataset.dir==='asc' ? 'desc' : 'asc';
      ths.forEach(h=>{ delete h.dataset.dir; h.textContent=h.textContent.replace(/ [\u25B2\u25BC]$/,''); });
      th.dataset.dir=dir; th.textContent += (dir==='asc'?' \u25B2':' \u25BC');
      const rows=[...tbl.tBodies[0].rows];
      rows.sort((a,b)=>{
        const A=(a.cells[ci]?a.cells[ci].textContent:'').trim();
        const B=(b.cells[ci]?b.cells[ci].textContent:'').trim();
        const nA=num(A), nB=num(B); let cmp;
        if(nA!=null&&nB!=null) cmp=nA-nB;
        else if(nA!=null) cmp=-1;
        else if(nB!=null) cmp=1;
        else cmp=A.localeCompare(B);
        return dir==='asc'?cmp:-cmp;
      });
      rows.forEach(r=>tbl.tBodies[0].appendChild(r));
    });
  });
})();
</script>
"""

# ── Generate HTML dashboard ────────────────────────────────────────────────────
def generate_dashboard(tickers, scores, anal_res, qual_res, tech_res,
                        regime, spy_ret12, run_time):
    print("  Generating HTML dashboard...")

    # % upside to average analyst target (needs a price: cached sheet price,
    # falling back to latest close from downloaded history)
    _upside = {}
    for t in tickers:
        sym   = t["ticker"]
        price = t.get("price") or tech_res.get(sym, {}).get("last_close")
        tgt   = anal_res.get(sym, {}).get("target_avg")
        if price and tgt and price > 0:
            _upside[sym] = round((tgt / price - 1) * 100, 1)

    # Compact payload embedded in the page for the screening charts
    chart_data = []
    for t in tickers:
        sym  = t["ticker"]
        sc   = scores.get(sym, {})
        tech = tech_res.get(sym, {})
        q    = qual_res.get(sym, {})
        a    = anal_res.get(sym, {})
        chart_data.append({
            "t":    sym,
            "c":    sc.get("composite"),
            "band": sc.get("band", ""),
            "m":    sc.get("momentum"),
            "q":    sc.get("quality"),
            "up":   _upside.get(sym),
            "dte":  a.get("days_to_earnings"),
            "r40":  q.get("rule40"),
            "ev":   q.get("ev_ebitda"),
            "hold": bool(t.get("holding")),
            "conv": bool(t.get("conviction")),
        })

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

        p52 = tech.get("pct_52w"); shp = tech.get("sharpe")
        p52_str = f"{p52:.0f}%" if isinstance(p52, (int,float)) else "—"
        shp_str = f"{shp:.2f}"  if isinstance(shp, (int,float)) else "—"
        # near a 52W high (>-5%) = green; deep below (<-30%) = red flag
        p52_bg  = ("#c8e6c9" if isinstance(p52,(int,float)) and p52 >= -5
                   else "#ffcdd2" if isinstance(p52,(int,float)) and p52 <= -30
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
  <td style="background:{p52_bg}">{p52_str}</td>
  <td>{shp_str}</td>
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
  <th>ANAL/13</th><th>RS/10</th><th>VAL/6</th><th>%52W HI</th><th>SHARPE</th>
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
  Sources: Yahoo Finance · Twelve Data (fallback) · ROIC.ai · Google Finance
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

    # Insert the screening charts section (with live data) before the legend
    # Insert the screening charts section (with live data) ABOVE the table —
    # screening-first layout. Sanitize: no NaN/Inf may leak into the page JS.
    def _clean(v):
        if isinstance(v, float) and not math.isfinite(v):
            return None
        return round(v, 2) if isinstance(v, float) else v
    chart_data = [{k: _clean(v) for k, v in d.items()} for d in chart_data]
    # Zone-qualifier summary strip (server-side, so it works even if JS fails)
    def _zpill(title, thresh, members, missing_msg):
        if members:
            shown = ", ".join(members[:12]) + (f" +{len(members)-12} more" if len(members) > 12 else "")
            return (f'<div class="zpill"><b>{title}</b>{thresh}<br>'
                    f'<span class="zt">{shown}</span></div>')
        return (f'<div class="zpill zempty"><b>{title}</b>{thresh}<br>{missing_msg}</div>')
    _has = lambda k: any(d.get(k) is not None for d in chart_data)
    z1 = [d["t"] for d in chart_data if (d.get("c") or 0) >= 65 and (d.get("up") is not None and d["up"] >= 20)]
    z2 = [d["t"] for d in chart_data if (d.get("q") or 0) >= 15 and (d.get("m") or 0) >= 24]
    z3 = [d["t"] for d in chart_data if (d.get("c") or 0) >= 65 and d.get("dte") is not None and 0 <= d["dte"] <= 10]
    z4 = [d["t"] for d in chart_data if (d.get("c") or 0) >= 80]
    z5 = [d["t"] for d in chart_data if d.get("r40") is not None and d["r40"] >= 40
          and d.get("ev") is not None and 0 < d["ev"] <= 15]
    zone_html = ('<div class="zonebar">'
        + _zpill("🎯 Score + upside", " (score ≥65, upside ≥20%)", z1,
                 "awaiting analyst data" if not _has("up") else "no qualifiers today")
        + _zpill("💪 Momentum + quality", " (mom ≥24/32, qual ≥15/22)", z2,
                 "awaiting quality data" if not _has("r40") else "no qualifiers today")
        + _zpill("📅 Catalyst window", " (score ≥65, earnings ≤10d)", z3,
                 "awaiting earnings dates" if not _has("dte") else "no qualifiers today")
        + _zpill("🚀 Exceptional", " (score ≥80)", z4, "no qualifiers today")
        + _zpill("💎 Quality value", " (Rule40 ≥40, EV/EBITDA ≤15)", z5,
                 "awaiting quality data" if not (_has("r40") and _has("ev")) else "no qualifiers today")
        + "</div>")
    n_all   = len(tickers)
    n_tech  = sum(1 for t in tickers if tech_res.get(t["ticker"], {}).get("ret12m") is not None)
    n_anal  = sum(1 for t in tickers if anal_res.get(t["ticker"]))
    n_qual  = sum(1 for t in tickers if qual_res.get(t["ticker"]))
    completeness = (f'<div style="padding:6px 20px;font-size:11px;color:#78909c">'
                    f'Data completeness — technicals {n_tech}/{n_all} · '
                    f'analyst {n_anal}/{n_all} · quality {n_qual}/{n_all}'
                    f'{" · gaps auto-repair on the overnight run" if min(n_tech,n_anal,n_qual) < n_all else ""}</div>')
    charts_html = CHARTS_TEMPLATE.replace("__CHART_DATA__",
                                          json.dumps(chart_data, allow_nan=False))
    band_rows = "".join(
        f'<tr><td><span style="display:inline-block;width:11px;height:11px;'
        f'border-radius:3px;background:{c};margin-right:6px"></span>{b}</td>'
        f'<td>{rng}</td><td>{desc}</td></tr>'
        for b, rng, c, desc in SCORE_BANDS)
    pillar_rows = "".join(
        f"<tr><td><b>{n}</b></td><td>{mx}</td><td>{what}</td><td>{note}</td></tr>"
        for n, mx, what, note in SCORE_KEY)
    risk_rows = "".join(f"<tr><td><b>{n}</b></td><td colspan=3>{what}</td></tr>"
                        for n, what in RISK_KEY)
    key_html = (
      '<div style="padding:0 20px 14px">'
      '<details style="background:#fff;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,.12);padding:12px 16px">'
      '<summary style="cursor:pointer;font-weight:bold;color:#0f3460;font-size:14px">'
      '📖 Scoring key — what each score measures &amp; the bands</summary>'
      '<div style="font-size:12px;margin-top:10px">'
      '<p style="margin-bottom:6px"><b>Composite /100</b> = sum of the seven pillars below. '
      'Bands:</p>'
      f'<table style="border-collapse:collapse;margin-bottom:12px">{band_rows}</table>'
      f'<table style="border-collapse:collapse;margin-bottom:12px">'
      f'<tr style="color:#0f3460"><th align=left>Pillar</th><th align=left>Max</th>'
      f'<th align=left>What it measures</th><th align=left>Notes</th></tr>{pillar_rows}</table>'
      '<p style="margin-bottom:6px;color:#0f3460"><b>Risk metrics (Technicals tab / table columns)</b></p>'
      f'<table style="border-collapse:collapse">{risk_rows}</table>'
      '<style>details td,details th{padding:3px 10px 3px 0;vertical-align:top;border-bottom:1px solid #eceff1}</style>'
      '</div></details></div>')
    charts_html = charts_html.replace("__ZONE_SUMMARY__", completeness + zone_html)
    charts_html = charts_html + key_html
    html = html.replace('<div class="wrap">', charts_html + '\n<div class="wrap">', 1)

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html)

    # Save data.json for future use.
    # IMPORTANT (v12 fix): the daily run re-loads quality data from here using
    # q_-prefixed keys, and the weekly quality run re-loads technicals/analyst
    # using t_/a_ prefixes — but these prefixed fields were never actually
    # written, so the cross-run cache silently loaded Nones (quality stuck at
    # the 1.5 default even after quality runs). Now written properly.
    def _prefixed(d, prefix):
        return {f"{prefix}{k}": v for k, v in (d or {}).items()
                if not k.startswith("_")}

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
                # Risk metrics + valuation for charts/screening
                "target_avg": anal_res.get(t["ticker"], {}).get("target_avg"),
                "upside":     _upside.get(t["ticker"]),
                "beta":       tech_res.get(t["ticker"], {}).get("beta"),
                "vol_ann":    tech_res.get(t["ticker"], {}).get("vol_ann"),
                "max_dd":     tech_res.get(t["ticker"], {}).get("max_dd"),
                "sharpe":     tech_res.get(t["ticker"], {}).get("sharpe"),
                "pct_52w":    tech_res.get(t["ticker"], {}).get("pct_52w"),
                "ev_ebitda":  qual_res.get(t["ticker"], {}).get("ev_ebitda"),
                # Prefixed caches (cross-run contract — do not remove)
                **_prefixed(qual_res.get(t["ticker"]), "q_"),
                **_prefixed(tech_res.get(t["ticker"]), "t_"),
                **_prefixed(anal_res.get(t["ticker"]), "a_"),
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
    print(f"Investment Watchlist Refresh {VERSION} — {run_time} UTC")
    print(f"Mode: {REFRESH_MODE} | Depth: {REFRESH_DEPTH} | Finnhub: {'configured' if FINNHUB_KEY else 'not configured'}")
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
    # Validate the optional Finnhub key up-front so a bad secret is visible in
    # the first lines of every log, not buried in the repair section.
    if FINNHUB_KEY:
        try:
            _fr = requests.get("https://finnhub.io/api/v1/stock/recommendation",
                               params={"symbol": "AAPL", "token": FINNHUB_KEY},
                               timeout=10)
            if _fr.status_code == 200:
                print("  FINNHUB_API_KEY: valid ✅", flush=True)
            elif _fr.status_code == 401:
                print("  ⚠️ FINNHUB_API_KEY: REJECTED by Finnhub (HTTP 401 Invalid "
                      "API key). Finnhub keys are ~20 chars — re-copy from "
                      "finnhub.io/dashboard into the GitHub secret (no spaces).", flush=True)
            else:
                print(f"  FINNHUB_API_KEY: unexpected HTTP {_fr.status_code} "
                      f"on validation ping", flush=True)
        except Exception as _e:
            print(f"  FINNHUB_API_KEY: could not validate ({_e})", flush=True)

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

    print("\nSTEP 2+3: Price history, market regime & technical indicators...")
    if REFRESH_MODE == "quality":
        print("  SKIPPING technicals (quality mode)")
        tech_res  = {}
        history   = fetch_all_history([])  # SPY only, for regime
        spy_ret12, regime = compute_regime_from_history(history)
    else:
        history   = fetch_all_history(tickers)
        spy_ret12, regime = compute_regime_from_history(history)
        tech_res  = compute_technicals(history, tickers)

    print("\nSTEP 4: Analyst data (Yahoo quoteSummary)...")
    if REFRESH_MODE == "quality":
        print("  SKIPPING analyst data (quality mode)")
        anal_res = {}
    else:
        # Load last run's analyst values from data.json (a_ prefixed keys,
        # written by v12) so Yahoo-blocked runs coast on cached data.
        anal_cache = {}
        try:
            with open("data.json") as f:
                _c = json.load(f)
            for s in _c.get("stocks", []):
                d = {k[2:]: v for k, v in s.items()
                     if k.startswith("a_") and v is not None}
                if d and s.get("ticker"):
                    anal_cache[s["ticker"]] = d
            print(f"  Analyst cache loaded for {len(anal_cache)} stocks (from data.json)")
        except Exception:
            print("  No analyst cache available yet (first v12 run)")
        anal_res = fetch_analyst_data(tickers, anal_cache)

    print("\nSTEP 5: Quality metrics (ROIC.ai — ~2h45m in quality mode)...")
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

    if REFRESH_MODE != "quality":
        print(f"\nSTEP 6a: Data completeness audit & repair (depth={REFRESH_DEPTH})...")
        repair_data(tickers, history, tech_res, anal_res, qual_res, REFRESH_DEPTH)

    print("\nSTEP 6: Computing composite scores...")
    # v11: price comes from the value cached during get_tickers() (col F was
    # already read in the initial get_all_values), falling back to the latest
    # close from the downloaded history. v10 made one Sheets READ call per
    # ticker here (167 calls vs the 60/min quota).
    scores = {}
    for t in tickers:
        price = t.get("price")
        if price is None:
            price = tech_res.get(t["ticker"], {}).get("last_close")
        scores[t["ticker"]] = compute_score(
            tech_res.get(t["ticker"], {}),
            anal_res.get(t["ticker"], {}),
            qual_res.get(t["ticker"], {}),
            price, spy_ret12, regime
        )
    print(f"  Scores computed for {len(scores)} stocks")
    # Debug: show first 5 scores with breakdown
    print("  Sample scores (first 5):")
    for sym, sc in list(scores.items())[:5]:
        print(f"    {sym}: {sc['composite']}/100 | mom={sc['momentum']} qual={sc['quality']} earn={sc['earnings']} anal={sc['analyst']} rs={sc['rel_str']}", flush=True)
        print(f"      price={sc.get('_price')} r12={sc.get('_r12')} r6={sc.get('_r6')} sma50={sc.get('_sma50')} sma200={sc.get('_sma200')}", flush=True)

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
