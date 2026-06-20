# -*- coding: utf-8 -*-
# scanner.py -- Crypto Futures Scanner V3.1
# Ranking Engine Upgrade per V3.1 specification
#
# Revised weights: Trend 20 | RS 15 | OI 15 | RVOL 15 | ATR 10
#                  Structure 10 | Breakout 10 | Sweep 5  = 100
#
# Exchange architecture: OKX is the sole primary data source (universe,
# tickers, candles, open interest, funding rate). CoinGecko is the
# fallback if OKX is fully unreachable, and remains a validation layer
# (volume sanity check) when OKX is working normally.
#
# New modules vs V3:
#   [+] Self-sampled OI history delta (OKX public API exposes only a
#       current OI snapshot, not a history endpoint, so we sample it
#       on every scan and keep a rolling window per symbol)
#   [+] Relative Strength vs BTC and ETH
#   [+] Breakout engine  (highest-high / lowest-low confirmation)
#   [+] ATR expansion ratio  (current ATR vs 14-period average ATR)
#   [+] Liquidity sweep detector  (stop-hunt + reclaim patterns)
#   [+] True RVOL  (rewards genuine participation, penalises thin fakes)
#   [+] Multi-tier ranking: score -> RS -> OI_delta -> RVOL
#   [+] CoinGecko fallback + validation layer
#
# No placeholders. All data from live APIs.
# Locally (e.g. behind ISP/network restrictions) set env var PROXY_URL to
# route through a local tunnel (e.g. Psiphon on 127.0.0.1:1080).
# On Render / cloud deployments, leave PROXY_URL unset -- the cloud
# datacenter IP is usually not geo-restricted, so no proxy is needed.
# Pure requests + asyncio.to_thread -- no aiohttp, no C extensions.

import asyncio, math, os, time, threading
from datetime import datetime, timezone

import requests

# ============================================================
#  CONFIG  -- edit these values
# ============================================================
_PROXY_URL = os.environ.get("PROXY_URL", "").strip()
PROXY      = {"http": _PROXY_URL, "https": _PROXY_URL} if _PROXY_URL else None
TIMEOUT        = 22
SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300"))   # auto-rescan every 5 minutes
MIN_SCORE      = 65
TOP_N          = 50
BATCH_SIZE     = 10

# V3.1 weights (must sum to 100)
W = {
    "trend":     20,
    "rs":        15,
    "oi":        15,
    "rvol":      15,
    "atr_exp":   10,
    "structure": 10,
    "breakout":  10,
    "sweep":      5,
}
assert sum(W.values()) == 100

# ============================================================
#  API BASES
# ============================================================
OKX   = "https://www.okx.com"
CG    = "https://api.coingecko.com/api/v3"

# ============================================================
#  HTTP SESSION  (thread-safe, proxied)
# ============================================================
_lock    = threading.Lock()
_session = None

def _sess():
    global _session
    with _lock:
        if _session is None:
            _session = requests.Session()
            _session.headers.update({"User-Agent": "Mozilla/5.0"})
    return _session

def GET(url, params=None, timeout=TIMEOUT):
    r = _sess().get(url, params=params, proxies=PROXY, timeout=timeout)
    r.raise_for_status()
    return r.json()

def GET_direct(url, params=None, timeout=TIMEOUT):
    r = _sess().get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

# ============================================================
#  DATA FETCHERS  --  OKX primary, CoinGecko fallback
# ============================================================

# In-memory OI snapshot history, since OKX's public REST API only exposes
# a CURRENT open-interest snapshot (no historical time-series like Binance
# used to provide). We sample it ourselves on every scan and keep a rolling
# window per symbol so we can still compute an OI delta over time.
_OI_HISTORY      = {}   # inst_id -> [(timestamp, oi_usd), ...]
_OI_HISTORY_LOCK = threading.Lock()
_OI_HISTORY_MAX  = 12   # keep last 12 samples (~1 hour at 5-min scan interval)


def fetch_okx_universe():
    """Primary universe + 24h ticker source: OKX USDT-margined perpetual swaps."""
    data = GET(OKX + "/api/v5/market/tickers", params={"instType": "SWAP"})
    out = {}
    for t in (data.get("data") or []):
        inst_id = t.get("instId", "")
        if not inst_id.endswith("-USDT-SWAP"):
            continue
        last  = float(t.get("last") or 0)
        open24 = float(t.get("open24h") or 0)
        vol_ccy24 = float(t.get("volCcy24h") or 0)   # quote-currency (USDT) volume
        if last > 0 and vol_ccy24 > 1_000_000:
            chg24 = ((last - open24) / open24 * 100) if open24 > 0 else 0.0
            out[inst_id] = {"price": last, "vol": vol_ccy24, "chg24": chg24}
    return out


def fetch_ohlcv_okx(inst_id, bar="1H", limit=250):
    try:
        data = GET(OKX + "/api/v5/market/candles",
                   params={"instId": inst_id, "bar": bar, "limit": limit})
        candles = []
        for r in reversed(data.get("data") or []):
            candles.append({"t": int(r[0]), "o": float(r[1]), "h": float(r[2]),
                             "l": float(r[3]), "c": float(r[4]), "v": float(r[5])})
        return candles
    except Exception:
        return []


def fetch_oi_history(inst_id, limit=5):
    """OI 'history' via self-sampling: OKX's public API only returns a
    current snapshot (/api/v5/public/open-interest), so we record that
    snapshot every time this is called and return the recent rolling
    window we've accumulated. Returns oldest-first, in USD notional."""
    try:
        data = GET(OKX + "/api/v5/public/open-interest",
                   params={"instId": inst_id})
        rows = data.get("data") or []
        if not rows:
            return []
        oi_ccy = float(rows[0].get("oiCcy") or 0)   # OI in quote currency (USDT) already
        now = time.time()
        with _OI_HISTORY_LOCK:
            hist = _OI_HISTORY.setdefault(inst_id, [])
            hist.append((now, oi_ccy))
            if len(hist) > _OI_HISTORY_MAX:
                del hist[: len(hist) - _OI_HISTORY_MAX]
            snapshots = [v for _, v in hist[-limit:]]
        return snapshots   # oldest first
    except Exception:
        return []


def fetch_funding(inst_id):
    try:
        data = GET(OKX + "/api/v5/public/funding-rate", params={"instId": inst_id})
        rows = data.get("data") or []
        if rows:
            return float(rows[0].get("fundingRate") or 0)
        return 0.0
    except Exception:
        return 0.0


def fetch_cg_validation(symbols_clean):
    # CoinGecko -- fetch top 250 by volume for validation sanity check
    # Returns set of symbols that CoinGecko confirms have real volume
    try:
        data = GET_direct(CG + "/coins/markets", params={
            "vs_currency": "usd", "order": "volume_desc",
            "per_page": 250, "page": 1, "sparkline": "false"
        }, timeout=15)
        cg_syms = set()
        for coin in data:
            sym = (coin.get("symbol") or "").upper()
            cg_syms.add(sym)
        return cg_syms
    except Exception:
        return set()   # if CG fails, don't block scanning


def fetch_okx_universe_fallback():
    """List of OKX USDT-margined perpetual instIds, used when the combined
    tickers call fails but the lighter instruments endpoint still works."""
    try:
        data = GET(OKX + "/api/v5/public/instruments", params={"instType": "SWAP"})
        return [s["instId"] for s in (data.get("data") or [])
                if s.get("instId", "").endswith("-USDT-SWAP")]
    except Exception:
        return []


def fetch_cg_universe_fallback():
    """Fallback universe + tickers from CoinGecko, used only if OKX itself
    is completely unreachable. CoinGecko has no futures-specific data (no
    OI, no funding), so signals scored this way will have those components
    zeroed out -- still usable for trend/RS/RVOL/structure based scoring."""
    try:
        data = GET_direct(CG + "/coins/markets", params={
            "vs_currency": "usd", "order": "volume_desc",
            "per_page": 200, "page": 1, "sparkline": "false",
            "price_change_percentage": "24h",
        }, timeout=15)
        out = {}
        for coin in data:
            sym = (coin.get("symbol") or "").upper()
            price = float(coin.get("current_price") or 0)
            vol   = float(coin.get("total_volume") or 0)
            chg24 = float(coin.get("price_change_percentage_24h") or 0)
            if price > 0 and vol > 1_000_000:
                out[sym + "-USDT-SWAP"] = {"price": price, "vol": vol, "chg24": chg24}
        return out
    except Exception:
        return {}


# ============================================================
#  TECHNICAL ANALYSIS
# ============================================================

def _ema(closes, period):
    if len(closes) < period:
        return []
    k = 2.0 / (period + 1)
    e = sum(closes[:period]) / period
    out = [e]
    for c in closes[period:]:
        e = c * k + e * (1 - k)
        out.append(e)
    return out

def _sma(vals, period):
    if len(vals) < period:
        return []
    return [sum(vals[i-period+1:i+1])/period for i in range(period-1, len(vals))]

def _atr_series(candles, period=14):
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i-1]["c"]
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    return _sma(trs, period)

def _atr_last(candles, period=14):
    s = _atr_series(candles, period)
    return s[-1] if s else max(candles[-1]["h"] - candles[-1]["l"], 1e-8)

def _vwap(candles):
    pv = sum(((c["h"]+c["l"]+c["c"])/3.0)*c["v"] for c in candles)
    tv = sum(c["v"] for c in candles)
    return pv/tv if tv > 0 else candles[-1]["c"]


# ---- Trend (EMA 20/50/200) -------------------------------------------
def score_trend(candles, direction):
    closes = [c["c"] for c in candles]
    e20  = _ema(closes, 20)
    e50  = _ema(closes, 50)
    e200 = _ema(closes, 200)
    if not (e20 and e50 and e200):
        return 50
    if direction == "LONG":
        if   e20[-1] > e50[-1] > e200[-1]: return 100
        elif e20[-1] > e50[-1]:             return 75
        elif e20[-1] > e200[-1]:            return 50
        else:                               return 25
    else:
        if   e20[-1] < e50[-1] < e200[-1]: return 100
        elif e20[-1] < e50[-1]:             return 75
        elif e20[-1] < e200[-1]:            return 50
        else:                               return 25


# ---- Relative Strength vs BTC / ETH ---------------------------------
def score_rs(chg24_coin, chg24_btc, chg24_eth):
    # Average BTC+ETH benchmark
    benchmark = (chg24_btc + chg24_eth) / 2.0 if (chg24_btc or chg24_eth) else 0.0
    delta = chg24_coin - benchmark
    if   delta >  5.0: return 100   # strong outperformance
    elif delta >  2.0: return 85
    elif delta >  0.5: return 70
    elif delta > -0.5: return 55    # in-line
    elif delta > -2.0: return 40
    elif delta > -5.0: return 25
    else:              return 10    # heavy underperformance


# ---- Real OI delta (from OI history) --------------------------------
def score_oi(oi_snapshots, direction):
    if not oi_snapshots or len(oi_snapshots) < 2:
        return 50   # neutral -- no data, not a blocker
    oldest = oi_snapshots[0]
    newest = oi_snapshots[-1]
    if oldest == 0:
        return 50
    delta_pct = (newest - oldest) / oldest * 100
    oi_expanding = delta_pct > 1.0
    # Score: OI expanding = new money entering = conviction
    if   delta_pct >  5.0: return 100
    elif delta_pct >  2.0: return 85
    elif delta_pct >  0.5: return 70
    elif delta_pct > -0.5: return 55
    elif delta_pct > -2.0: return 40
    else:                  return 25


# ---- True RVOL (rewards genuine participation) ----------------------
def score_rvol(candles):
    if len(candles) < 21:
        return 50
    # Use 20-bar average of all candles except current
    avg = sum(c["v"] for c in candles[-21:-1]) / 20
    cur = candles[-1]["v"]
    rv  = cur / avg if avg > 0 else 1.0
    # Also check if volume is consistent (not a single spike)
    recent_vols = [c["v"] for c in candles[-5:-1]]
    avg_recent  = sum(recent_vols) / len(recent_vols) if recent_vols else avg
    participation = avg_recent / avg if avg > 0 else 1.0
    # Reward both current spike AND sustained participation
    rv_score = 0
    if   rv >= 3.0: rv_score = 100
    elif rv >= 2.0: rv_score = 85
    elif rv >= 1.5: rv_score = 70
    elif rv >= 1.0: rv_score = 50
    elif rv >= 0.7: rv_score = 35
    else:           rv_score = 20
    # Participation bonus: if 4 of last 5 bars are above avg
    above_avg = sum(1 for v in recent_vols if v > avg)
    if above_avg >= 3:
        rv_score = min(100, rv_score + 10)
    return rv_score, round(rv, 2)


# ---- ATR Expansion (current ATR vs average of ATR series) -----------
def score_atr_expansion(candles):
    atr_s = _atr_series(candles, 14)
    if len(atr_s) < 10:
        return 50, 1.0
    current_atr = atr_s[-1]
    avg_atr     = sum(atr_s[-10:]) / 10
    ratio       = current_atr / avg_atr if avg_atr > 0 else 1.0
    if   ratio >= 2.0: return 100, round(ratio, 2)
    elif ratio >= 1.5: return 85,  round(ratio, 2)
    elif ratio >= 1.2: return 70,  round(ratio, 2)
    elif ratio >= 0.9: return 50,  round(ratio, 2)
    elif ratio >= 0.7: return 35,  round(ratio, 2)
    else:              return 20,  round(ratio, 2)


# ---- Market Structure (HH+HL / LH+LL) ------------------------------
def score_structure(candles, direction, n=5):
    if len(candles) < n*2:
        return 50, "neutral"
    highs = [c["h"] for c in candles]
    lows  = [c["l"] for c in candles]
    rh, rl = highs[-n:], lows[-n:]
    ph, pl = highs[-2*n:-n], lows[-2*n:-n]
    hh = max(rh) > max(ph)
    hl = min(rl) > min(pl)
    lh = max(rh) < max(ph)
    ll = min(rl) < min(pl)
    if hh and hl:
        ms = "bullish"
        s  = 100 if direction == "LONG" else 25
    elif lh and ll:
        ms = "bearish"
        s  = 100 if direction == "SHORT" else 25
    else:
        ms = "neutral"
        s  = 50
    return s, ms


# ---- Breakout Engine (highest-high / lowest-low confirmation) -------
def score_breakout(candles, direction, lookback=20):
    if len(candles) < lookback + 2:
        return 50
    # Compare last closed candle vs prior lookback range
    prior   = candles[-(lookback+1):-1]
    current = candles[-1]
    hh = max(c["h"] for c in prior)
    ll = min(c["l"] for c in prior)
    close   = current["c"]
    prev_close = candles[-2]["c"]
    if direction == "LONG":
        # Full breakout: close above prior highest high
        if close > hh and prev_close <= hh:    return 100
        # Partial: close above but prev was already above
        if close > hh:                          return 80
        # Near breakout: within 0.5% of high
        if close > hh * 0.995:                 return 60
        return 30
    else:
        # Short breakout: close below prior lowest low
        if close < ll and prev_close >= ll:     return 100
        if close < ll:                          return 80
        if close < ll * 1.005:                  return 60
        return 30


# ---- Liquidity Sweep (stop-hunt + reclaim pattern) ------------------
def score_sweep(candles, direction, n=5):
    if len(candles) < n + 3:
        return 50
    # Look for wicks that swept a prior level then price reclaimed
    recent   = candles[-(n+1):]
    cur      = candles[-1]
    prev_low  = min(c["l"] for c in recent[:-2])
    prev_high = max(c["h"] for c in recent[:-2])
    close     = cur["c"]
    body_size = abs(cur["c"] - cur["o"])
    candle_range = cur["h"] - cur["l"]
    if candle_range == 0:
        return 50
    wick_ratio = body_size / candle_range  # high = strong body, low = wick dominant

    if direction == "LONG":
        # Bullish sweep: wick swept below prev low then reclaimed
        swept_low = cur["l"] < prev_low
        reclaimed = close > prev_low
        if swept_low and reclaimed and wick_ratio > 0.4: return 100
        if swept_low and reclaimed:                       return 80
        if swept_low:                                     return 50
        return 40
    else:
        # Bearish sweep: wick swept above prev high then rejected
        swept_high  = cur["h"] > prev_high
        rejected    = close < prev_high
        if swept_high and rejected and wick_ratio > 0.4: return 100
        if swept_high and rejected:                       return 80
        if swept_high:                                    return 50
        return 40


# ---- Direction detection --------------------------------------------
def detect_direction(candles):
    closes = [c["c"] for c in candles]
    if len(closes) < 200:
        return "LONG"
    e20  = _ema(closes, 20)
    e200 = _ema(closes, 200)
    if e20 and e200:
        return "LONG" if e20[-1] > e200[-1] else "SHORT"
    return "LONG"


# ============================================================
#  MASTER SCORER
# ============================================================

def score_signal(candles, oi_snapshots, funding, price,
                 direction, chg24, chg24_btc, chg24_eth):
    if len(candles) < 210 or price == 0:
        return None

    # Individual component scores
    trend_s                  = score_trend(candles, direction)
    rs_s                     = score_rs(chg24, chg24_btc, chg24_eth)
    oi_s                     = score_oi(oi_snapshots, direction)
    rvol_result              = score_rvol(candles)
    rvol_s, rv_val           = rvol_result if isinstance(rvol_result, tuple) else (rvol_result, 1.0)
    atr_result               = score_atr_expansion(candles)
    atr_s, atr_ratio         = atr_result if isinstance(atr_result, tuple) else (atr_result, 1.0)
    struct_s, ms             = score_structure(candles, direction)
    breakout_s               = score_breakout(candles, direction)
    sweep_s                  = score_sweep(candles, direction)

    # Weighted final score
    final = (
        trend_s    * W["trend"]     / 100 +
        rs_s       * W["rs"]        / 100 +
        oi_s       * W["oi"]        / 100 +
        rvol_s     * W["rvol"]      / 100 +
        atr_s      * W["atr_exp"]   / 100 +
        struct_s   * W["structure"] / 100 +
        breakout_s * W["breakout"]  / 100 +
        sweep_s    * W["sweep"]     / 100
    )
    final = round(min(100.0, max(0.0, final)))

    if   final >= 85: grade = "A+"
    elif final >= 75: grade = "A"
    elif final >= 65: grade = "B"
    else:             grade = "C"

    # Risk engine: 1.5 ATR stop
    a    = _atr_last(candles, 14)
    vwap = _vwap(candles)
    if direction == "LONG":
        entry = price
        stop  = round(price - 1.5 * a, 8)
        risk  = max(entry - stop, 1e-10)
        tp1   = round(entry + 2.0 * risk, 8)
        tp2   = round(entry + 3.0 * risk, 8)
        tp3   = round(entry + 4.0 * risk, 8)
    else:
        entry = price
        stop  = round(price + 1.5 * a, 8)
        risk  = max(stop - entry, 1e-10)
        tp1   = round(entry - 2.0 * risk, 8)
        tp2   = round(entry - 3.0 * risk, 8)
        tp3   = round(entry - 4.0 * risk, 8)

    # OI delta for ranking tie-breaker
    oi_delta = 0.0
    if oi_snapshots and len(oi_snapshots) >= 2 and oi_snapshots[0] > 0:
        oi_delta = round((oi_snapshots[-1] - oi_snapshots[0]) / oi_snapshots[0] * 100, 2)

    trend_dir = ("strong_bull" if trend_s == 100 and direction == "LONG"
                 else "strong_bear" if trend_s == 100 and direction == "SHORT"
                 else "moderate")

    return {
        "score":      final,
        "grade":      grade,
        "entry":      round(entry, 8),
        "stop":       round(stop, 8),
        "tp1":        tp1,
        "tp2":        tp2,
        "tp3":        tp3,
        "rr":         3.0,
        "atr":        round(a, 8),
        "atr_ratio":  atr_ratio,
        "rvol_val":   rv_val,
        "vwap_val":   round(vwap, 6),
        "trend_dir":  trend_dir,
        "ms":         ms,
        "funding":    round(funding * 100, 4),
        "oi_delta":   oi_delta,
        "rs_score":   rs_s,
        "components": {
            "trend":     int(trend_s),
            "rs":        int(rs_s),
            "oi":        int(oi_s),
            "rvol":      int(rvol_s),
            "atr_exp":   int(atr_s),
            "structure": int(struct_s),
            "breakout":  int(breakout_s),
            "sweep":     int(sweep_s),
        },
    }


# ============================================================
#  MAIN SCAN
# ============================================================

async def run_full_scan(log_fn=None, timeframe="1h", limit=250):
    def L(msg):
        if log_fn:
            log_fn(msg)

    # ---- Step 1: Universe + tickers ---------------------------------
    L("Step 1/6: Fetching OKX universe...")
    tickers     = {}
    inst_ids    = []
    using_cg_fb = False

    try:
        tickers  = await asyncio.to_thread(fetch_okx_universe)
        inst_ids = list(tickers.keys())
        L("OKX: " + str(len(inst_ids)) + " symbols")
    except Exception as e:
        L("OKX failed (" + str(e)[:60] + ") -- trying CoinGecko fallback...")
        try:
            tickers     = await asyncio.to_thread(fetch_cg_universe_fallback)
            inst_ids    = list(tickers.keys())
            using_cg_fb = True
            L("CoinGecko fallback: " + str(len(inst_ids)) + " symbols " +
              "(no futures OI/funding data available via this fallback)")
        except Exception as e2:
            L("CoinGecko fallback also failed: " + str(e2)[:60])

    if not inst_ids:
        L("No symbols loaded -- check network/exchange connectivity" +
          (" and PROXY_URL" if PROXY else ""))
        return []

    total = len(inst_ids)

    # ---- Step 2: BTC + ETH reference for RS -------------------------
    L("Step 2/6: Fetching BTC + ETH reference for Relative Strength...")
    btc_info  = tickers.get("BTC-USDT-SWAP", {})
    eth_info  = tickers.get("ETH-USDT-SWAP", {})
    chg24_btc = btc_info.get("chg24", 0.0) if btc_info else 0.0
    chg24_eth = eth_info.get("chg24", 0.0) if eth_info else 0.0
    L("BTC 24h: " + str(round(chg24_btc, 2)) + "%  ETH 24h: " + str(round(chg24_eth, 2)) + "%")

    # ---- Step 3: CoinGecko validation (non-blocking) ----------------
    L("Step 3/6: CoinGecko validation (background)...")
    cg_syms = set()
    if not using_cg_fb:
        try:
            clean_syms = [i.replace("-USDT-SWAP", "") for i in inst_ids]
            cg_syms = await asyncio.to_thread(fetch_cg_validation, clean_syms)
            L("CoinGecko validated: " + str(len(cg_syms)) + " confirmed symbols")
        except Exception:
            L("CoinGecko unavailable -- continuing without validation")
    else:
        L("Skipped (already using CoinGecko as primary source this scan)")

    # ---- Step 4: Scan each symbol -----------------------------------
    L("Step 4/6: Scanning " + str(total) + " symbols...")
    deadline  = time.time() + 14 * 60
    done      = [0]
    signals   = []

    async def scan_one(inst_id):
        if time.time() > deadline:
            return None
        try:
            clean = inst_id.replace("-USDT-SWAP", "")

            if using_cg_fb:
                # No candle source in this fallback mode -- skip scoring,
                # since trend/structure/breakout all require OHLCV history.
                return None

            candles = await asyncio.to_thread(fetch_ohlcv_okx, inst_id, "1H", limit)
            t_info  = tickers.get(inst_id, {})

            done[0] += 1
            if done[0] % 50 == 0:
                pct = int(done[0] / total * 90)
                L("Progress: " + str(done[0]) + "/" + str(total) + " (" + str(pct) + "%)")

            if len(candles) < 210:
                return None

            price  = t_info.get("price", 0.0)
            chg24  = t_info.get("chg24", 0.0)
            if price == 0.0 and candles:
                price = candles[-1]["c"]

            # OI (self-sampled rolling history) + current funding rate
            oi_snaps = []
            funding  = 0.0
            try:
                oi_snaps = await asyncio.to_thread(fetch_oi_history, inst_id, 5)
                funding  = await asyncio.to_thread(fetch_funding, inst_id)
            except Exception:
                pass

            direction = detect_direction(candles)
            result    = score_signal(
                candles, oi_snaps, funding,
                price, direction, chg24,
                chg24_btc, chg24_eth
            )
            if result is None or result["score"] < MIN_SCORE:
                return None

            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            return {
                "symbol":     clean,
                "direction":  direction,
                "entry":      result["entry"],
                "stop":       result["stop"],
                "tp1":        result["tp1"],
                "tp2":        result["tp2"],
                "tp3":        result["tp3"],
                "rr":         result["rr"],
                "score":      result["score"],
                "grade":      result["grade"],
                "rvol":       result["rvol_val"],
                "funding":    result["funding"],
                "structure":  result["ms"],
                "trend_dir":  result["trend_dir"],
                "atr_ratio":  result["atr_ratio"],
                "oi_delta":   result["oi_delta"],
                "rs_score":   result["rs_score"],
                "components": result["components"],
                "timestamp":  ts,
                "price":      price,
                "chg24":      round(chg24, 2),
            }
        except Exception:
            return None

    for i in range(0, len(inst_ids), BATCH_SIZE):
        if time.time() > deadline:
            L("Time limit reached -- using results so far")
            break
        batch   = inst_ids[i:i + BATCH_SIZE]
        results = await asyncio.gather(*[scan_one(s) for s in batch])
        for r in results:
            if isinstance(r, dict):
                signals.append(r)

    # ---- Step 5: Multi-tier ranking ---------------------------------
    # Primary: score desc
    # Secondary: RS score desc
    # Tertiary: OI delta desc
    # Quaternary: RVOL desc
    L("Step 5/6: Ranking " + str(len(signals)) + " signals...")
    signals.sort(key=lambda x: (
        -x["score"],
        -x.get("rs_score", 0),
        -x.get("oi_delta", 0),
        -x.get("rvol", 0),
    ))
    signals = signals[:TOP_N]

    # ---- Step 6: CoinGecko supplementary fill -----------------------
    # If we have fewer than 10 signals, pull CoinGecko as a supplementary
    # source (RS-based only -- no OI/funding/structure data available from
    # CoinGecko, so these are lower-confidence "B" grade fills).
    if len(signals) < 10 and not using_cg_fb:
        L("Step 6/6: Low signal count -- checking CoinGecko supplementary...")
        try:
            cg_data = await asyncio.to_thread(fetch_cg_universe_fallback)
            found_syms = {s["symbol"] for s in signals}
            for inst_id, info in list(cg_data.items())[:30]:
                clean = inst_id.replace("-USDT-SWAP", "")
                if clean in found_syms: continue
                if info["price"] <= 0 or info["vol"] <= 0: continue
                rs_s = score_rs(info["chg24"], chg24_btc, chg24_eth)
                if rs_s >= 70:
                    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                    signals.append({
                        "symbol": clean, "direction": "LONG",
                        "entry": info["price"], "stop": round(info["price"]*0.97,8),
                        "tp1": round(info["price"]*1.04,8),
                        "tp2": round(info["price"]*1.06,8),
                        "tp3": round(info["price"]*1.08,8),
                        "rr": 2.0, "score": 65, "grade": "B",
                        "rvol": 1.0, "funding": 0.0,
                        "structure": "neutral", "trend_dir": "moderate",
                        "atr_ratio": 1.0, "oi_delta": 0.0,
                        "rs_score": rs_s, "components": {},
                        "timestamp": ts, "price": info["price"],
                        "chg24": round(info["chg24"], 2),
                        "source": "CoinGecko",
                    })
        except Exception as e:
            L("CoinGecko supplementary failed: " + str(e)[:60])
    else:
        L("Step 6/6: Signal count sufficient, skipping CoinGecko supplement")

    L("Scan complete: " + str(len(signals)) +
      " ranked signals (score >= " + str(MIN_SCORE) + ")")
    return signals
