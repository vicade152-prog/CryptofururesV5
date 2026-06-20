# -*- coding: utf-8 -*-
# app.py  --  Crypto Futures Scanner V3.1 (CLOUD / RENDER edition)
# Full auto-scanning web app. No manual button press needed.
# Scans start automatically on launch and repeat on schedule.
# Reads PORT, SCAN_INTERVAL_SECONDS, PROXY_URL from environment variables
# so it works unmodified on Render's free tier.

import os, sys, traceback

def _global_crash_handler(exc_type, exc_value, exc_tb):
    print("", flush=True)
    print("=" * 60, flush=True)
    print("FATAL CRASH: " + type(exc_value).__name__ + " -- " + str(exc_value), flush=True)
    print("=" * 60, flush=True)
    traceback.print_exception(exc_type, exc_value, exc_tb)

sys.excepthook = _global_crash_handler

def _thread_crash_handler(args):
    _global_crash_handler(args.exc_type, args.exc_value, args.exc_traceback)

import threading
threading.excepthook = _thread_crash_handler

# -- Imports ----------------------------------------------------------------
import asyncio, json, queue, time, uuid
from datetime import datetime, timezone

from flask import Flask, Response, redirect, stream_with_context

import database as db
import scanner  as sc

# ==========================================================================
#  CONFIGURATION  --  reads from environment variables (Render sets PORT)
# ==========================================================================
SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300"))   # auto-rescan every 5 minutes
FIRST_SCAN_DELAY      = int(os.environ.get("FIRST_SCAN_DELAY", "8"))          # seconds after startup before first scan
WEB_PORT              = int(os.environ.get("PORT", "5000"))                   # Render injects PORT automatically
TIMEFRAME             = "1h"     # candle timeframe for scoring
CANDLE_LIMIT          = 250      # candles fetched per symbol
TOP_N_DISPLAY         = 50       # max rows shown on dashboard
# ==========================================================================

app = Flask(__name__, static_folder=None)
db.init_db()

@app.after_request
def _add_cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp

# -- Shared state -----------------------------------------------------------
STATE = {
    "running":      False,
    "signals":      [],
    "last_scan":    None,
    "next_scan":    None,
    "scan_count":   0,
    "error":        None,
    "log":          [],
    "pct":          0,
    "history":      [],
}
_lock  = threading.Lock()
_subs  = []   # SSE subscriber queues

def _push(etype, data):
    msg = "event: " + etype + "\ndata: " + json.dumps(data) + "\n\n"
    with _lock:
        dead = []
        for q in _subs:
            try:    q.put_nowait(msg)
            except: dead.append(q)
        for q in dead:
            _subs.remove(q)

def _log(msg, pct=None):
    ts   = datetime.now(timezone.utc).strftime("%H:%M:%S")
    line = "[" + ts + "] " + msg
    with _lock:
        STATE["log"].append(line)
        if len(STATE["log"]) > 300:
            STATE["log"] = STATE["log"][-300:]
        STATE["pct"] = pct if pct is not None else STATE["pct"]
    _push("log", {"msg": line, "pct": STATE["pct"]})
    print(line, flush=True)


# -- Background auto-scanner ------------------------------------------------
def _do_scan():
    with _lock:
        if STATE["running"]:
            return
        STATE["running"] = True
        STATE["error"]   = None
        STATE["log"]     = []
        STATE["pct"]     = 0

    scan_id = str(uuid.uuid4())[:8]
    started = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    db.log_scan_run(scan_id, "main", started)
    _push("status", {"running": True, "scan_count": STATE["scan_count"]})

    async def _run():
        try:
            signals = await sc.run_full_scan(
                log_fn   = _log,
                timeframe= TIMEFRAME,
                limit    = CANDLE_LIMIT,
            )
            finished = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            with _lock:
                STATE["signals"]   = signals
                STATE["last_scan"] = finished
                STATE["scan_count"] += 1
                STATE["pct"]       = 100
                STATE["history"]   = db.get_scan_history(10)
            if signals:
                db.save_signals(signals, scan_id)
            db.log_scan_run(scan_id, "main", started, finished,
                            len(signals), "done")
            db.compute_analytics_from_signals()
            _log("Scan #" + str(STATE["scan_count"]) +
                 " complete -- " + str(len(signals)) + " signals", 100)
            _push("done", {
                "last_scan":  finished,
                "count":      len(signals),
                "scan_count": STATE["scan_count"],
            })
        except Exception as e:
            with _lock:
                STATE["error"] = str(e)
            _log("ERROR: " + str(e))
            _push("error", {"msg": str(e)})
        finally:
            with _lock:
                STATE["running"] = False
                next_t = datetime.fromtimestamp(
                    time.time() + SCAN_INTERVAL_SECONDS, tz=timezone.utc
                ).strftime("%H:%M UTC")
                STATE["next_scan"] = next_t
            _push("status", {"running": False, "next_scan": STATE["next_scan"]})

    asyncio.run(_run())


def _scheduler():
    """Runs in a background daemon thread. First scan after FIRST_SCAN_DELAY,
    then every SCAN_INTERVAL_SECONDS automatically."""
    _log("Scheduler started -- first scan in " +
         str(FIRST_SCAN_DELAY) + "s, then every " +
         str(SCAN_INTERVAL_SECONDS) + "s")
    time.sleep(FIRST_SCAN_DELAY)
    while True:
        _do_scan()
        _log("Next auto-scan in " + str(SCAN_INTERVAL_SECONDS) + "s")
        time.sleep(SCAN_INTERVAL_SECONDS)


# ==========================================================================
#  CSS  (dark terminal theme, no frameworks)
# ==========================================================================
CSS = """
:root{
  --bg:#060a12;--bg2:#0b1220;--bg3:#111928;--bg4:#182032;
  --border:#1a2d48;--border2:#243d5c;
  --text:#c8d8f0;--muted:#4a6a8f;--muted2:#2a3f5f;
  --blue:#3b9eff;--blue2:#1255a0;
  --green:#22c55e;--green2:#0d2b1a;
  --amber:#f59e0b;--amber2:#2d1a03;
  --red:#ef4444;--red2:#2b0808;
  --mono:'Courier New','Consolas',monospace;
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;background:var(--bg);color:var(--text);
          font-family:var(--mono);font-size:13px;line-height:1.5}
a{color:var(--blue);text-decoration:none}
a:hover{color:#7bbfff}

/* NAV */
.nav{background:var(--bg3);border-bottom:2px solid var(--border);
     display:flex;align-items:stretch;padding:0 20px;
     position:sticky;top:0;z-index:100;gap:0}
.nav-brand{font-size:14px;font-weight:bold;color:var(--blue);
           padding:0 20px 0 0;border-right:1px solid var(--border);
           margin-right:6px;display:flex;align-items:center;gap:6px}
.nav-brand .v{color:var(--amber);font-size:11px}
.nav-tab{padding:14px 16px;color:var(--muted);border-bottom:3px solid transparent;
         font-size:12px;display:flex;align-items:center;gap:6px;transition:.15s}
.nav-tab:hover{color:var(--text)}
.nav-tab.active{color:var(--blue);border-bottom-color:var(--blue)}
.nav-status{margin-left:auto;display:flex;align-items:center;
            gap:12px;font-size:11px;color:var(--muted)}
.dot{width:8px;height:8px;border-radius:50%;display:inline-block;
     background:var(--muted2)}
.dot.live{background:var(--green);box-shadow:0 0 8px var(--green);
          animation:pulse 2s infinite}
.dot.scan{background:var(--amber);animation:blink .8s infinite}
.dot.err{background:var(--red)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}

/* LAYOUT */
.page{max-width:1600px;margin:0 auto;padding:20px}
.row{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:16px}

/* CARDS */
.card{background:var(--bg2);border:1px solid var(--border);
      border-radius:8px;padding:14px 18px}
.card .cv{font-size:28px;font-weight:bold;line-height:1.1;color:var(--blue)}
.card .cl{font-size:10px;color:var(--muted);margin-top:3px;text-transform:uppercase;
          letter-spacing:.05em}
.card .cs{font-size:10px;color:var(--muted2);margin-top:6px}
.card.cg .cv{color:var(--green)} .card.ca .cv{color:var(--amber)}
.card.cr .cv{color:var(--red)}

/* PROGRESS */
.prog-outer{background:var(--bg3);border-radius:2px;height:3px;
            border:1px solid var(--border);overflow:hidden;margin:8px 0}
.prog-inner{height:100%;background:linear-gradient(90deg,var(--blue2),var(--blue));
            border-radius:2px;transition:width .5s}

/* STATUS BAR */
.statusbar{background:var(--bg3);border:1px solid var(--border);
           border-radius:6px;padding:8px 14px;font-size:11px;
           color:var(--muted);display:flex;align-items:center;gap:16px;
           flex-wrap:wrap;margin-bottom:14px}
.statusbar b{color:var(--text)}
.pill{border-radius:3px;padding:2px 8px;font-size:10px;
      font-weight:bold;display:inline-block}
.pg{background:var(--green2);color:var(--green)}
.pa{background:var(--amber2);color:var(--amber)}
.pr{background:var(--red2);color:var(--red)}
.pb{background:#0a1f35;color:var(--blue)}

/* LOG BOX */
.logbox{background:#030810;border:1px solid var(--border);border-radius:6px;
        padding:8px 12px;height:160px;overflow-y:auto;font-size:11px;
        color:#4a8fbd;margin-bottom:14px;scroll-behavior:smooth}
.ll{padding:1px 0;border-bottom:1px solid rgba(26,45,72,.25)}
.ll:last-child{border:none} .ll.err{color:var(--red)}

/* TABLE */
.tbl-wrap{overflow-x:auto;border:1px solid var(--border);
          border-radius:8px;margin-bottom:16px}
table{border-collapse:collapse;width:100%;font-size:12px}
thead tr{background:#080f1e}
th{padding:10px 14px;text-align:left;color:var(--muted);font-weight:normal;
   font-size:10px;text-transform:uppercase;letter-spacing:.06em;
   border-bottom:1px solid var(--border);white-space:nowrap}
td{padding:8px 14px;border-bottom:1px solid rgba(26,45,72,.35);
   white-space:nowrap;vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:rgba(11,18,32,.9)}
.sym{color:#e8f0ff;font-weight:bold}
.num{color:#7bbfd8} .tg{color:var(--green)}
.ta{color:var(--amber)} .tr{color:var(--red)} .tb{color:var(--blue)}

/* SCORE BARS */
.sbar-wrap{display:flex;gap:2px;align-items:center}
.sbar{height:6px;border-radius:2px;background:var(--blue2);min-width:2px}
.score-num{font-size:11px;font-weight:bold;margin-left:4px}

/* ALERTS */
.alert{border-radius:6px;padding:10px 16px;font-size:12px;
       margin-bottom:12px;border-left:3px solid}
.ai{background:#0a1f35;border-color:var(--blue);color:#7bbfef}
.aw{background:var(--amber2);border-color:var(--amber);color:#fcd34d}
.ag{background:var(--green2);border-color:var(--green);color:#86efac}
.ae{background:var(--red2);border-color:var(--red);color:#fca5a5}

/* COUNTDOWN RING */
.ring-wrap{display:flex;align-items:center;gap:8px}
#countdown{font-size:18px;font-weight:bold;color:var(--amber);
           font-variant-numeric:tabular-nums}

/* ANALYTICS GRID */
.ana-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px}

/* COMPONENT BARS on detail */
.comp-bar-row{display:flex;align-items:center;gap:8px;margin:3px 0;font-size:11px}
.comp-label{width:70px;color:var(--muted);text-align:right;flex-shrink:0}
.comp-track{flex:1;background:var(--bg3);border-radius:2px;height:8px;overflow:hidden}
.comp-fill{height:100%;border-radius:2px;transition:width .4s}
.comp-val{width:32px;text-align:right;color:var(--text)}

/* TABS */
.tabs{display:flex;gap:0;border-bottom:1px solid var(--border);margin-bottom:16px}
.tab-btn{padding:8px 18px;background:none;border:none;
         border-bottom:2px solid transparent;color:var(--muted);
         font-family:var(--mono);font-size:12px;cursor:pointer;transition:.15s}
.tab-btn.on{color:var(--blue);border-bottom-color:var(--blue)}
.tab-pane{display:none} .tab-pane.on{display:block}

/* HISTORY TABLE */
.hist-row{display:grid;grid-template-columns:80px 140px 60px 80px 1fr;
          gap:8px;padding:6px 0;border-bottom:1px solid var(--border);
          font-size:11px;align-items:center}
.hist-row:last-child{border:none}
"""

# ==========================================================================
#  HTML BUILDER HELPERS
# ==========================================================================

def _now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def _nav(active="dashboard"):
    s = STATE
    dot_cls = "scan" if s["running"] else ("live" if s["last_scan"] else "")
    status_txt = ("Scanning..." if s["running"] else
                  ("Live -- " + s["last_scan"] if s["last_scan"] else "Awaiting first scan"))
    next_txt = ("Next: " + str(s.get("next_scan") or "--")) if s.get("next_scan") and not s["running"] else ""

    tabs = [
        ("dashboard", "/",          "Dashboard"),
        ("signals",   "/signals",   "Signals"),
        ("analytics", "/analytics", "Analytics"),
        ("log",       "/log",       "Live Log"),
    ]
    tab_html = ""
    for tid, href, label in tabs:
        cls = "nav-tab active" if tid == active else "nav-tab"
        n   = len(s["signals"]) if tid == "signals" and s["signals"] else ""
        badge = (" <span class='pill pb'>" + str(n) + "</span>") if n else ""
        tab_html += "<a class='" + cls + "' href='" + href + "'>" + label + badge + "</a>"

    return (
        "<nav class='nav'>"
        "<div class='nav-brand'>CRYPTO FUTURES SCANNER <span class='v'>V3</span></div>"
        + tab_html +
        "<div class='nav-status'>"
        "<span class='dot " + dot_cls + "'></span>"
        "<span>" + status_txt + "</span>"
        + ("<span style='color:var(--muted2)'>|</span><span>" + str(next_txt) + "</span>" if next_txt else "") +
        "</div></nav>"
    )


def _page(title, body, active, include_sse=True):
    sse_script = _sse_js() if include_sse else ""
    return (
        "<!DOCTYPE html><html lang='en'><head>"
        "<meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<title>" + title + " -- CFS V3</title>"
        "<style>" + CSS + "</style>"
        "<style>" + _coin_detail_css() + "</style>"
        "</head><body>"
        + _nav(active) +
        "<div class='page'>" + body + "</div>"
        + _coin_detail_modal_html() +
        sse_script +
        _coin_detail_js() +
        "</body></html>"
    )


def _score_bar(score):
    color = ("#22c55e" if score >= 85 else
             "#3b9eff" if score >= 75 else
             "#f59e0b" if score >= 65 else "#ef4444")
    w = int(score)
    return (
        "<div class='sbar-wrap'>"
        "<div class='sbar' style='width:" + str(w) + "px;background:" + color + "'></div>"
        "<span class='score-num' style='color:" + color + "'>" + str(score) + "</span>"
        "</div>"
    )


def _grade_pill(grade):
    cls = {"A+": "pg", "A": "pb", "B": "pa", "C": "pr"}.get(grade, "pb")
    return "<span class='pill " + cls + "'>" + grade + "</span>"


def _dir_cell(direction):
    if direction == "LONG":
        return "<span class='tg'>&#9650; LONG</span>"
    return "<span class='tr'>&#9660; SHORT</span>"


def _fmt(val, decimals=4):
    try:
        v = float(val)
        if v == 0: return "--"
        if v > 1000:   return str(round(v, 2))
        if v > 1:      return str(round(v, 4))
        return str(round(v, 8))
    except:
        return str(val)


# ==========================================================================
#  COIN DETAIL PANEL  --  click a row, see live safe-trade-check metrics
#  This is fully separate from the scan/scoring pipeline above. It calls
#  GET /api/coin/<symbol>, which itself makes fresh OKX calls at request
#  time -- nothing here touches or recomputes the scanner's own signals.
# ==========================================================================

def _coin_detail_css():
    return """
    .cd-overlay{display:none;position:fixed;inset:0;background:rgba(3,8,16,.75);
                z-index:200;align-items:flex-start;justify-content:center;
                overflow-y:auto;padding:24px 12px}
    .cd-overlay.open{display:flex}
    .cd-panel{background:var(--bg2);border:1px solid var(--border2);border-radius:10px;
              max-width:640px;width:100%;margin-top:24px;padding:0;overflow:hidden}
    .cd-head{display:flex;align-items:center;justify-content:space-between;
             padding:16px 20px;border-bottom:1px solid var(--border2);background:var(--bg3)}
    .cd-head h2{font-size:16px;color:var(--blue);margin:0}
    .cd-close{cursor:pointer;color:var(--muted);font-size:20px;line-height:1;
              background:none;border:none;padding:4px 8px}
    .cd-close:hover{color:var(--text)}
    .cd-body{padding:18px 20px}
    .cd-loading{text-align:center;padding:40px;color:var(--muted)}
    .cd-verdict{border-radius:6px;padding:12px 16px;font-size:13px;font-weight:bold;
                margin-bottom:16px;text-align:center}
    .cd-verdict.safe{background:var(--green2);color:var(--green);border:1px solid #154d2e}
    .cd-verdict.risk{background:var(--red2);color:var(--red);border:1px solid #4d1212}
    .cd-verdict.unknown{background:#0a1f35;color:var(--blue);border:1px solid var(--border2)}
    .cd-check{border:1px solid var(--border2);border-radius:6px;padding:12px 14px;margin-bottom:10px}
    .cd-check-head{display:flex;align-items:center;gap:8px;font-size:12px;font-weight:bold;margin-bottom:6px}
    .cd-badge{font-size:10px;padding:2px 7px;border-radius:3px;font-weight:bold}
    .cd-badge.pass{background:var(--green2);color:var(--green)}
    .cd-badge.fail{background:var(--red2);color:var(--red)}
    .cd-badge.insufficient_data{background:var(--amber2);color:var(--amber)}
    .cd-detail{font-size:12px;color:var(--muted);line-height:1.5}
    .cd-raw{font-size:11px;color:var(--muted2);margin-top:4px}
    .cd-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:16px;font-size:11px}
    .cd-grid div{background:var(--bg3);border-radius:5px;padding:8px 10px}
    .cd-grid b{color:var(--text);font-size:13px;display:block;margin-top:2px}
    .cd-errs{font-size:11px;color:var(--red);margin-top:10px}
    """


def _coin_detail_modal_html():
    return (
        "<div class='cd-overlay' id='cdOverlay' onclick='if(event.target===this)closeCoinDetail()'>"
        "<div class='cd-panel'>"
        "<div class='cd-head'><h2 id='cdTitle'>--</h2>"
        "<button class='cd-close' onclick='closeCoinDetail()'>&times;</button></div>"
        "<div class='cd-body' id='cdBody'><div class='cd-loading'>Loading live metrics...</div></div>"
        "</div></div>"
    )


def _coin_detail_js():
    return """
    <script>
    function openCoinDetail(symbol){
        document.getElementById('cdTitle').textContent = symbol + ' -- Live Safe-Trade Check';
        document.getElementById('cdBody').innerHTML = '<div class="cd-loading">Loading live metrics from OKX...</div>';
        document.getElementById('cdOverlay').classList.add('open');
        fetch('/api/coin/' + encodeURIComponent(symbol), {cache: 'no-store'})
            .then(r => r.json())
            .then(d => renderCoinDetail(d))
            .catch(e => {
                document.getElementById('cdBody').innerHTML =
                    '<div class="cd-errs">Failed to load live data: ' + e.message + '</div>';
            });
    }
    function closeCoinDetail(){
        document.getElementById('cdOverlay').classList.remove('open');
    }
    document.addEventListener('keydown', function(e){
        if(e.key === 'Escape') closeCoinDetail();
    });

    function cdFmt(v, d){
        if(v === null || v === undefined) return '--';
        var n = Number(v);
        if(isNaN(n)) return String(v);
        return n.toFixed(d === undefined ? 4 : d);
    }

    function cdCheckBlock(name, check){
        if(!check) check = {status:'insufficient_data', detail:'No data returned.'};
        var badgeText = check.status === 'pass' ? 'PASS' :
                        check.status === 'fail' ? 'FAIL' : 'INSUFFICIENT DATA';
        return '<div class="cd-check">' +
               '<div class="cd-check-head"><span>' + name + '</span>' +
               '<span class="cd-badge ' + check.status + '">' + badgeText + '</span></div>' +
               '<div class="cd-detail">' + (check.detail || '') + '</div>' +
               '</div>';
    }

    function renderCoinDetail(d){
        if(d.verdict === 'ERROR'){
            document.getElementById('cdBody').innerHTML =
                '<div class="cd-errs">Live fetch failed: ' +
                (d.errors ? d.errors.join('; ') : 'unknown error') + '</div>';
            return;
        }

        var verdictClass = d.verdict === 'SAFE_TRADE' ? 'safe' :
                            d.verdict === 'BREAKDOWN_RISK' ? 'risk' : 'unknown';
        var verdictText  = d.verdict === 'SAFE_TRADE' ? 'SAFE TRADE -- all 3 conditions pass' :
                            d.verdict === 'BREAKDOWN_RISK' ? 'BREAKDOWN RISK -- at least one condition failed' :
                            'INSUFFICIENT DATA -- not enough live data yet to give a verdict';

        var html = '<div class="cd-verdict ' + verdictClass + '">' + verdictText + '</div>';

        html += '<div class="cd-grid">' +
                '<div>Live Price<b>' + cdFmt(d.price, 6) + '</b></div>' +
                '<div>Funding Rate (8h)<b>' + (d.funding != null ? (d.funding*100).toFixed(4)+'%' : '--') + '</b></div>' +
                '<div>15m EMA(20)<b>' + cdFmt(d.ema20_15m, 6) + '</b></div>' +
                '<div>Distance from EMA<b>' + (d.ema_dist_pct != null ? d.ema_dist_pct.toFixed(2)+'%' : '--') + '</b></div>' +
                '<div>Current Open Interest<b>$' + cdFmt(d.oi_now, 0) + '</b></div>' +
                '<div>OI Samples Collected<b>' + (d.oi_history_samples||0) + ' (' + (d.oi_history_window_minutes||0) + ' min span)</b></div>' +
                '</div>';

        html += cdCheckBlock('1. Funding Rate &lt; 0.05%', d.funding_check);
        html += cdCheckBlock('2. Price near 15m EMA(20) (within 1.5%)', d.ema_check);
        html += cdCheckBlock('3. OI fell during latest minor dip', d.oi_check);

        if(d.dip && d.dip.detected){
            html += '<div class="cd-raw">Dip detected: ' + d.dip.dip_pct + '% pullback from ' +
                    cdFmt(d.dip.swing_high,6) + ' to ' + cdFmt(d.dip.swing_low,6) +
                    ' (' + d.dip.high_candle_idx_ago + ' candles ago to ' + d.dip.low_candle_idx_ago + ' candles ago, on the 15m chart)</div>';
        } else if(d.dip && d.dip.reason){
            html += '<div class="cd-raw">Dip check: ' + d.dip.reason + '</div>';
        }

        html += '<div class="cd-raw" style="margin-top:10px">Fetched live at ' + (d.fetched_at || '--') + '</div>';

        if(d.errors && d.errors.length){
            html += '<div class="cd-errs">Partial data -- some live calls failed: ' + d.errors.join('; ') + '</div>';
        }

        document.getElementById('cdBody').innerHTML = html;
    }
    </script>
    """


# ==========================================================================
#  SSE  (Server-Sent Events -- live push, no websocket needed)
# ==========================================================================

def _sse_js():
    """Minimal JS -- purely for receiving SSE events and updating the DOM."""
    return """<script>
(function(){
  var es = new EventSource('/stream');

  es.addEventListener('log', function(e){
    var d = JSON.parse(e.data);
    var box = document.getElementById('logbox');
    if(box){
      var ln = document.createElement('div');
      ln.className = 'll' + (d.msg.indexOf('ERROR') >= 0 ? ' err' : '');
      ln.textContent = d.msg;
      box.appendChild(ln);
      if(box.children.length > 150) box.removeChild(box.firstChild);
      box.scrollTop = box.scrollHeight;
    }
    var pb = document.getElementById('progbar');
    if(pb && d.pct) pb.style.width = d.pct + '%';
    var pt = document.getElementById('prog-txt');
    if(pt && d.pct) pt.textContent = d.pct + '%';
  });

  es.addEventListener('status', function(e){
    var d = JSON.parse(e.data);
    var dot  = document.getElementById('nav-dot');
    var stxt = document.getElementById('scan-status');
    if(d.running){
      if(dot)  dot.className = 'dot scan';
      if(stxt) stxt.textContent = 'Scanning...';
      var btn = document.getElementById('manual-btn');
      if(btn) btn.disabled = true;
    } else {
      if(dot) dot.className = 'dot live';
      var btn = document.getElementById('manual-btn');
      if(btn) btn.disabled = false;
      if(d.next_scan && stxt) stxt.textContent = 'Next: ' + d.next_scan;
    }
  });

  es.addEventListener('done', function(e){
    var d = JSON.parse(e.data);
    // Refresh signals table without full page reload
    fetch('/partial/signals')
      .then(function(r){ return r.text(); })
      .then(function(html){
        var tbl = document.getElementById('signals-wrap');
        if(tbl) tbl.innerHTML = html;
      });
    // Update summary cards
    fetch('/partial/cards')
      .then(function(r){ return r.text(); })
      .then(function(html){
        var c = document.getElementById('cards-wrap');
        if(c) c.innerHTML = html;
      });
    // Update countdown
    resetCountdown();
  });

  es.addEventListener('error', function(e){
    if(!e.data) return;
    var d = JSON.parse(e.data);
    var box = document.getElementById('logbox');
    if(box){
      var ln = document.createElement('div');
      ln.className = 'll err';
      ln.textContent = '[ERROR] ' + d.msg;
      box.appendChild(ln);
      box.scrollTop = box.scrollHeight;
    }
  });

  // Countdown timer to next scan
  var countdownSecs = """ + str(SCAN_INTERVAL_SECONDS) + """;
  var _cd = null;
  function resetCountdown(){
    clearInterval(_cd);
    countdownSecs = """ + str(SCAN_INTERVAL_SECONDS) + """;
    tick();
    _cd = setInterval(tick, 1000);
  }
  function tick(){
    var el = document.getElementById('countdown');
    if(!el) return;
    if(countdownSecs <= 0){ el.textContent = 'Scanning...'; return; }
    var m = Math.floor(countdownSecs / 60);
    var s = countdownSecs % 60;
    el.textContent = (m > 0 ? m + 'm ' : '') + s + 's';
    countdownSecs--;
  }
  resetCountdown();
})();
</script>"""


# ==========================================================================
#  SIGNAL TABLE  (reusable partial)
# ==========================================================================

def _build_signals_table(signals, limit=TOP_N_DISPLAY):
    if not signals:
        return (
            "<div style='text-align:center;padding:48px;color:var(--muted2)'>"
            "<div style='font-size:16px;margin-bottom:8px'>No signals yet</div>"
            "<div style='font-size:12px;color:var(--muted)'>"
            "First scan starts automatically -- check the Live Log tab</div>"
            "</div>"
        )
    rows = ""
    for s in signals[:limit]:
        comp = s.get("components") or {}
        if isinstance(comp, str):
            try:    comp = json.loads(comp)
            except: comp = {}

        # Direction
        dir_html = _dir_cell(str(s.get("direction","LONG")))

        # Trend
        trend_txt = (
            "<span class='tg'>Strong Bull</span>" if s.get("trend_dir") == "strong_bull" else
            "<span class='tr'>Strong Bear</span>" if s.get("trend_dir") == "strong_bear" else
            "<span class='ta'>Moderate</span>"
        )

        # OI -- real delta from history
        oi_delta = s.get("oi_delta", 0.0)
        if oi_delta is None: oi_delta = 0.0
        if   oi_delta >  2.0: oi_txt, oi_col = "+" + str(round(oi_delta,1)) + "% Rising", "tg"
        elif oi_delta >  0.5: oi_txt, oi_col = "+" + str(round(oi_delta,1)) + "% Building", "tg"
        elif oi_delta > -0.5: oi_txt, oi_col = "Stable", "ta"
        else:                  oi_txt, oi_col = str(round(oi_delta,1)) + "% Falling", "tr"

        # Relative Strength
        rs_s   = int(s.get("rs_score") or comp.get("rs", 50) or 50)
        if   rs_s >= 85: rs_txt, rs_col = "Outperform", "tg"
        elif rs_s >= 70: rs_txt, rs_col = "Above Avg",  "tg"
        elif rs_s >= 55: rs_txt, rs_col = "In-Line",    "ta"
        else:             rs_txt, rs_col = "Lagging",    "tr"

        # ATR expansion
        atr_r  = s.get("atr_ratio", 1.0)
        if atr_r is None: atr_r = 1.0
        atr_col = "tg" if float(atr_r) >= 1.5 else ("ta" if float(atr_r) >= 1.0 else "tr")

        # RVOL
        rv     = s.get("rvol", 1.0)
        if rv is None: rv = 1.0
        rv_col = "tg" if float(rv) >= 2.0 else ("ta" if float(rv) >= 1.0 else "tr")

        # Breakout score
        bk_s = int(comp.get("breakout", 50) or 50)
        bk_col = "tg" if bk_s >= 80 else ("ta" if bk_s >= 50 else "tr")

        # 24h change
        chg = s.get("chg24", 0.0)
        if chg is None: chg = 0.0
        chg_col = "tg" if float(chg) > 0 else "tr"

        rows += (
            "<tr class='coin-row' onclick=\"openCoinDetail('" + str(s.get("symbol","")) + "')\" style='cursor:pointer'>"
            "<td class='sym'>" + str(s.get("symbol","")) + "</td>"
            "<td>" + dir_html + "</td>"
            "<td class='num'>" + _fmt(s.get("entry",0)) + "</td>"
            "<td class='tr'>"  + _fmt(s.get("stop",0))  + "</td>"
            "<td class='tg'>"  + _fmt(s.get("tp1",0))   + "</td>"
            "<td class='tg'>"  + _fmt(s.get("tp2",0))   + "</td>"
            "<td class='num'>" + str(s.get("rr","3.0")) + "R</td>"
            "<td>" + _score_bar(int(s.get("score",0))) + "</td>"
            "<td>" + _grade_pill(str(s.get("grade","B"))) + "</td>"
            "<td>" + trend_txt + "</td>"
            "<td class='" + rs_col  + "'>" + rs_txt  + "</td>"
            "<td class='" + oi_col  + "'>" + oi_txt  + "</td>"
            "<td class='" + rv_col  + "'>" + str(rv)  + "x</td>"
            "<td class='" + atr_col + "'>" + str(round(float(atr_r),2)) + "x ATR</td>"
            "<td class='" + bk_col  + "'>" + ("Confirmed" if bk_s >= 80 else ("Near" if bk_s >= 60 else "None")) + "</td>"
            "<td class='" + chg_col + "'>" + ("+" if float(chg) > 0 else "") + str(round(float(chg),2)) + "%</td>"
            "<td style='font-size:10px;color:var(--muted)'>" + str(s.get("timestamp","")) + "</td>"
            "</tr>"
        )

    header = (
        "<div class='tbl-wrap'><table>"
        "<thead><tr>"
        "<th>Symbol</th><th>Dir</th>"
        "<th>Entry</th><th>Stop</th><th>TP1 (2R)</th><th>TP2 (3R)</th>"
        "<th>R:R</th><th>Score</th><th>Grade</th>"
        "<th>Trend</th><th>Rel Strength</th><th>OI Delta</th>"
        "<th>RVOL</th><th>ATR Exp</th><th>Breakout</th><th>24h</th><th>Time</th>"
        "</tr></thead>"
        "<tbody>"
    )
    return header + rows + "</tbody></table></div>"


# ==========================================================================
#  SUMMARY CARDS
# ==========================================================================

def _build_cards():
    s     = STATE
    sigs  = s["signals"]
    total = len(sigs)
    aplus = sum(1 for x in sigs if x.get("grade") == "A+")
    a_g   = sum(1 for x in sigs if x.get("grade") == "A")
    longs = sum(1 for x in sigs if x.get("direction") == "LONG")
    shorts= total - longs
    scan_n= s["scan_count"]

    return (
        "<div class='row'>"
        "<div class='card " + ("cg" if total > 0 else "") + "' style='min-width:110px'>"
        "<div class='cv'>" + str(total) + "</div>"
        "<div class='cl'>Total Signals</div>"
        "<div class='cs'>Min score " + str(sc.MIN_SCORE) + "</div>"
        "</div>"
        "<div class='card " + ("cg" if aplus > 0 else "") + "' style='min-width:110px'>"
        "<div class='cv'>" + str(aplus) + "</div>"
        "<div class='cl'>A+ Grade (85+)</div>"
        "<div class='cs'>Highest confidence</div>"
        "</div>"
        "<div class='card " + ("cg" if a_g > 0 else "") + "' style='min-width:110px'>"
        "<div class='cv'>" + str(a_g) + "</div>"
        "<div class='cl'>A Grade (75-84)</div>"
        "<div class='cs'>Strong setups</div>"
        "</div>"
        "<div class='card " + ("cg" if longs > 0 else "") + "' style='min-width:110px'>"
        "<div class='cv'>" + str(longs) + "</div>"
        "<div class='cl'>Long Setups</div></div>"
        "<div class='card " + ("cr" if shorts > 0 else "") + "' style='min-width:110px'>"
        "<div class='cv'>" + str(shorts) + "</div>"
        "<div class='cl'>Short Setups</div></div>"
        "<div class='card ca' style='min-width:110px'>"
        "<div class='cv'>" + str(scan_n) + "</div>"
        "<div class='cl'>Scans Run</div>"
        "<div class='cs'>Auto every " + str(SCAN_INTERVAL_SECONDS) + "s</div>"
        "</div>"
        "</div>"
    )


# ==========================================================================
#  STATUS BAR
# ==========================================================================

def _build_statusbar():
    s    = STATE
    pct  = int(s.get("pct") or 0)
    run  = bool(s.get("running"))
    nxt  = str(s.get("next_scan") or "--")
    err  = s.get("error")
    last = str(s.get("last_scan") or "Never")

    running_txt = (
        "Scanning now (" + str(pct) + "%)..." if run else
        "Last scan: <b>" + last + "</b>"
    )
    next_txt     = "Next auto-scan: <b id='countdown'>--</b>" if not run else ""
    err_html     = ("<span class='tr'>Error: " + str(err)[:80] + "</span>") if err else ""
    proxy_txt    = "Direct connection (no proxy)" if not sc.PROXY else "Proxy: " + str(sc._PROXY_URL)
    tf_txt       = "TF: " + str(TIMEFRAME)
    interval_txt = "Interval: " + str(SCAN_INTERVAL_SECONDS) + "s"

    prog = (
        "<div class='prog-outer' style='width:120px;display:inline-block;vertical-align:middle'>"
        "<div class='prog-inner' id='progbar' style='width:" + str(pct) + "%'></div>"
        "</div>"
        "<span id='prog-txt' style='font-size:10px;color:var(--muted)'>" + str(pct) + "%</span>"
    ) if run else ""

    form = (
        "<form method='post' action='/scan-now' style='margin:0'>"
        "<button id='manual-btn' style='background:var(--blue2);border:none;border-radius:4px;"
        "padding:4px 12px;color:#fff;font-family:var(--mono);font-size:11px;cursor:pointer"
        + (";opacity:.5;cursor:not-allowed" if run else "") + "'"
        + (" disabled" if run else "") + ">"
        "Force Scan</button></form>"
    )

    return (
        "<div class='statusbar'>"
        "<span>" + running_txt + "</span>"
        + (prog if prog else "")
        + ("<span>" + next_txt + "</span>" if next_txt else "")
        + "<span style='color:var(--muted2)'>|</span><span>" + proxy_txt + "</span>"
        + "<span style='color:var(--muted2)'>|</span><span>" + tf_txt + "</span>"
        + "<span style='color:var(--muted2)'>|</span><span>" + interval_txt + "</span>"
        + (err_html if err_html else "")
        + "<span style='margin-left:auto'>" + form + "</span>"
        "</div>"
    )

def _build_logbox():
    lines = "".join(
        "<div class='ll" + (" err" if "ERROR" in l else "") + "'>" + l + "</div>"
        for l in STATE["log"][-80:]
    )
    return (
        "<div class='logbox' id='logbox'>" + lines + "</div>"
        "<script>var b=document.getElementById('logbox');"
        "if(b)b.scrollTop=b.scrollHeight;</script>"
    )


# ==========================================================================
#  ROUTES
# ==========================================================================

@app.route("/")
def dashboard():
    sigs  = STATE["signals"]
    total = len(sigs)
    run   = STATE["running"]

    alert = ""
    if run:
        alert = "<div class='alert aw'>Scan in progress -- results update automatically when complete.</div>"
    elif not sigs:
        alert = ("<div class='alert ai'>Auto-scan starts in " + str(FIRST_SCAN_DELAY) +
                 " seconds after launch.</div>")
    elif STATE.get("error"):
        alert = "<div class='alert ae'>Last scan error: " + str(STATE["error"])[:200] + "</div>"
    else:
        alert = "<div class='alert ag'>Live -- " + str(total) + " signals ranked. Auto-refreshing every " + str(SCAN_INTERVAL_SECONDS) + "s.</div>"

    # Top 5 A+ signals preview
    top5 = [s for s in sigs if s.get("grade") == "A+"][:5]
    top5_html = ""
    if top5:
        top5_html = (
            "<h3 style='font-size:12px;color:var(--muted);margin-bottom:8px;text-transform:uppercase;letter-spacing:.05em'>"
            "Top A+ Signals</h3>"
            + _build_signals_table(top5, 5)
        )

    body = (
        "<div style='display:flex;align-items:baseline;gap:12px;margin-bottom:4px'>"
        "<h2 style='font-size:16px;color:#e8f0ff'>Crypto Futures Scanner V3</h2>"
        "<span style='font-size:11px;color:var(--muted)'>OKX Futures | CoinGecko Fallback | Cloud</span>"
        "</div>"
        + alert
        + _build_statusbar()
        + "<div id='cards-wrap'>" + _build_cards() + "</div>"
        + top5_html
        + "<div style='font-size:11px;color:var(--muted2);margin-top:8px'>"
        "V3.1 ranking: Trend 20% | RS 15% | Real OI 15% | RVOL 15% | ATR Exp 10% | Structure 10% | Breakout 10% | Sweep 5%"
        "</div>"
    )
    return _page("Dashboard", body, "dashboard")


@app.route("/signals")
def signals_page():
    sigs  = STATE["signals"]
    f_dir = "" # future: filter by direction
    f_grade = ""

    body = (
        _build_statusbar()
        + "<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:12px'>"
        "<h2 style='font-size:14px;color:#e8f0ff'>"
        + str(len(sigs)) + " Ranked Signals</h2>"
        "<span style='font-size:11px;color:var(--muted)'>Auto-updates after each scan</span>"
        "</div>"
        + "<div id='signals-wrap'>" + _build_signals_table(sigs) + "</div>"
    )
    return _page("Signals", body, "signals")


@app.route("/analytics")
def analytics_page():
    ana  = db.get_analytics()
    hist = db.get_scan_history(15)

    if ana:
        cards = (
            "<div class='ana-grid' style='margin-bottom:20px'>"
            "<div class='card'><div class='cv " + ("cg" if float(ana.get("win_rate") or 0)>60 else "ca") + "'>"
            + str(ana.get("win_rate","--")) + "%</div><div class='cl'>Est. Win Rate</div></div>"
            "<div class='card'><div class='cv'>" + str(ana.get("avg_rr","--")) + "R</div><div class='cl'>Avg R:R</div></div>"
            "<div class='card'><div class='cv " + ("cg" if float(ana.get("expectancy") or 0)>0 else "cr") + "'>"
            + str(ana.get("expectancy","--")) + "</div><div class='cl'>Expectancy</div></div>"
            "<div class='card'><div class='cv " + ("cg" if float(ana.get("profit_factor") or 0)>1.5 else "ca") + "'>"
            + str(ana.get("profit_factor","--")) + "</div><div class='cl'>Profit Factor</div></div>"
            "<div class='card'><div class='cv ca'>" + str(ana.get("max_drawdown","--")) + "%</div><div class='cl'>Est Max DD</div></div>"
            "<div class='card'><div class='cv'>" + str(ana.get("total_signals","--")) + "</div><div class='cl'>Signals Tracked</div></div>"
            "</div>"
        )
    else:
        cards = "<div class='alert ai'>Analytics will populate after the first scan completes.</div>"

    # Score distribution from current signals
    sigs  = STATE["signals"]
    aplus = sum(1 for s in sigs if s.get("grade")=="A+")
    a_g   = sum(1 for s in sigs if s.get("grade")=="A")
    b_g   = sum(1 for s in sigs if s.get("grade")=="B")

    dist = ""
    if sigs:
        total = len(sigs)
        def bar(n, total, color):
            w = int(n/total*200) if total>0 else 0
            return ("<div style='display:flex;align-items:center;gap:8px;margin:4px 0'>"
                    "<div style='width:200px;background:var(--bg3);border-radius:2px;height:12px'>"
                    "<div style='width:" + str(w) + "px;background:" + color + ";height:100%;border-radius:2px'></div>"
                    "</div><span style='font-size:11px;color:var(--text)'>" + str(n) + " (" + str(round(n/total*100)) + "%)</span>"
                    "</div>")
        dist = (
            "<h3 style='font-size:12px;color:var(--muted);margin-bottom:8px'>Grade Distribution (Current Scan)</h3>"
            "<div style='font-size:11px;margin-bottom:4px;color:var(--muted)'>A+ (85+)</div>" + bar(aplus,total,"#22c55e")
            + "<div style='font-size:11px;margin-bottom:4px;color:var(--muted)'>A  (75-84)</div>" + bar(a_g,total,"#3b9eff")
            + "<div style='font-size:11px;margin-bottom:4px;color:var(--muted)'>B  (65-74)</div>" + bar(b_g,total,"#f59e0b")
        )

    # Component averages
    comp_avgs = {}
    if sigs:
        for key in ["trend","rs","oi","rvol","atr_exp","structure","breakout","sweep"]:
            vals = []
            for s in sigs:
                c = s.get("components") or {}
                if isinstance(c,str):
                    try: c=json.loads(c)
                    except: c={}
                if key in c: vals.append(float(c[key]))
            comp_avgs[key] = round(sum(vals)/len(vals)) if vals else 0

    comp_html = ""
    if comp_avgs:
        comp_html = "<h3 style='font-size:12px;color:var(--muted);margin:16px 0 8px'>Average Component Scores (Current Scan)</h3>"
        colors = {"trend":"#22c55e","oi":"#3b9eff","vwap":"#f59e0b",
                  "rvol":"#22c55e","structure":"#3b9eff","cvd":"#f59e0b","position":"#ef4444"}
        weights= {"trend":20,"rs":15,"oi":15,"rvol":15,"atr_exp":10,"structure":10,"breakout":10,"sweep":5}
        for k,v in comp_avgs.items():
            color = colors.get(k,"#3b9eff")
            w_pct = weights.get(k,0)
            comp_html += (
                "<div class='comp-bar-row'>"
                "<div class='comp-label'>" + k.upper() + " " + str(w_pct) + "%</div>"
                "<div class='comp-track'><div class='comp-fill' style='width:" + str(v) + "%;background:" + color + "'></div></div>"
                "<div class='comp-val'>" + str(v) + "</div>"
                "</div>"
            )

    # Scan history
    hist_html = ""
    if hist:
        hist_html = "<h3 style='font-size:12px;color:var(--muted);margin:16px 0 8px'>Scan History</h3>"
        hist_html += (
            "<div class='hist-row' style='color:var(--muted);font-size:10px'>"
            "<div>Scan ID</div><div>Started</div><div>Signals</div><div>Status</div><div>Duration</div>"
            "</div>"
        )
        for h in hist:
            status_col = "tg" if h.get("status")=="done" else ("ta" if h.get("status")=="running" else "tr")
            hist_html += (
                "<div class='hist-row'>"
                "<div class='num'>#" + str(h.get("id","")) + "</div>"
                "<div style='font-size:10px;color:var(--muted)'>" + str(h.get("started_at",""))[:16] + "</div>"
                "<div class='tg'>" + str(h.get("signal_count","0")) + "</div>"
                "<div class='" + status_col + "'>" + str(h.get("status","")).upper() + "</div>"
                "<div style='font-size:10px;color:var(--muted2)'>auto</div>"
                "</div>"
            )

    body = (
        "<h2 style='font-size:14px;color:#e8f0ff;margin-bottom:14px'>Analytics & Performance</h2>"
        + cards + dist + comp_html + hist_html
        + "<div style='font-size:11px;color:var(--muted2);margin-top:16px'>"
        "Win rates are estimated from grade distribution. "
        "Track live trades manually to build verified performance data.</div>"
    )
    return _page("Analytics", body, "analytics")


@app.route("/log")
def log_page():
    body = (
        "<h2 style='font-size:14px;color:#e8f0ff;margin-bottom:12px'>"
        "Live Scanner Log</h2>"
        "<div style='font-size:11px;color:var(--muted);margin-bottom:10px'>"
        "Auto-updates in real time. Scroll to see full log.</div>"
        + _build_logbox()
        + _build_statusbar()
    )
    return _page("Live Log", body, "log")


@app.route("/api/state")
def api_state():
    from flask import jsonify
    with _lock:
        return jsonify({
            "running":    STATE["running"],
            "signals":    STATE["signals"],
            "last_scan":  STATE["last_scan"],
            "next_scan":  STATE["next_scan"],
            "scan_count": STATE["scan_count"],
            "error":      STATE["error"],
            "pct":        STATE["pct"],
            "log":        STATE["log"][-80:],
        })


@app.route("/api/coin/<symbol>")
def api_coin_detail(symbol):
    """Live per-coin detail panel data. Makes fresh OKX API calls at
    request time -- nothing here is cached or carried over from the last
    scan (except the OI rolling-history window the scan loop already
    maintains, which is read as-is, not re-sampled). This does NOT touch
    or affect the scanner's own scoring/ranking logic in any way."""
    from flask import jsonify
    clean   = symbol.upper().replace("-USDT-SWAP", "").replace("USDT", "")
    inst_id = clean + "-USDT-SWAP"
    try:
        detail = sc.fetch_coin_detail(inst_id)
        return jsonify(detail)
    except Exception as e:
        return jsonify({
            "inst_id": inst_id,
            "errors": ["fetch_coin_detail crashed: " + str(e)[:200]],
            "verdict": "ERROR",
        }), 500


@app.route("/mobile")
def mobile():
    """Standalone lightweight dashboard -- pure JSON polling, no server-side
    template rendering. Auto-detects this same Render URL as its backend,
    so it works on a phone with zero configuration."""
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "index.html"), "r", encoding="utf-8") as f:
            return Response(f.read(), mimetype="text/html")
    except FileNotFoundError:
        return Response("index.html not found next to app.py", status=500)


@app.route("/scan-now", methods=["POST"])
def scan_now():
    if not STATE["running"]:
        threading.Thread(target=_do_scan, daemon=True).start()
    return redirect("/")


@app.route("/partial/signals")
def partial_signals():
    return _build_signals_table(STATE["signals"])

@app.route("/partial/cards")
def partial_cards():
    return _build_cards()


@app.route("/stream")
def stream():
    q = queue.Queue(maxsize=300)
    with _lock:
        _subs.append(q)

    def gen():
        try:
            while True:
                try:
                    yield q.get(timeout=25)
                except queue.Empty:
                    yield ": ping\n\n"
        except GeneratorExit:
            pass
        finally:
            with _lock:
                if q in _subs:
                    _subs.remove(q)

    return Response(stream_with_context(gen()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


# ==========================================================================
#  ENTRY POINT
# ==========================================================================

print("", flush=True)
print("=" * 60, flush=True)
print("  Crypto Futures Scanner V3.1 (Cloud)", flush=True)
print("  OKX Futures | CoinGecko Fallback", flush=True)
print("  V3.1: RS | Real OI | Breakout | ATR Exp | Sweep", flush=True)
print("=" * 60, flush=True)
print("", flush=True)

print("[1/3] Initialising database...", flush=True)
db.init_db()
print("[OK] Database ready", flush=True)

print("[2/3] Starting auto-scan scheduler...", flush=True)
threading.Thread(target=_scheduler, daemon=True).start()
print("[OK] Scheduler running", flush=True)

print("[3/3] Flask app object ready for WSGI server.", flush=True)
print("", flush=True)

# When run directly (local dev: `python app.py`), use Flask's built-in
# server. On Render, gunicorn imports this module and calls `app` itself,
# so this block does not run -- but the setup above (DB + scheduler) already
# happened at import time either way.
if __name__ == "__main__":
    print("[DEV MODE] Starting Flask dev server on port " + str(WEB_PORT) + "...", flush=True)
    app.run(host="0.0.0.0", port=WEB_PORT, debug=False, threaded=True)
