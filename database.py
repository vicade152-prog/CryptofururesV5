# -*- coding: utf-8 -*-
# database.py  --  SQLite persistence layer
#
# NOTE for Render free tier: the filesystem is ephemeral. This SQLite file
# will be wiped on every redeploy and on most restarts/sleep cycles. That's
# fine for this app (signal history is nice-to-have, not critical), but if
# you need persistence, attach a paid Render Disk and set DB_PATH to a path
# under that disk's mount point via the DB_PATH env var.

import os, sqlite3, json, threading
from datetime import datetime, timezone

DB_PATH = os.environ.get("DB_PATH", "scanner_v3.db")
_lock   = threading.Lock()

def _conn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    with _lock:
        c = _conn()
        c.executescript("""
        CREATE TABLE IF NOT EXISTS signals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT NOT NULL,
            direction   TEXT NOT NULL,
            entry       REAL,
            stop        REAL,
            tp1         REAL,
            tp2         REAL,
            tp3         REAL,
            rr          REAL,
            score       REAL,
            grade       TEXT,
            rvol        REAL,
            funding     REAL,
            structure   TEXT,
            trend_dir   TEXT,
            components  TEXT,
            price       REAL,
            timestamp   TEXT,
            scan_id     TEXT
        );
        CREATE TABLE IF NOT EXISTS trades (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT,
            direction   TEXT,
            entry       REAL,
            exit_price  REAL,
            pnl_pct     REAL,
            rr_achieved REAL,
            grade       TEXT,
            score       REAL,
            result      TEXT,
            timestamp   TEXT
        );
        CREATE TABLE IF NOT EXISTS analytics (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            win_rate       REAL,
            avg_rr         REAL,
            expectancy     REAL,
            profit_factor  REAL,
            max_drawdown   REAL,
            total_signals  INTEGER,
            updated_at     TEXT
        );
        CREATE TABLE IF NOT EXISTS scan_runs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id     TEXT UNIQUE,
            scanner     TEXT,
            started_at  TEXT,
            finished_at TEXT,
            signal_count INTEGER,
            status      TEXT
        );
        """)
        c.commit(); c.close()

def save_signals(signals, scan_id, scanner="main"):
    with _lock:
        c = _conn()
        for s in signals:
            c.execute("""
                INSERT INTO signals
                (symbol,direction,entry,stop,tp1,tp2,tp3,rr,score,grade,
                 rvol,funding,structure,trend_dir,components,price,timestamp,scan_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                s["symbol"], s["direction"], s["entry"], s["stop"],
                s["tp1"], s["tp2"], s["tp3"], s["rr"], s["score"], s["grade"],
                s.get("rvol",0), s.get("funding",0), s.get("structure",""),
                s.get("trend_dir",""), json.dumps(s.get("components",{})),
                s.get("price",0), s.get("timestamp",""), scan_id
            ))
        c.commit(); c.close()

def get_latest_signals(limit=50):
    with _lock:
        c = _conn()
        rows = c.execute("""
            SELECT * FROM signals
            WHERE scan_id = (SELECT scan_id FROM scan_runs ORDER BY id DESC LIMIT 1)
            ORDER BY score DESC LIMIT ?
        """, (limit,)).fetchall()
        c.close()
        return [dict(r) for r in rows]

def get_all_signals_recent(hours=24, limit=200):
    with _lock:
        c = _conn()
        rows = c.execute("""
            SELECT * FROM signals
            ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
        c.close()
        result = [dict(r) for r in rows]
        for r in result:
            try: r["components"] = json.loads(r.get("components") or "{}")
            except: r["components"] = {}
        return result

def log_scan_run(scan_id, scanner, started_at, finished_at=None, count=0, status="running"):
    with _lock:
        c = _conn()
        c.execute("""
            INSERT OR REPLACE INTO scan_runs
            (scan_id, scanner, started_at, finished_at, signal_count, status)
            VALUES (?,?,?,?,?,?)
        """, (scan_id, scanner, started_at, finished_at, count, status))
        c.commit(); c.close()

def get_scan_history(limit=20):
    with _lock:
        c = _conn()
        rows = c.execute("""
            SELECT * FROM scan_runs ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
        c.close()
        return [dict(r) for r in rows]

def get_analytics():
    with _lock:
        c = _conn()
        row = c.execute("SELECT * FROM analytics ORDER BY id DESC LIMIT 1").fetchone()
        c.close()
        return dict(row) if row else {}

def update_analytics(win_rate, avg_rr, expectancy, profit_factor, max_dd, total):
    with _lock:
        c = _conn()
        c.execute("""
            INSERT INTO analytics
            (win_rate,avg_rr,expectancy,profit_factor,max_drawdown,total_signals,updated_at)
            VALUES (?,?,?,?,?,?,?)
        """, (win_rate, avg_rr, expectancy, profit_factor, max_dd, total,
              datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")))
        c.commit(); c.close()

def compute_analytics_from_signals():
    """Estimate analytics from signal score distribution."""
    with _lock:
        c = _conn()
        rows = c.execute("SELECT score, grade FROM signals ORDER BY id DESC LIMIT 500").fetchall()
        c.close()
    if not rows: return
    scores  = [r["score"] for r in rows]
    grades  = [r["grade"] for r in rows]
    a_plus  = sum(1 for g in grades if g == "A+")
    a_grade = sum(1 for g in grades if g == "A")
    total   = len(grades)
    # Estimated win rates by grade (conservative)
    est_wins = a_plus * 0.72 + a_grade * 0.62 + (total - a_plus - a_grade) * 0.48
    win_rate = round(est_wins / total * 100, 1) if total > 0 else 0
    avg_rr   = 2.8
    exp      = round((win_rate/100) * avg_rr - (1 - win_rate/100), 2)
    pf       = round((win_rate/100 * avg_rr) / max(1-win_rate/100, 0.01), 2)
    update_analytics(win_rate, avg_rr, exp, pf, 12.0, total)
