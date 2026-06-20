# Crypto Futures Scanner V3 -- Cloud Edition (GitHub + Render)

This is the cloud-deployable version of the scanner, built for Render's
free tier and accessible from your phone's browser anywhere with internet
access (no Psiphon/proxy needed -- see "Network notes" below).

## What changed vs. the desktop (Windows) version

- No `webbrowser.open()`, no Windows console hacks, no runtime `pip install`
  -- dependencies are now in `requirements.txt` and installed at build time.
- Reads `PORT` from the environment (Render sets this automatically)
  instead of a hardcoded port.
- The Psiphon proxy is now **optional** via the `PROXY_URL` environment
  variable. Leave it unset on Render -- cloud datacenter IPs generally
  aren't geo-restricted the way some ISPs are.
- Runs under `gunicorn` in production (the file `Procfile` / `render.yaml`
  configure this), with a single worker + multiple threads so the
  background scan scheduler only runs once (not once per worker).
- Added `/mobile` -- a lightweight, dependency-free dashboard that polls
  `/api/state` as JSON. This is the recommended page to bookmark on your
  phone; it's simpler and more robust than the full server-rendered `/`
  dashboard.
- Added `/api/state` -- a JSON endpoint with the scanner's full live state
  (signals, log, progress, errors), used by `/mobile` and available for any
  other client you want to build later.

## Deploying

### 1. Push this folder to a new GitHub repository

```
git init
git add .
git commit -m "Initial commit -- cloud scanner"
git branch -M main
git remote add origin https://github.com/<your-username>/<your-repo>.git
git push -u origin main
```

### 2. Create the Render service

**Option A -- one-click via Blueprint (recommended):**
1. Go to https://dashboard.render.com/blueprints
2. Click "New Blueprint Instance", connect your GitHub repo.
3. Render reads `render.yaml` automatically and configures everything
   (build command, start command, free plan, health check).
4. Click "Apply" / "Create".

**Option B -- manual web service:**
1. Go to https://dashboard.render.com -> "New" -> "Web Service".
2. Connect your GitHub repo.
3. Runtime: Python 3.
4. Build command: `pip install -r requirements.txt`
5. Start command: `gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 120 app:app`
6. Plan: Free.
7. Click "Create Web Service".

### 3. Open it on your phone

Render gives you a URL like `https://crypto-futures-scanner.onrender.com`.
Open that URL on your phone, then go to `/mobile`
(e.g. `https://crypto-futures-scanner.onrender.com/mobile`) and add it to
your home screen as a bookmark/shortcut for one-tap access.

## Network notes

- The scanner talks to OKX (USDT-margined perpetual swaps) directly as its
  primary data source for universe, tickers, candles, open interest, and
  funding rate. CoinGecko is used as a fallback if OKX itself becomes
  fully unreachable, and separately as a validation/sanity-check layer
  when OKX is working normally. Note: CoinGecko has no futures-specific
  data (no OI, no funding, no perpetual candles), so any signals filled
  in via the CoinGecko fallback path are lower-confidence (RS-based only,
  capped at grade B) compared to normal OKX-sourced signals.
  If Render's outbound IP gets rate-limited or blocked by an exchange,
  set the `PROXY_URL` environment variable in Render's dashboard
  (Environment tab) to a working HTTP/HTTPS proxy. Leave it unset to try
  a direct connection first -- that's the default and simplest setup.

## Free tier limitations -- important

These are accurate as of mid-2026 but Render's pricing/plans can change,
so double check the current terms at https://render.com/pricing before
relying on this for anything time-sensitive:

- **Spin-down:** Render's free web services go to sleep after about 15
  minutes with no incoming requests, and take roughly 30-60 seconds to
  wake back up on the next request. This means the background scanner
  (and its 5-minute auto-rescan loop) effectively **pauses while the
  service is asleep** -- it only scans while someone has the page open
  recently enough to keep it awake.
  - Render explicitly discourages self-pinging (sending your own traffic
    just to prevent sleep), as it can be flagged as abnormal traffic.
  - Practical workaround: open `/mobile` on your phone when you actually
    want fresh signals; the first load after sleep will be slow (cold
    start) but then it'll scan normally until it goes idle again.
  - If you need truly continuous 24/7 scanning, that requires a paid
    "Starter" instance ($7/mo at last check) which doesn't spin down.
- **512 MB RAM / 0.1 CPU** on the free instance -- plenty for this app's
  scan workload, but don't run anything else heavy alongside it.
- **Ephemeral disk:** the SQLite database (`scanner_v3.db`) resets on
  every redeploy and most restarts. Signal history won't persist long
  term on the free tier. This doesn't affect live scanning -- only the
  historical log in the database.

## Environment variables (optional, set in Render's dashboard)

| Variable                | Default | Purpose                                  |
|--------------------------|---------|-------------------------------------------|
| `SCAN_INTERVAL_SECONDS`  | `300`   | Seconds between automatic rescans         |
| `FIRST_SCAN_DELAY`       | `8`     | Seconds after startup before first scan   |
| `PROXY_URL`              | (unset) | Optional HTTP/HTTPS proxy for API calls   |
| `DB_PATH`                | `scanner_v3.db` | SQLite file path (ephemeral by default) |

## Local testing before deploying

```
pip install -r requirements.txt
PORT=5000 python app.py
```

Then open `http://localhost:5000` or `http://localhost:5000/mobile`.

To test the exact production command Render will run:

```
gunicorn --bind 0.0.0.0:5000 --workers 1 --threads 8 --timeout 120 app:app
```
