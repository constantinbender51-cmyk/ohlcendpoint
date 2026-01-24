import os
import sys
import time
import threading
import datetime
import fcntl  # specific to Unix/Linux (Railway/Docker)
import pandas as pd
import ccxt
from flask import Flask, send_file, abort

# --- Configuration ---
COINS = [
    'BTC', 'ETH', 'XRP', 'SOL', 'DOGE', 
    'ADA', 'BCH', 'LINK', 'XLM', 'SUI', 
    'AVAX', 'LTC', 'HBAR', 'SHIB', 'TON'
]
PAIR_SUFFIX = '/USDT'
TIMEFRAME = '1m'
START_DATE = "2018-01-01 00:00:00"
CSV_SUFFIX = "1m.csv"
DATA_DIR = "/app/data"
LOCK_FILE = "/tmp/fetcher.lock"  # Lock file to ensure singleton thread

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# --- Logging ---
def log(msg, level="INFO"):
    """
    Production friendly logging. 
    Flushing stdout is required for container logs to appear immediately.
    """
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level}] {msg}")
    sys.stdout.flush()

# --- Data Management ---

def get_filepath(symbol):
    """Returns absolute path for CSV."""
    clean_symbol = symbol.replace('/', '').replace('USDT', '')
    return os.path.join(DATA_DIR, f"{clean_symbol}{CSV_SUFFIX}")

def load_data(filepath):
    if os.path.exists(filepath):
        try:
            df = pd.read_csv(filepath)
            df['timestamp'] = df['timestamp'].astype(int)
            return df
        except Exception as e:
            log(f"Corrupt file found {filepath}: {e}", level="ERROR")
    return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

def save_data(df, filepath):
    # Atomic write pattern: write to temp then rename to prevent read-conflicts
    tmp_path = filepath + ".tmp"
    df.to_csv(tmp_path, index=False)
    os.replace(tmp_path, filepath)

# --- Fetcher Logic ---

def fetch_loop():
    """Main loop for fetching data."""
    exchange = ccxt.binance({'enableRateLimit': True})
    start_ts = int(datetime.datetime.strptime(START_DATE, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
    
    log("Fetcher thread active and running.")

    while True:
        for coin in COINS:
            symbol = f"{coin}{PAIR_SUFFIX}"
            filepath = get_filepath(coin)
            
            df = load_data(filepath)
            
            if not df.empty:
                since = df['timestamp'].iloc[-1] + 60000
            else:
                since = start_ts

            # Optimization: Don't hit API if we are up to date (2 min buffer)
            if since > (exchange.milliseconds() - 120000):
                continue

            try:
                ohlcv = exchange.fetch_ohlcv(symbol, TIMEFRAME, since, 1000)
                if ohlcv:
                    new_df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df = pd.concat([df, new_df]).drop_duplicates('timestamp', keep='last').sort_values('timestamp')
                    save_data(df, filepath)
                    
                    last_date = datetime.datetime.fromtimestamp(ohlcv[-1][0]/1000)
                    log(f"[{coin}] Sync: {last_date}")
            except Exception as e:
                log(f"[{coin}] Error: {e}", level="WARN")
                time.sleep(5) # Backoff on error

            time.sleep(0.5) # Rate limit protection
        
        time.sleep(2) # Global loop delay

# --- Singleton Thread Manager ---

def start_background_thread():
    """
    Uses a non-blocking file lock to ensure ONLY ONE worker process 
    starts the background thread.
    """
    try:
        # Create/Open a lock file
        f = open(LOCK_FILE, 'w')
        # Try to acquire an exclusive lock (LOCK_EX) with non-blocking (LOCK_NB)
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        
        # If we got here, we own the lock. Start the thread.
        log("Lock acquired. Starting Background Fetcher.")
        t = threading.Thread(target=fetch_loop, daemon=True)
        t.start()
        
        # We purposely do not close 'f' here. We hold it open for the life of the process.
        # If this worker dies, the OS releases the file lock automatically.
        return True
    except IOError:
        # Another process holds the lock.
        log("Lock active in another worker. Fetcher thread skipped.")
        return False

# --- Flask Application ---

app = Flask(__name__)

# Trigger thread startup when this module is loaded by Gunicorn
# This will run in every worker, but only ONE will succeed in acquiring the lock.
start_background_thread()

@app.route('/health')
def health():
    return "OK", 200

@app.route('/download/<coin>')
def download(coin):
    coin = coin.upper()
    if coin not in COINS:
        abort(404, description="Invalid Coin")
    
    filepath = get_filepath(coin)
    if os.path.exists(filepath):
        return send_file(filepath, as_attachment=True)
    
    abort(404, description="Data not yet available")
