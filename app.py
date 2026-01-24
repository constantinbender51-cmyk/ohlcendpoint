import os
import sys
import time
import threading
import datetime
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

# Ensure data directory exists
if not os.path.exists(DATA_DIR):
    try:
        os.makedirs(DATA_DIR)
        print(f"Created directory: {DATA_DIR}")
        sys.stdout.flush()
    except OSError as e:
        print(f"Error creating directory {DATA_DIR}: {e}")
        sys.stdout.flush()

# Flask App
app = Flask(__name__)

# --- Helper Functions ---

def log(msg):
    """Prints message and forces stdout flush."""
    # Add timestamp to logs for clarity in Railway
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")
    sys.stdout.flush()

def get_filename(symbol):
    """Returns the absolute filepath for a given symbol."""
    clean_symbol = symbol.replace('/', '')
    ticker = clean_symbol.replace('USDT', '') 
    filename = f"{ticker}{CSV_SUFFIX}"
    return os.path.join(DATA_DIR, filename)

# --- Data Management ---

def load_existing_data(filepath):
    """Loads existing CSV if present, otherwise returns empty DataFrame."""
    if os.path.exists(filepath):
        try:
            df = pd.read_csv(filepath)
            df['timestamp'] = df['timestamp'].astype(int)
            return df
        except Exception as e:
            log(f"Error reading {filepath}: {e}")
    
    return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

def save_data(df, filepath):
    """Saves DataFrame to CSV."""
    df.to_csv(filepath, index=False)

# --- Fetcher Logic ---

def fetch_worker():
    """Background thread to fetch OHLC data."""
    # Slight delay to ensure app is fully loaded before logic starts
    time.sleep(2)
    
    exchange = ccxt.binance({
        'enableRateLimit': True, 
        'options': {'defaultType': 'spot'}
    })
    
    start_ts = int(datetime.datetime.strptime(START_DATE, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

    log("--- Fetcher Thread Started (Background) ---")

    while True:
        for coin in COINS:
            symbol = f"{coin}{PAIR_SUFFIX}"
            filepath = get_filename(coin)
            
            # 1. Load current state
            df = load_existing_data(filepath)
            
            # 2. Determine 'since' parameter
            if not df.empty:
                last_ts = df['timestamp'].iloc[-1]
                since = last_ts + 60000 
            else:
                since = start_ts

            # Check if we are already up to date (allow 2 mins buffer)
            now_ts = exchange.milliseconds()
            if since > (now_ts - 120000):
                continue

            # 3. Fetch Data
            try:
                ohlcv = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, since=since, limit=1000)
                
                if ohlcv:
                    new_data = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    
                    df = pd.concat([df, new_data])
                    df = df.drop_duplicates(subset=['timestamp'], keep='last')
                    df = df.sort_values(by='timestamp')
                    
                    save_data(df, filepath)
                    
                    last_time_readable = datetime.datetime.fromtimestamp(ohlcv[-1][0]/1000).strftime('%Y-%m-%d %H:%M')
                    log(f"[{coin}] Updated. Last candle: {last_time_readable}. Rows: {len(df)}")
                
            except ccxt.BadSymbol:
                log(f"[{coin}] Symbol {symbol} not found.")
            except Exception as e:
                log(f"[{coin}] Fetch error: {e}")
            
            time.sleep(1) 
        
        time.sleep(1)

# --- Server Logic ---

@app.route('/download/<coin>', methods=['GET'])
def download_file(coin):
    coin = coin.upper()
    if coin not in COINS:
        abort(404, description="Coin not tracked.")
    
    filepath = get_filename(coin)
    
    if os.path.exists(filepath):
        return send_file(filepath, as_attachment=True)
    else:
        abort(404, description="File not found.")

def server_worker():
    """Legacy: Only used if running locally via python main.py"""
    log("--- Server Thread Started on port 5000 ---")
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

# --- GLOBAL EXECUTION (Run on Import) ---

# We define a startup function to ensure the thread starts 
# regardless of whether Gunicorn or Python runs the file.
def start_background_tasks():
    # Only start if not already running (basic check)
    # Note: In Gunicorn, every worker will run this. 
    # Ensure you set Railway "Start Command" to use 1 worker if possible, 
    # or rely on the OS locking files (risky).
    # For now, we assume 1 worker or we accept the race condition for simplicity.
    t_fetch = threading.Thread(target=fetch_worker, daemon=True)
    t_fetch.start()
    log("Background task initialized.")

# Trigger startup immediately upon import
start_background_tasks()

# --- Main Execution (Local Only) ---

if __name__ == "__main__":
    # If run locally, we also need to start the server manually.
    # In production (Gunicorn), this block is SKIPPED.
    server_worker()
