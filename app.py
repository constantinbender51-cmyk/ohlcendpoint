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
    except OSError as e:
        print(f"Error creating directory {DATA_DIR}: {e}")
        sys.stdout.flush()

# Flask App
app = Flask(__name__)

# --- Helper Functions ---

def log(msg):
    """Prints message and forces stdout flush."""
    print(msg)
    sys.stdout.flush()

def get_filename(symbol):
    """Returns the absolute filepath for a given symbol (e.g., /app/data/BTC1m.csv)."""
    clean_symbol = symbol.replace('/', '')
    # User requested format: xxx1m.csv (using the coin ticker)
    ticker = clean_symbol.replace('USDT', '') 
    filename = f"{ticker}{CSV_SUFFIX}"
    return os.path.join(DATA_DIR, filename)

# --- Data Management ---

def load_existing_data(filepath):
    """Loads existing CSV if present, otherwise returns empty DataFrame."""
    if os.path.exists(filepath):
        try:
            df = pd.read_csv(filepath)
            # Ensure timestamp is correct type for comparison
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
    exchange = ccxt.binance({
        'enableRateLimit': True, 
        'options': {'defaultType': 'spot'}
    })
    
    # parse start date to timestamp ms
    start_ts = int(datetime.datetime.strptime(START_DATE, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

    log("--- Fetcher Thread Started ---")

    while True:
        for coin in COINS:
            symbol = f"{coin}{PAIR_SUFFIX}"
            filepath = get_filename(coin)
            
            # 1. Load current state
            df = load_existing_data(filepath)
            
            # 2. Determine 'since' parameter
            if not df.empty:
                last_ts = df['timestamp'].iloc[-1]
                since = last_ts + 60000 # Start 1 minute after last record
            else:
                since = start_ts

            # Check if we are already up to date (allow 2 mins buffer)
            now_ts = exchange.milliseconds()
            if since > (now_ts - 120000):
                continue

            # 3. Fetch Data (Chunked)
            try:
                # Binance limit is usually 1000 candles per call
                ohlcv = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, since=since, limit=1000)
                
                if ohlcv:
                    new_data = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    
                    # Concat and Deduplicate
                    df = pd.concat([df, new_data])
                    df = df.drop_duplicates(subset=['timestamp'], keep='last')
                    df = df.sort_values(by='timestamp')
                    
                    save_data(df, filepath)
                    
                    # Convert last timestamp to readable for logging
                    last_time_readable = datetime.datetime.fromtimestamp(ohlcv[-1][0]/1000).strftime('%Y-%m-%d %H:%M')
                    log(f"[{coin}] Updated. Last candle: {last_time_readable}. Rows: {len(df)}")
                
                else:
                    # No data returned, possibly requesting future data or symbol issue
                    pass

            except ccxt.BadSymbol:
                log(f"[{coin}] Symbol {symbol} not found on exchange.")
            except Exception as e:
                log(f"[{coin}] Fetch error: {e}")
            
            # Rate limit sleep is handled partly by CCXT, but adding a small buffer helps CPU usage
            time.sleep(1) 
        
        # Determine global sleep. 
        # If we are catching up history, we loop fast. 
        # If we are live, we can sleep longer, but we check continually.
        time.sleep(1)

# --- Server Logic ---

@app.route('/download/<coin>', methods=['GET'])
def download_file(coin):
    """Endpoint to download the specific CSV file."""
    # Sanitize input
    coin = coin.upper()
    if coin not in COINS:
        abort(404, description="Coin not tracked.")
    
    filepath = get_filename(coin)
    
    if os.path.exists(filepath):
        return send_file(filepath, as_attachment=True)
    else:
        abort(404, description="File not found (fetching might be in progress).")

def server_worker():
    """Background thread to run Flask server."""
    log("--- Server Thread Started on port 5000 ---")
    # debug=False is critical for threading context
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

# --- Main Execution ---

if __name__ == "__main__":
    # 1. Start Fetcher Thread
    t_fetch = threading.Thread(target=fetch_worker, daemon=True)
    t_fetch.start()

    # 2. Start Server Thread (Blocking call, keeps main alive or reverse)
    # Since app.run blocks, we run it in a thread or main. 
    # Here we run it in a thread to allow potential main-thread management if extended later.
    t_server = threading.Thread(target=server_worker, daemon=True)
    t_server.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log("Stopping...")
