import os
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

# Flask App
app = Flask(__name__)

# --- Data Management ---

def get_filename(symbol):
    """Returns the filename for a given symbol (e.g., BTC -> BTC1m.csv)."""
    clean_symbol = symbol.replace('/', '')
    # User requested format: xxx1m.csv (using the coin ticker)
    ticker = clean_symbol.replace('USDT', '') 
    return f"{ticker}{CSV_SUFFIX}"

def load_existing_data(filepath):
    """Loads existing CSV if present, otherwise returns empty DataFrame."""
    if os.path.exists(filepath):
        try:
            df = pd.read_csv(filepath)
            # Ensure timestamp is correct type for comparison
            df['timestamp'] = df['timestamp'].astype(int)
            return df
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
    
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

    print("--- Fetcher Thread Started ---")

    while True:
        for coin in COINS:
            symbol = f"{coin}{PAIR_SUFFIX}"
            filename = get_filename(coin)
            
            # 1. Load current state
            df = load_existing_data(filename)
            
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
                    
                    save_data(df, filename)
                    
                    # Convert last timestamp to readable for logging
                    last_time_readable = datetime.datetime.fromtimestamp(ohlcv[-1][0]/1000).strftime('%Y-%m-%d %H:%M')
                    print(f"[{coin}] Updated. Last candle: {last_time_readable}. Rows: {len(df)}")
                
                else:
                    # No data returned, possibly requesting future data or symbol issue
                    pass

            except ccxt.BadSymbol:
                print(f"[{coin}] Symbol {symbol} not found on exchange.")
            except Exception as e:
                print(f"[{coin}] Fetch error: {e}")
            
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
    
    filename = get_filename(coin)
    
    if os.path.exists(filename):
        return send_file(filename, as_attachment=True)
    else:
        abort(404, description="File not found (fetching might be in progress).")

def server_worker():
    """Background thread to run Flask server."""
    print("--- Server Thread Started on port 5000 ---")
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
        print("Stopping...")
