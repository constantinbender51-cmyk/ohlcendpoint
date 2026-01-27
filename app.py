import os
import sys
import glob
import threading
import pandas as pd
import ccxt
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

# --- 1. Force Unbuffered Logging ---
sys.stdout.reconfigure(line_buffering=True)

# --- Configuration ---
DATA_DIR = "/app/data/"
TIMEFRAME = '1m'
SINCE_STR = '2020-01-01 00:00:00'
END_STR = '2026-01-01 00:00:00'
SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "XRP/USDT", "SOL/USDT", "DOGE/USDT",
    "ADA/USDT", "BCH/USDT", "LINK/USDT", "XLM/USDT", "SUI/USDT",
    "AVAX/USDT", "LTC/USDT", "HBAR/USDT", "SHIB/USDT", "TON/USDT",
]

# Map "BTC/USDT" -> "btc1m.csv"
SYMBOL_TO_FILE = {
    s: f"{s.split('/')[0].lower()}1m.csv" for s in SYMBOLS
}

os.makedirs(DATA_DIR, exist_ok=True)
exchange = ccxt.binance({'enableRateLimit': True})

# --- Helper Functions ---

def get_file_path(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)

def get_last_timestamp(file_path: str) -> int:
    """
    Reads the last line of the CSV to get the last timestamp efficiently.
    Returns None if file is empty or invalid.
    """
    try:
        if os.path.getsize(file_path) < 100:
            return None # Too small to contain data
            
        with open(file_path, 'rb') as f:
            # Jump to near the end of the file
            try:
                f.seek(-1024, 2)
            except OSError:
                f.seek(0)
            
            lines = f.readlines()
            if not lines:
                return None
                
            # Get last line
            last_line = lines[-1].decode().strip()
            # Split by comma, first item is timestamp
            return int(last_line.split(',')[0])
    except Exception as e:
        print(f"Error reading last timestamp of {file_path}: {e}")
        return None

def append_data(file_path: str, data: list):
    if not data: return
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    # Append without header
    df.to_csv(file_path, mode='a', header=False, index=False)
    print(f"[{os.path.basename(file_path)}] Appended {len(df)} rows.")

def create_new_file(file_path: str, data: list):
    if not data: return
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    df.to_csv(file_path, mode='w', header=True, index=False)
    print(f"[{os.path.basename(file_path)}] Created new file with {len(df)} rows.")

def cleanup_directory():
    """Deletes files that are not in our target list."""
    print("--- CLEANUP STARTED ---")
    allowed_files = set(SYMBOL_TO_FILE.values())
    
    for filename in os.listdir(DATA_DIR):
        if filename not in allowed_files:
            full_path = os.path.join(DATA_DIR, filename)
            if os.path.isfile(full_path):
                os.remove(full_path)
                print(f"Deleted extraneous file: {filename}")
    print("--- CLEANUP FINISHED ---")

def fetch_worker():
    """Smart Background Downloader."""
    cleanup_directory()
    print("--- SMART SYNC STARTED ---")
    
    target_start_ms = exchange.parse8601(SINCE_STR)
    target_end_ms = exchange.parse8601(END_STR)
    duration_ms = 60 * 1000

    for symbol in SYMBOLS:
        filename = SYMBOL_TO_FILE[symbol]
        file_path = get_file_path(filename)
        
        start_fetch_from = target_start_ms
        mode = 'write' # 'write' or 'append'
        
        # 1. Check existing file status
        if os.path.exists(file_path):
            last_ts = get_last_timestamp(file_path)
            
            if last_ts:
                readable_last = exchange.iso8601(last_ts)
                
                if last_ts >= (target_end_ms - duration_ms):
                    print(f"[{symbol}] Complete ({readable_last}). Skipping.")
                    continue
                else:
                    print(f"[{symbol}] Incomplete. Ends at {readable_last}. Resuming...")
                    start_fetch_from = last_ts + duration_ms
                    mode = 'append'
            else:
                print(f"[{symbol}] File invalid/empty. Re-downloading from scratch.")
        
        # 2. Fetch Loop
        current_since = start_fetch_from
        batch_data = []
        
        print(f"[{symbol}] Fetching {exchange.iso8601(current_since)} -> {END_STR}...")
        
        while current_since < target_end_ms:
            try:
                ohlcv = exchange.fetch_ohlcv(symbol, TIMEFRAME, since=current_since, limit=1000)
                
                if not ohlcv:
                    # No data found (gap or coin didn't exist yet)
                    current_since += (1000 * duration_ms)
                    if current_since >= target_end_ms: break
                    continue

                # Filter logic: Stop at 2026 strict
                ohlcv = [x for x in ohlcv if x[0] < target_end_ms]
                if not ohlcv: break

                batch_data.extend(ohlcv)
                current_since = ohlcv[-1][0] + duration_ms
                
                # Save/Append in chunks of 50k to avoid memory spikes
                if len(batch_data) >= 50000:
                    if mode == 'write':
                        create_new_file(file_path, batch_data)
                        mode = 'append' # Switch to append for subsequent chunks
                    else:
                        append_data(file_path, batch_data)
                    batch_data = [] # Clear memory

            except Exception as e:
                print(f"[{symbol}] Error: {e}")
                import time; time.sleep(5)

        # Flush remaining data
        if batch_data:
            if mode == 'write':
                create_new_file(file_path, batch_data)
            else:
                append_data(file_path, batch_data)
        
    print("--- SMART SYNC FINISHED ---")

# --- FastAPI Lifecycle ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = threading.Thread(target=fetch_worker, daemon=True)
    thread.start()
    yield

app = FastAPI(lifespan=lifespan)

# Serve files directly at /data/btc1m.csv
app.mount("/data", StaticFiles(directory=DATA_DIR), name="data")

@app.get("/")
def index():
    files = os.listdir(DATA_DIR) if os.path.exists(DATA_DIR) else []
    links = {f: f"/data/{f}" for f in files}
    return {"status": "running", "files": links}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
