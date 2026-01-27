import os
import sys
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

os.makedirs(DATA_DIR, exist_ok=True)
exchange = ccxt.binance({'enableRateLimit': True})

# --- Helper Functions ---

def get_filename(symbol: str) -> str:
    """
    Converts 'BTC/USDT' -> 'btc1m.csv'
    """
    base = symbol.split('/')[0].lower() # Takes 'BTC' from 'BTC/USDT' and lowercases it
    return os.path.join(DATA_DIR, f"{base}1m.csv")

def save_data(symbol: str, data: list):
    if not data: return
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.sort_values('timestamp', inplace=True)
    df.drop_duplicates(subset='timestamp', keep='last', inplace=True)
    
    filename = get_filename(symbol)
    df.to_csv(filename, index=False)
    print(f"[{symbol}] SAVED {len(df)} rows to {filename}")

def fetch_worker():
    """Background thread that downloads data."""
    print("--- BACKGROUND DOWNLOADER STARTED ---")
    
    start_ts = exchange.parse8601(SINCE_STR)
    end_ts = exchange.parse8601(END_STR)
    duration = 60 * 1000

    for symbol in SYMBOLS:
        filename = get_filename(symbol)
        
        # Check if file exists to prevent re-downloading
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            print(f"[{symbol}] Found {os.path.basename(filename)}. Skipping download.")
            continue

        print(f"[{symbol}] Downloading to {os.path.basename(filename)}...")
        all_ohlcv = []
        current_since = start_ts
        
        while current_since < end_ts:
            try:
                ohlcv = exchange.fetch_ohlcv(symbol, TIMEFRAME, since=current_since, limit=1000)
                if not ohlcv:
                    current_since += (1000 * duration)
                    if current_since >= end_ts: break
                    continue

                ohlcv = [x for x in ohlcv if x[0] < end_ts]
                if not ohlcv: break

                all_ohlcv.extend(ohlcv)
                current_since = ohlcv[-1][0] + duration
                
                if len(all_ohlcv) % 50000 == 0:
                    print(f"[{symbol}] ... fetched {len(all_ohlcv)} rows")

            except Exception as e:
                print(f"[{symbol}] Error: {e}")
                import time; time.sleep(5)

        save_data(symbol, all_ohlcv)
    
    print("--- BACKGROUND DOWNLOADER FINISHED ---")

# --- FastAPI Lifecycle ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = threading.Thread(target=fetch_worker, daemon=True)
    thread.start()
    yield

app = FastAPI(lifespan=lifespan)

# --- SERVE FILES ---
# Access via: http://localhost:8000/data/btc1m.csv
app.mount("/data", StaticFiles(directory=DATA_DIR), name="data")

@app.get("/")
def index():
    files = os.listdir(DATA_DIR)
    links = {f: f"/data/{f}" for f in files if f.endswith(".csv")}
    return {"status": "running", "available_files": links}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
