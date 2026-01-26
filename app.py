import os
import sys
import threading
import pandas as pd
import ccxt
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException

# --- 1. Force Unbuffered Logging (Fixes missing logs) ---
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
    return os.path.join(DATA_DIR, f"{symbol.replace('/', '_')}.csv")

def save_data(symbol: str, data: list):
    if not data: return
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.sort_values('timestamp', inplace=True)
    df.drop_duplicates(subset='timestamp', keep='last', inplace=True)
    
    filename = get_filename(symbol)
    df.to_csv(filename, index=False)
    print(f"[{symbol}] SAVED {len(df)} rows to disk.")

def fetch_worker():
    """Background thread that runs once on startup."""
    print("--- STARTUP: BACKGROUND DOWNLOADER STARTED ---")
    
    start_ts = exchange.parse8601(SINCE_STR)
    end_ts = exchange.parse8601(END_STR)
    timeframe_duration_ms = 60 * 1000

    for symbol in SYMBOLS:
        filename = get_filename(symbol)
        
        # 3. Load if found (Skip download if file exists to save time on restarts)
        if os.path.exists(filename):
            print(f"[{symbol}] Found local file. Skipping download.")
            continue

        print(f"[{symbol}] Downloading data (2020-2026)...")
        
        # Fetch Logic
        all_ohlcv = []
        current_since = start_ts
        
        while current_since < end_ts:
            try:
                ohlcv = exchange.fetch_ohlcv(symbol, TIMEFRAME, since=current_since, limit=1000)
                if not ohlcv:
                    current_since += (1000 * timeframe_duration_ms)
                    if current_since >= end_ts: break
                    continue

                # Filter > 2026
                ohlcv = [x for x in ohlcv if x[0] < end_ts]
                if not ohlcv: break

                all_ohlcv.extend(ohlcv)
                current_since = ohlcv[-1][0] + timeframe_duration_ms
                
                # Optional: Partial save or log every 50k rows could go here
                if len(all_ohlcv) % 10000 == 0:
                    print(f"[{symbol}] ... fetched {len(all_ohlcv)} candles so far")

            except Exception as e:
                print(f"[{symbol}] Error: {e}")
                import time; time.sleep(5)

        save_data(symbol, all_ohlcv)
    
    print("--- BACKGROUND DOWNLOADER FINISHED ---")

# --- FastAPI Lifecycle ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the downloader in a separate thread so it doesn't block the API
    thread = threading.Thread(target=fetch_worker, daemon=True)
    thread.start()
    yield
    # (Optional) Cleanup logic here if needed

app = FastAPI(lifespan=lifespan)

# --- Endpoints ---

@app.get("/")
def index():
    return {"status": "running", "local_data": os.listdir(DATA_DIR)}

@app.get("/data/{symbol_ticker}")
def get_ohlc(symbol_ticker: str, limit: int = 100):
    target = f"{symbol_ticker.upper()}/USDT"
    filename = get_filename(target)
    
    if not os.path.exists(filename):
        # If file is currently downloading, it might not exist yet
        raise HTTPException(404, detail="Data downloading or not found")
        
    df = pd.read_csv(filename)
    return {
        "symbol": target,
        "total_records": len(df),
        "data": df.tail(limit).to_dict(orient="records")
    }

@app.get("/download/{symbol_ticker}")
def download_full_csv(symbol_ticker: str):
    target = f"{symbol_ticker.upper()}/USDT"
    file_path = get_filename(target) # e.g., /app/data/BTC_USDT.csv
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        path=file_path, 
        filename=os.path.basename(file_path), # <--- Forces download name to be "BTC_USDT.csv"
        media_type='text/csv'
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
