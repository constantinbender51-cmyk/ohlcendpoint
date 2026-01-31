import os
import sys
import threading
import pandas as pd
import ccxt
import gc
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# --- 1. Force Real-Time Logging ---
sys.stdout.reconfigure(line_buffering=True)

# --- Configuration ---
DATA_DIR = "/app/data/"
TIMEFRAME = '1m'
SINCE_STR = '2020-01-01 00:00:00'
END_STR = '2026-01-01 00:00:00'

# Timeframes to pre-generate
DERIVED_TFS = ['5m', '15m', '1h', '4h', '1d']

SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "XRP/USDT", "SOL/USDT", "DOGE/USDT",
    "ADA/USDT", "BCH/USDT", "LINK/USDT", "XLM/USDT", "SUI/USDT",
    "AVAX/USDT", "LTC/USDT", "HBAR/USDT", "SHIB/USDT", "TON/USDT",
]

# Base Map: "BTC/USDT" -> "btc"
SYMBOL_SLUGS = {
    s: s.split('/')[0].lower() for s in SYMBOLS
}

os.makedirs(DATA_DIR, exist_ok=True)
exchange = ccxt.binance({'enableRateLimit': True})

# --- Helper Functions ---

def get_filename(symbol_slug: str, timeframe: str) -> str:
    return f"{symbol_slug}{timeframe}.csv"

def get_file_path(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)

def is_file_complete(file_path: str, target_end_ms: int) -> bool:
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) < 100:
            return False
        with open(file_path, 'rb') as f:
            try:
                f.seek(-1024, 2)
            except OSError:
                f.seek(0)
            lines = f.readlines()
            if not lines: return False
            last_line = lines[-1].decode().strip()
            if not last_line: return False
            last_ts = int(last_line.split(',')[0])
            # 2 min tolerance
            return last_ts >= (target_end_ms - 120000)
    except Exception:
        return False

def append_to_csv(file_path: str, data: list):
    if not data: return
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.to_csv(file_path, mode='a', header=False, index=False)
    del df
    gc.collect()

def create_new_csv(file_path: str, data: list):
    if not data: return
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.to_csv(file_path, mode='w', header=True, index=False)
    print(f"[{os.path.basename(file_path)}] Created new file with {len(df)} rows.")
    del df
    gc.collect()

def generate_derived_files(symbol_slug: str, base_file_path: str):
    """Loads 1m file and generates 5m, 1h, etc."""
    print(f"[{symbol_slug}] Generating derived timeframes...")
    
    if not os.path.exists(base_file_path):
        print(f"[{symbol_slug}] Base file missing. Skipping derived.")
        return

    # Load Base
    try:
        df = pd.read_csv(base_file_path)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('datetime', inplace=True)
    except Exception as e:
        print(f"[{symbol_slug}] Error reading base file: {e}")
        return

    tf_map = {
        '5m': '5min', '15m': '15min', 
        '1h': '1h', '4h': '4h', 
        '1d': '1D'
    }

    agg_dict = {
        'timestamp': 'first', # Start of bucket
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }

    for tf in DERIVED_TFS:
        target_file = get_file_path(get_filename(symbol_slug, tf))
        rule = tf_map.get(tf)
        
        try:
            # Resample
            resampled = df.resample(rule).agg(agg_dict)
            resampled.dropna(inplace=True)
            
            # Fix timestamp (ensure it's int)
            resampled['timestamp'] = resampled['timestamp'].astype('int64')
            
            # Save
            cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            resampled[cols].to_csv(target_file, index=False)
            print(f"[{symbol_slug}] -> Generated {tf}")
            
            del resampled
        except Exception as e:
            print(f"[{symbol_slug}] Failed to generate {tf}: {e}")
            
    del df
    gc.collect()

def fetch_worker():
    print("--- STRICT SYNC STARTED ---")
    
    target_start_ms = exchange.parse8601(SINCE_STR)
    target_end_ms = exchange.parse8601(END_STR)
    duration_ms = 60 * 1000

    for symbol in SYMBOLS:
        slug = SYMBOL_SLUGS[symbol]
        filename = get_filename(slug, '1m')
        file_path = get_file_path(filename)
        
        # 1. Check/Sync Base File
        needs_sync = False
        
        if is_file_complete(file_path, target_end_ms):
            print(f"[{filename}] COMPLETE. Checking derived...")
        else:
            needs_sync = True
            if os.path.exists(file_path):
                print(f"[{filename}] INCOMPLETE. Deleting...")
                try:
                    os.remove(file_path)
                except OSError: pass

            print(f"[{filename}] Starting download...")
            current_since = target_start_ms
            batch_data = []
            mode = 'write'
            
            while current_since < target_end_ms:
                try:
                    ohlcv = exchange.fetch_ohlcv(symbol, TIMEFRAME, since=current_since, limit=1000)
                    if not ohlcv:
                        current_since += (1000 * duration_ms)
                        if current_since >= target_end_ms: break
                        continue

                    ohlcv = [x for x in ohlcv if x[0] < target_end_ms]
                    if not ohlcv: break

                    batch_data.extend(ohlcv)
                    current_since = ohlcv[-1][0] + duration_ms
                    
                    if len(batch_data) >= 50000:
                        if mode == 'write':
                            create_new_csv(file_path, batch_data)
                            mode = 'append'
                        else:
                            append_to_csv(file_path, batch_data)
                        print(f"[{filename}] ... {exchange.iso8601(current_since)}")
                        batch_data = []
                        gc.collect()

                except Exception as e:
                    print(f"Retrying {symbol}: {e}")
                    import time; time.sleep(5)

            if batch_data:
                if mode == 'write': create_new_csv(file_path, batch_data)
                else: append_to_csv(file_path, batch_data)
            
            del batch_data
            gc.collect()

        # 2. Generate Derived Files
        # We regenerate if main file was synced OR if a specific derived file is missing
        derived_missing = any(not os.path.exists(get_file_path(get_filename(slug, tf))) for tf in DERIVED_TFS)
        
        if needs_sync or derived_missing:
            generate_derived_files(slug, file_path)
        else:
            print(f"[{slug}] All files up to date.")

    print("--- ALL FILES SYNCED ---")

# --- FastAPI Lifecycle ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = threading.Thread(target=fetch_worker, daemon=True)
    thread.start()
    yield

app = FastAPI(lifespan=lifespan)

# --- Serve Files ---
app.mount("/data", StaticFiles(directory=DATA_DIR), name="data")

@app.get("/")
def index():
    files = sorted(os.listdir(DATA_DIR)) if os.path.exists(DATA_DIR) else []
    links = {f: f"/data/{f}" for f in files if f.endswith('.csv')}
    return {
        "status": "active", 
        "files": links,
        "usage": "GET /export?symbol=BTC/USDT&timeframe=1h"
    }

@app.get("/export")
def export_data(symbol: str = Query(..., description="e.g. BTC/USDT"), 
               timeframe: str = Query("1m", description="1m, 5m, 15m, 1h, 4h, 1d")):
    
    # Normalize
    sym = symbol.strip().upper().replace('-', '/').replace('_', '/')
    if sym not in SYMBOL_SLUGS:
        return {"error": "Symbol not found", "available": list(SYMBOL_SLUGS.keys())}
    
    slug = SYMBOL_SLUGS[sym]
    target_tf = timeframe.lower()
    
    # Validate timeframe
    allowed = ['1m'] + DERIVED_TFS
    if target_tf not in allowed:
        return {"error": f"Timeframe {target_tf} not pre-generated.", "available": allowed}

    filename = get_filename(slug, target_tf)
    file_path = get_file_path(filename)
    
    if not os.path.exists(file_path):
        return {"error": "File not generated yet. Please wait for sync."}
        
    return FileResponse(
        file_path, 
        media_type='text/csv', 
        filename=filename
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
