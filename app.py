import os
import sys
import threading
import pandas as pd
import ccxt
import gc
import io
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse, FileResponse

# --- 1. Force Real-Time Logging ---
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

# Map: "BTC/USDT" -> "btc1m.csv"
SYMBOL_TO_FILE = {
    s: f"{s.split('/')[0].lower()}1m.csv" for s in SYMBOLS
}

os.makedirs(DATA_DIR, exist_ok=True)
exchange = ccxt.binance({'enableRateLimit': True})

# --- Helper Functions ---

def get_file_path(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)

def is_file_complete(file_path: str, target_end_ms: int) -> bool:
    """
    Returns True ONLY if the file exists and ends within 2 mins of 2026.
    Returns False if missing, empty, or incomplete.
    """
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
            
            # Check if last timestamp is close to 2026 cutoff
            # 120,000 ms = 2 minutes tolerance
            if last_ts >= (target_end_ms - 120000):
                return True
            else:
                return False
    except Exception:
        return False

def append_to_csv(file_path: str, data: list):
    """Appends data to CSV."""
    if not data: return
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.to_csv(file_path, mode='a', header=False, index=False)
    del df
    gc.collect()

def create_new_csv(file_path: str, data: list):
    """Creates new CSV."""
    if not data: return
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.to_csv(file_path, mode='w', header=True, index=False)
    print(f"[{os.path.basename(file_path)}] Created new file with {len(df)} rows.")
    del df
    gc.collect()

def fetch_worker():
    print("--- STRICT SYNC STARTED ---")
    
    target_start_ms = exchange.parse8601(SINCE_STR)
    target_end_ms = exchange.parse8601(END_STR)
    duration_ms = 60 * 1000

    for symbol in SYMBOLS:
        filename = SYMBOL_TO_FILE[symbol]
        file_path = get_file_path(filename)
        
        # 1. STRICT CHECK
        if is_file_complete(file_path, target_end_ms):
            print(f"[{filename}] COMPLETE (2020-2026). Skipping.")
            continue
        
        # If we are here, file is either missing OR incomplete.
        if os.path.exists(file_path):
            print(f"[{filename}] INCOMPLETE/CORRUPTED. Deleting...")
            try:
                os.remove(file_path)
            except OSError as e:
                print(f"Error deleting {filename}: {e}")

        # 2. FRESH DOWNLOAD
        print(f"[{filename}] Starting fresh download (2020-2026)...")
        
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

                # Strict filter for 2026
                ohlcv = [x for x in ohlcv if x[0] < target_end_ms]
                if not ohlcv: break

                batch_data.extend(ohlcv)
                current_since = ohlcv[-1][0] + duration_ms
                
                # Write to disk every 50k rows
                if len(batch_data) >= 50000:
                    if mode == 'write':
                        create_new_csv(file_path, batch_data)
                        mode = 'append'
                    else:
                        append_to_csv(file_path, batch_data)
                    
                    print(f"[{filename}] ... reached {exchange.iso8601(current_since)}")
                    batch_data = [] # Clear memory
                    gc.collect()

            except Exception as e:
                print(f"[{filename}] Error: {e}. Retrying...")
                import time; time.sleep(5)

        # Final flush
        if batch_data:
            if mode == 'write':
                create_new_csv(file_path, batch_data)
            else:
                append_to_csv(file_path, batch_data)
            print(f"[{filename}] DOWNLOAD COMPLETE.")
            
        del batch_data
        gc.collect()
        
    print("--- ALL FILES SYNCED ---")

def resample_buffer(file_path: str, timeframe: str) -> io.StringIO:
    """Reads CSV, resamples, returns string buffer."""
    # Mapping
    tf_map = {
        '1m': '1min', '3m': '3min', '5m': '5min', '15m': '15min', '30m': '30min',
        '1h': '1h', '2h': '2h', '4h': '4h', '6h': '6h', '8h': '8h', '12h': '12h',
        '1d': '1D', '3d': '3D', '1w': '1W', '1M': '1ME'
    }
    
    rule = tf_map.get(timeframe)
    if not rule:
        # Generic fallback
        if timeframe.endswith('m'): rule = timeframe.replace('m', 'min')
        elif timeframe.endswith('h'): rule = timeframe.upper()
        elif timeframe.endswith('d'): rule = timeframe.upper()
        else: raise ValueError(f"Invalid timeframe: {timeframe}")

    # Load
    df = pd.read_csv(file_path)
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('datetime', inplace=True)
    
    # Resample
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    
    resampled = df.resample(rule).agg(agg_dict)
    resampled.dropna(inplace=True)
    
    # Reconstruct timestamp from index (start of bin)
    resampled['timestamp'] = resampled.index.astype('int64') // 10**6
    
    # Reorder columns
    cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    resampled = resampled[cols]
    
    # Write to buffer
    output = io.StringIO()
    resampled.to_csv(output, index=False)
    output.seek(0)
    
    del df
    del resampled
    gc.collect()
    
    return output

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
               timeframe: str = Query("1m", description="e.g. 1m, 1h, 1d")):
    
    # Normalize symbol
    sym = symbol.strip().upper()
    if sym not in SYMBOL_TO_FILE:
        # Fallback check for "BTCUSDT" or "BTC-USDT"
        normalized = sym.replace('-', '/').replace('_', '/')
        if normalized in SYMBOL_TO_FILE:
            sym = normalized
        else:
            return {"error": "Symbol not found", "available": list(SYMBOL_TO_FILE.keys())}
            
    filename = SYMBOL_TO_FILE[sym]
    file_path = get_file_path(filename)
    
    if not os.path.exists(file_path):
        return {"error": f"Data for {sym} not synced yet."}
        
    try:
        if timeframe == '1m':
            # Serve directly
            return FileResponse(file_path, media_type='text/csv', filename=f"{sym.replace('/', '')}_{timeframe}.csv")
        else:
            # Resample
            buf = resample_buffer(file_path, timeframe)
            return StreamingResponse(
                iter([buf.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={sym.replace('/', '')}_{timeframe}.csv"}
            )
    except Exception as e:
        return {"error": f"Processing error: {str(e)}"}
    finally:
        gc.collect()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
