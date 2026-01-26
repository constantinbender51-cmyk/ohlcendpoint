import os
import time
import pandas as pd
import ccxt
from fastapi import FastAPI, HTTPException, BackgroundTasks
from typing import Optional

# --- Configuration ---
DATA_DIR = "/app/data/"
TIMEFRAME = '1m'
SINCE_STR = '2020-01-01 00:00:00'
END_STR = '2026-01-01 00:00:00'  # Hard cutoff before 2026 starts
EXCHANGE_ID = 'binance'

# Target Symbols
SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "XRP/USDT", "SOL/USDT", "DOGE/USDT",
    "ADA/USDT", "BCH/USDT", "LINK/USDT", "XLM/USDT", "SUI/USDT",
    "AVAX/USDT", "LTC/USDT", "HBAR/USDT", "SHIB/USDT", "TON/USDT",
]

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# Initialize Exchange
exchange = ccxt.binance({
    'enableRateLimit': True, 
    'options': {'defaultType': 'spot'}
})

app = FastAPI(title="Crypto OHLC Server")

# --- Helper Functions ---

def get_filename(symbol: str) -> str:
    return os.path.join(DATA_DIR, f"{symbol.replace('/', '_')}.csv")

def parse_date_to_ms(date_str: str) -> int:
    return exchange.parse8601(date_str)

def fetch_ohlcv_pagination(symbol: str, start_ts: int, end_ts: int):
    """
    Fetches OHLCV data from start_ts up to end_ts.
    Stops exactly at end_ts (2026-01-01).
    """
    all_ohlcv = []
    current_since = start_ts
    timeframe_duration_ms = 60 * 1000  # 1m in ms
    
    print(f"[{symbol}] Fetching range: {exchange.iso8601(start_ts)} -> {exchange.iso8601(end_ts)}")

    while current_since < end_ts:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, TIMEFRAME, since=current_since, limit=1000)
            
            if not ohlcv:
                print(f"[{symbol}] No data at {exchange.iso8601(current_since)}. Advancing...")
                current_since += (1000 * timeframe_duration_ms)
                if current_since >= end_ts:
                    break
                continue

            # Filter out any candles that might go beyond the end_ts
            # (Rare, but exchange might return a candle exactly at the boundary if inclusive)
            ohlcv = [x for x in ohlcv if x[0] < end_ts]
            
            if not ohlcv:
                break

            all_ohlcv.extend(ohlcv)
            last_candle_ts = ohlcv[-1][0]
            
            # Move iterator forward
            next_since = last_candle_ts + timeframe_duration_ms
            
            if next_since <= current_since:
                current_since += (1000 * timeframe_duration_ms)
            else:
                current_since = next_since

            # Log progress
            print(f"[{symbol}] Fetched {len(ohlcv)} candles. Latest: {exchange.iso8601(last_candle_ts)}")

        except Exception as e:
            print(f"[{symbol}] Error: {e}")
            time.sleep(5)

    return all_ohlcv

def save_data(symbol: str, data: list):
    if not data:
        return
    
    filename = get_filename(symbol)
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    # Sort and drop duplicates to ensure clean data
    df.sort_values('timestamp', inplace=True)
    df.drop_duplicates(subset='timestamp', keep='last', inplace=True)
    
    df.to_csv(filename, index=False)
    print(f"[{symbol}] Saved {len(df)} rows to {filename}")

def load_data(symbol: str) -> Optional[pd.DataFrame]:
    filename = get_filename(symbol)
    if os.path.exists(filename):
        return pd.read_csv(filename)
    return None

def sync_symbol(symbol: str):
    start_ms = parse_date_to_ms(SINCE_STR)
    end_ms = parse_date_to_ms(END_STR)
    
    # Check if we already have data to potentially resume or skip
    # For strict compliance, we fetch the defined range
    data = fetch_ohlcv_pagination(symbol, start_ms, end_ms)
    save_data(symbol, data)

# --- HTTP Endpoints ---

@app.post("/fetch-all")
async def trigger_fetch(background_tasks: BackgroundTasks):
    def run_batch():
        for symbol in SYMBOLS:
            sync_symbol(symbol)
    background_tasks.add_task(run_batch)
    return {"message": "Background fetch started (2020-2025)."}

@app.get("/data/{symbol_ticker}")
def get_ohlc(symbol_ticker: str, limit: int = 100):
    target_symbol = f"{symbol_ticker.upper()}/USDT"
    if target_symbol not in SYMBOLS:
        raise HTTPException(status_code=404, detail="Symbol not monitored")
    
    df = load_data(target_symbol)
    if df is None:
        raise HTTPException(status_code=404, detail="Data not found")
    
    return {
        "symbol": target_symbol,
        "count": len(df),
        "data": df.tail(limit).to_dict(orient="records")
    }

if __name__ == "__main__":
    import uvicorn
    # Load check
    for sym in SYMBOLS:
        df = load_data(sym)
        if df is not None:
            print(f"[{sym}] Loaded {len(df)} records.")
            
    uvicorn.run(app, host="0.0.0.0", port=8000)
