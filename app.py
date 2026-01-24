import os
import sys
import time
import datetime
import threading
import csv
import requests
import pandas as pd
from http.server import HTTPServer, BaseHTTPRequestHandler

# Configuration
DATA_DIR = "/app/data"
BINANCE_URL = "https://api.binance.com/api/v3/klines"
START_DATE = "2018-01-01"
END_DATE = "2026-01-01"

def get_asset_symbol():
    """Retrieves the asset symbol from the environment variable."""
    asset = os.environ.get("ASSET")
    if not asset:
        print("Error: ASSET environment variable not set. Example: ASSET=BTCUSDT")
        sys.exit(1)
    return asset.upper()

def ensure_directory():
    """Ensures the data directory exists."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def fetch_ohlcv(symbol):
    """
    Fetches 1m OHLCV data from Binance from 2018 to 2026.
    Handles pagination and rate limits.
    """
    filename = f"{symbol}1m.csv"
    filepath = os.path.join(DATA_DIR, filename)
    
    print(f"Starting fetch for {symbol} (1m) from {START_DATE} to {END_DATE}...")

    # Convert dates to milliseconds timestamp
    start_ts = int(datetime.datetime.strptime(START_DATE, "%Y-%m-%d").timestamp() * 1000)
    end_ts = int(datetime.datetime.strptime(END_DATE, "%Y-%m-%d").timestamp() * 1000)
    
    # Columns for Binance Kline data
    columns = [
        "Open Time", "Open", "High", "Low", "Close", "Volume",
        "Close Time", "Quote Asset Volume", "Number of Trades",
        "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore"
    ]
    
    # Open file in write mode to start fresh, write header
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columns)

    current_ts = start_ts
    
    while current_ts < end_ts:
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": current_ts,
            "endTime": end_ts,
            "limit": 1000
        }

        try:
            response = requests.get(BINANCE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            if not data:
                print(f"No more data available for {symbol} at timestamp {current_ts}.")
                break

            # Append batch to CSV
            with open(filepath, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(data)

            # Update timestamp: Last Close Time + 1ms
            last_close_time = data[-1][6]
            current_ts = last_close_time + 1
            
            # Progress indicator
            progress_date = datetime.datetime.fromtimestamp(current_ts / 1000)
            print(f"Fetched up to {progress_date}...", end='\r')

            # Rate limit handling (Binance weight is low for 1m, but safety first)
            time.sleep(0.1) 

        except requests.exceptions.RequestException as e:
            print(f"\nRequest failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

    print(f"\nData fetch complete. Saved to {filepath}")

class SingleFileHandler(BaseHTTPRequestHandler):
    """
    Custom HTTP Handler that serves only the specific CSV file
    assigned to its server instance.
    """
    def __init__(self, target_file, *args, **kwargs):
        self.target_file = target_file
        super().__init__(*args, **kwargs)

    def do_GET(self):
        try:
            # We serve the file regardless of the path requested to ensure strictly one file per server
            if os.path.exists(self.target_file):
                with open(self.target_file, 'rb') as f:
                    content = f.read()
                
                self.send_response(200)
                self.send_header('Content-type', 'text/csv')
                self.send_header('Content-Disposition', f'attachment; filename="{os.path.basename(self.target_file)}"')
                self.end_headers()
                self.wfile.write(content)
            else:
                self.send_error(404, "File not found")
        except Exception as e:
            self.send_error(500, f"Internal Server Error: {str(e)}")

    def log_message(self, format, *args):
        # Suppress default logging to keep console clean
        pass

def start_server_thread(filepath, port):
    """Starts a HTTPServer for a specific file in a new thread."""
    def handler_factory(*args, **kwargs):
        return SingleFileHandler(filepath, *args, **kwargs)

    server = HTTPServer(('0.0.0.0', port), handler_factory)
    print(f"Serving {os.path.basename(filepath)} on port {port} (Thread: {threading.current_thread().name})")
    server.serve_forever()

def serve_csvs():
    """Scans the data directory and spawns a thread/server for every CSV found."""
    files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
    
    if not files:
        print("No CSV files found to serve.")
        return

    base_port = 8000
    threads = []

    print("\n--- Starting Servers ---")
    for i, filename in enumerate(files):
        filepath = os.path.join(DATA_DIR, filename)
        port = base_port + i
        
        # Create a thread for this specific file server
        t = threading.Thread(target=start_server_thread, args=(filepath, port), name=f"Server-{filename}")
        t.daemon = True # Daemon threads exit when main program exits
        t.start()
        threads.append(t)

    print(f"Active servers: {len(threads)}. Press Ctrl+C to stop.")
    
    # Keep the main thread alive to allow daemon threads to run
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping servers...")

def main():
    ensure_directory()
    
    # 1. Fetch Data
    asset = get_asset_symbol()
    fetch_ohlcv(asset)
    
    # 2. Serve Data
    serve_csvs()

if __name__ == "__main__":
    main()
