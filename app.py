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
SERVER_PORT = 8000

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
    If the file already exists, the download is skipped.
    """
    filename = f"{symbol}1m.csv"
    filepath = os.path.join(DATA_DIR, filename)

    # Check if file already exists
    if os.path.exists(filepath):
        print(f"Data for {symbol} already exists at {filepath}. Skipping download.")
        return
    
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

class CSVRequestHandler(BaseHTTPRequestHandler):
    """
    Custom HTTP Handler that serves a file list on root and downloads CSVs on specific paths.
    """
    def do_GET(self):
        try:
            # Route 1: Root / displays the list of files
            if self.path == '/':
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                
                files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
                
                html = "<html><head><title>Data Files</title></head><body>"
                html += "<h1>Available CSV Files</h1><ul>"
                
                if files:
                    for f in files:
                        html += f'<li><a href="/{f}">{f}</a></li>'
                else:
                    html += "<li>No files found.</li>"
                    
                html += "</ul></body></html>"
                self.wfile.write(html.encode('utf-8'))
                return

            # Route 2: /filename.csv downloads the file
            # Strip leading slash to get filename
            requested_file = self.path.lstrip('/')
            
            # Basic security: ensure no directory traversal and strict csv extension
            if '..' in requested_file or not requested_file.endswith('.csv'):
                 self.send_error(403, "Forbidden")
                 return

            filepath = os.path.join(DATA_DIR, requested_file)

            if os.path.exists(filepath):
                with open(filepath, 'rb') as f:
                    content = f.read()
                
                self.send_response(200)
                self.send_header('Content-type', 'text/csv')
                self.send_header('Content-Disposition', f'attachment; filename="{requested_file}"')
                self.end_headers()
                self.wfile.write(content)
            else:
                self.send_error(404, "File not found")
                
        except Exception as e:
            self.send_error(500, f"Internal Server Error: {str(e)}")

    def log_message(self, format, *args):
        # Suppress default logging to keep console clean
        pass

def serve_csvs():
    """Starts a single HTTPServer to serve the data directory."""
    server = HTTPServer(('0.0.0.0', SERVER_PORT), CSVRequestHandler)
    print(f"\nServer running on port {SERVER_PORT}")
    print(f"Access list at: http://localhost:{SERVER_PORT}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server...")
        server.server_close()

def main():
    ensure_directory()
    
    # 1. Fetch Data
    asset = get_asset_symbol()
    fetch_ohlcv(asset)
    
    # 2. Serve Data
    serve_csvs()

if __name__ == "__main__":
    main()
