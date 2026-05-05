import yfinance as yf
import pandas as pd
import json
import time
import argparse
from datetime import datetime
import os
import sys

# Add the parent directory to the path so we can import the kafka_producer
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.kafka_producer import FinceptKafkaProducer

def fetch_and_feed(tickers, interval, broker, loop=False, delay=60):
    producer = FinceptKafkaProducer(bootstrap_servers=broker)
    
    # Track last sent timestamp per ticker to avoid duplicate Kafka messages
    last_sent_timestamps = {ticker: None for ticker in tickers.split()}
    
    print(f"Starting feeder for {tickers} at {interval} interval...")
    if loop:
        print(f"Loop mode enabled. Refreshing every {delay} seconds.")
    
    while True:
        # Download data from yfinance with retry logic
        max_retries = 3
        data = None
        
        for attempt in range(max_retries):
            try:
                # Use a small period for looping, but 1d is safest for yfinance 1m interval
                data = yf.download(tickers, period="1d", interval=interval, group_by="ticker", progress=False)
                if not data.empty:
                    break
            except Exception as e:
                print(f"Error downloading data: {e}")
            
            if attempt < max_retries - 1:
                time.sleep(2)
                
        if data is not None and not data.empty:
            ticker_list = tickers.split()
            if len(ticker_list) == 1:
                data = {tickers: data}
                
            records_sent_this_cycle = 0
            
            for ticker in ticker_list:
                time.sleep(1) # Be polite to Yahoo Finance API
                if ticker not in data or data[ticker].empty:
                    continue
                    
                df = data[ticker].reset_index().dropna(subset=['Open', 'Close'])
                print(f"Data for {ticker}: {df.shape[0]} valid rows found.")
                
                # Detect timestamp column (Datetime, Date, or index)
                dt_col = None
                for col in ['Datetime', 'Date', 'index']:
                    if col in df.columns:
                        dt_col = col
                        break
                
                if not dt_col:
                    print(f"Warning: No timestamp column found for {ticker}. Columns: {list(df.columns)}")
                    continue
                
                # Filter for only new records since the last poll
                for _, row in df.iterrows():
                    ts = row[dt_col]
                    
                    # Skip rows with missing timestamp or prices
                    if pd.isna(ts) or pd.isna(row['Open']) or pd.isna(row['Close']):
                        continue
                        
                    ts_iso = ts.isoformat()
                    
                    if last_sent_timestamps[ticker] is None or ts > last_sent_timestamps[ticker]:
                        try:
                            record = {
                                "symbol": ticker,
                                "timestamp": ts_iso,
                                "open": float(row['Open']),
                                "high": float(row['High']),
                                "low": float(row['Low']),
                                "close": float(row['Close']),
                                "volume": int(row['Volume'])
                            }
                            
                            topic = "market-data-minute" if "m" in interval or "h" in interval else "market-data-daily"
                            producer.produce(topic, record)
                            records_sent_this_cycle += 1
                            last_sent_timestamps[ticker] = ts
                        except (ValueError, TypeError) as e:
                            print(f"Error processing row for {ticker}: {e}")
                            continue
            
            if records_sent_this_cycle > 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent {records_sent_this_cycle} new records to Kafka.")
        
        if not loop:
            break
            
        time.sleep(delay)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Feed yfinance data to Kafka")
    parser.add_argument("--tickers", type=str, default="AAPL MSFT GOOGL NVDA TSLA AMZN BTC-USD", help="Space-separated list of tickers")
    parser.add_argument("--interval", type=str, default="1m", help="Data interval (e.g. 1m, 1d)")
    parser.add_argument("--broker", type=str, default="localhost:9093", help="Kafka broker address")
    parser.add_argument("--loop", action="store_true", help="Run in a continuous loop")
    parser.add_argument("--delay", type=int, default=60, help="Delay between polls in seconds")
    
    args = parser.parse_args()
    try:
        fetch_and_feed(args.tickers, args.interval, args.broker, args.loop, args.delay)
    except KeyboardInterrupt:
        print("\nFeeder stopped by user.")
        sys.exit(0)
