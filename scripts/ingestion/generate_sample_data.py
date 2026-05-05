#!/usr/bin/env python3
"""
Generate sample minute-level financial data for testing
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def generate_minute_data(symbol, start_date, end_date, base_price=100):
    """
    Generate minute-level OHLCV data for a symbol
    
    Args:
        symbol: Asset symbol (e.g., 'AAPL')
        start_date: Start datetime
        end_date: End datetime
        base_price: Starting price
    
    Returns:
        DataFrame with minute-level data
    """
    
    # Generate timestamps (market hours: 9:30 AM - 4:00 PM)
    timestamps = []
    current_date = start_date
    
    while current_date <= end_date:
        # Only include weekdays
        if current_date.weekday() < 5:
            # Market hours: 9:30 AM to 4:00 PM
            market_open = current_date.replace(hour=9, minute=30, second=0)
            market_close = current_date.replace(hour=16, minute=0, second=0)
            
            current_time = market_open
            while current_time <= market_close:
                timestamps.append(current_time)
                current_time += timedelta(minutes=1)
        
        current_date += timedelta(days=1)
    
    n_records = len(timestamps)
    
    # Generate price data with random walk
    returns = np.random.randn(n_records) * 0.001  # 0.1% std dev per minute
    price_series = base_price * (1 + returns).cumprod()
    
    # Generate OHLC from close prices
    data = []
    for i, timestamp in enumerate(timestamps):
        close_price = price_series[i]   # must be assigned before open_price references it
        open_price  = max(0.01, (price_series[i - 1] if i > 0 else base_price) + \
                      np.random.randn() * close_price * 0.0005)
        close_price = max(0.01, close_price)
        volatility  = close_price * 0.002
        high_price  = max(open_price, close_price) + abs(np.random.randn() * volatility)
        low_price   = max(0.01, min(open_price, close_price) - abs(np.random.randn() * volatility))
        volume      = int(np.random.exponential(100000))

        data.append({
            'symbol':         symbol,
            'timestamp':      timestamp,
            'open':           round(float(open_price),   2),
            'high':           round(float(high_price),   2),
            'low':            round(float(low_price),    2),
            'close':          round(float(close_price),  2),
            'volume':         volume,
            'adjusted_close': round(float(close_price),  2),
        })
    
    df = pd.DataFrame(data)
    return df

def main():
    """Generate sample data for multiple symbols"""
    
    # Configuration
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
    start_date = datetime.now() - timedelta(days=30)
    end_date = datetime.now()
    output_dir = '/opt/spark/data-external/raw/csv'
    
    print(f"Generating sample data for {len(symbols)} symbols...")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    
    os.makedirs(output_dir, exist_ok=True)
    
    for symbol in symbols:
        print(f"\nGenerating data for {symbol}...")
        
        # Generate data
        df = generate_minute_data(symbol, start_date, end_date)
        
        # Save to CSV
        output_file = os.path.join(output_dir, f'{symbol}_minute_data.csv')
        df.to_csv(output_file, index=False)
        
        print(f"  Generated {len(df):,} records")
        print(f"  Saved to: {output_file}")
    
    print(f"\n✓ Sample data generation complete!")
    print(f"  Total files: {len(symbols)}")
    print(f"  Output directory: {output_dir}")

if __name__ == "__main__":
    main()
