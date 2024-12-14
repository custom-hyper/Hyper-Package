import ccxt
import pandas as pd
import sqlite3
from datetime import datetime
import logging

logging.basicConfig(filename='crypto_data.log', level=logging.ERROR)


def get_binance_spot_markets():
    # Initialize Binance exchange
    exchange = ccxt.binance()

    # Fetch all markets
    markets = exchange.load_markets()

    # Filter spot markets (they don't have "PERP" in the symbol)
    spot_markets = [symbol for symbol in markets if '/USDT' in symbol and ':USDT' not in symbol]# Spot pairs usually have '/' (e.g., BTC/USDT)

    return spot_markets#[0:10]
        

def fetch_binance_and_store_data(tokens, timeframe='1d', limit=1000, db_name='crypto_data.db'):
    exchange = ccxt.binance()
    for symbol in tokens:
        table_name = symbol.replace("/", "_").replace(":USDT", "").lower()
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Create table if it does not exist
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS daily_{table_name} (
                timestamp TEXT PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL
            )
        ''')

        # Get the last timestamp in the database
        cursor.execute(f"SELECT MAX(timestamp) FROM daily_{table_name}")
        last_timestamp = cursor.fetchone()[0]

        since = exchange.parse8601('2010-07-17T00:00:00Z') if not last_timestamp else int(pd.Timestamp(last_timestamp).timestamp() * 1000) + 1
        all_data = []

        while True:
            try:
                candles = exchange.fetch_ohlcv(symbol, timeframe, since, limit)
            except Exception as e:
                print(f"Error fetching data for {symbol}: {e}")
                logging.error(f"Error fetching Binance spot markets: {e}")
                break

            if not candles:
                break

            # Filter out overlapping data
            candles = [candle for candle in candles if candle[0] > since]

            if not candles:
                break  # Stop if all fetched candles overlap

            all_data.extend(candles)
            since = candles[-1][0] + 1  # Update for the next fetch

            if len(candles) < limit:
                break  # End if fewer candles than the limit are fetched

        # Convert to DataFrame
        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        dataframe = pd.DataFrame(all_data, columns=columns)
        dataframe['timestamp'] = pd.to_datetime(dataframe['timestamp'], unit='ms').dt.strftime('%Y-%m-%d %H:%M:%S')

        # Remove rows already in the database
        existing_timestamps = pd.read_sql(f"SELECT timestamp FROM daily_{table_name}", conn)
        dataframe = dataframe[~dataframe['timestamp'].isin(existing_timestamps['timestamp'])]

        # Drop duplicates within fetched data
        dataframe.drop_duplicates(subset=['timestamp'], inplace=True)

        # Insert data into the database
        for _, row in dataframe.iterrows():
            cursor.execute(f'''
                INSERT OR REPLACE INTO daily_{table_name} (timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (row['timestamp'], row['open'], row['high'], row['low'], row['close'], row['volume']))

        conn.commit()
        conn.close()

        print(f"Data for {symbol} saved to SQLite. Total rows inserted: {len(dataframe)}")
