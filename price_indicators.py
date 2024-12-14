import sqlite3
import pandas as pd
from tqdm import tqdm

class CryptoIndicatorsProcessor:
    def __init__(self, db_name):
        self.db_name = db_name

    def get_tables_starting_with_daily(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        conn.close()
        return [table[0] for table in tables if table[0].startswith('daily_') and not table[0].startswith('daily_indicators_')]

    def fetch_data_from_table(self, table_name):
        conn = sqlite3.connect(self.db_name)
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, conn)
        conn.close()
        
        # Extract symbol name from the table name
        symbol_name = table_name.split('_')[1]
        df['symbol'] = symbol_name
        
        return df

    @staticmethod
    def calculate_daily_return(df):
        df['daily_return'] = df['close'].pct_change() * 100
        return df

    @staticmethod
    def calculate_weekly_return(df):
        df['weekly_return'] = df['close'].pct_change(periods=7) * 100
        return df

    @staticmethod
    def calculate_monthly_return(df):
        df['monthly_return'] = df['close'].pct_change(periods=30) * 100
        return df

    @staticmethod
    def calculate_quarterly_return(df):
        df['quarterly_return'] = df['close'].pct_change(periods=91) * 100
        return df

    @staticmethod
    def calculate_percent_daily_volume_change(df):
        df['volume_pct_change'] = df['volume'].pct_change() * 100
        return df

    @staticmethod
    def calculate_rolling_volume(df, window=30):
        df['rolling_volume_30'] = df['volume'].rolling(window=window).sum()
        return df

    @staticmethod
    def calculate_percent_change_vs_rolling_volume(df):
        df['percent_change_vs_rolling_volume'] = (df['volume'] / df['rolling_volume_30'] - 1) * 100
        return df

    @staticmethod
    def calculate_sma(df, periods):
        for period in periods:
            df[f'sma_{period}'] = df['close'].rolling(window=period).mean()
        return df

    @staticmethod
    def calculate_ema(df, periods):
        for period in periods:
            df[f'ema_{period}'] = df['close'].ewm(span=period, adjust=False).mean()
        return df

    @staticmethod
    def calculate_rsi(df, periods=[14]):
        for period in periods:
            delta = df['close'].diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)

            avg_gain = gain.rolling(window=period).mean()
            avg_loss = loss.rolling(window=period).mean()

            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            df[f'rsi_{period}'] = rsi
        return df

    def process_table_and_store_indicators(self, table_name):
        conn = sqlite3.connect(self.db_name)

        df = self.fetch_data_from_table(table_name)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Calculate indicators
        sma_periods = [10, 20, 50, 100, 200]
        ema_periods = [10, 20, 50, 100, 200]
        df = self.calculate_sma(df, sma_periods)
        df = self.calculate_ema(df, ema_periods)
        df = self.calculate_rsi(df, periods=[14, 30])
        df = self.calculate_daily_return(df)
        df = self.calculate_weekly_return(df)
        df = self.calculate_monthly_return(df)
        df = self.calculate_quarterly_return(df)
        df = self.calculate_percent_daily_volume_change(df)
        df = self.calculate_rolling_volume(df, 30)
        df = self.calculate_percent_change_vs_rolling_volume(df)

        new_table_name = f"daily_indicators_{table_name.split('_')[1]}"
        df.to_sql(new_table_name, conn, if_exists='replace', index=False)
        conn.close()
        print(f"Indicators stored in table: {new_table_name}")

    def process_all_tables(self):
        daily_tables = self.get_tables_starting_with_daily()
        for table in tqdm(daily_tables, desc="Processing tables", unit="table"):
            self.process_table_and_store_indicators(table)
