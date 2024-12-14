import sqlite3
import pandas as pd

class power_bi:
    def __init__(self, db_name):
        """
        Initialize the power_bi module with the database name.
        
        :param db_name: Name of the SQLite database file.
        """
        self.db_name = db_name

    def _get_tables_starting_with_daily(self):
        """
        Get all table names in the database that start with 'daily_indicators_'.
        
        :return: List of table names.
        """
        try:
            # Connect to the SQLite database
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()

            # Query to get all table names
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            # Filter the tables that start with 'daily_indicators_'
            daily_tables = [table[0] for table in tables if table[0].startswith('daily_indicators_')]

            return daily_tables
        finally:
            conn.close()

    def _fetch_data_from_table(self, table_name):
        """
        Fetch data from a specific table in the database.
        
        :param table_name: Name of the table to fetch data from.
        :return: DataFrame containing the table data.
        """
        conn = sqlite3.connect(self.db_name)
        try:
            query = f"SELECT * FROM {table_name};"
            return pd.read_sql(query, conn)
        finally:
            conn.close()

    def merging_tables_with_symbol(self):
        """
        Merge all tables starting with 'daily_indicators_' and add a 'symbol' column.
        
        :return: DataFrame containing the merged data.
        """
        # Get the list of tables starting with 'daily_indicators_'
        daily_tables = self._get_tables_starting_with_daily()
        merged_df = pd.DataFrame()

        for table in daily_tables:
            print(f"Fetching data from table: {table}")

            # Fetch the data from the table
            df = self._fetch_data_from_table(table)

            # Add a 'symbol' column based on the table name
            symbol = table.split('daily_indicators_')[1]  # Extract symbol from table name
            df['symbol'] = symbol

            # Merge data
            merged_df = pd.concat([merged_df, df], ignore_index=True)

        return merged_df

    def load_to_sqlite(self, merged_df):
        """
        Load the merged DataFrame into a new table in the SQLite database.
        
        :param merged_df: DataFrame to load into the database.
        """
        conn = sqlite3.connect(self.db_name)
        try:
            merged_df.to_sql('power_bi_daily', conn, if_exists='replace', index=False)
            print("Data loaded into 'power_bi_daily' table.")
        finally:
            conn.close()
