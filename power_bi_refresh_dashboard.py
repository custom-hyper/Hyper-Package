import sqlite3
import requests
import msal

class PowerBISQLiteHandler:
    def __init__(self, db_path, group_id, dataset_id, access_token):
        """
        Initialize the handler with SQLite database path and Power BI API details.

        :param db_path: Path to the SQLite database file.
        :param group_id: Power BI workspace (group) ID.
        :param dataset_id: Power BI dataset ID.
        :param access_token: Power BI API access token.
        """
        self.db_path = db_path
        self.group_id = group_id
        self.dataset_id = dataset_id
        self.access_token = access_token

    def get_table_column_names(self, table_name):
        """
        Get the column names of a specified table in the SQLite database.

        :param table_name: Name of the table to get column names from.
        :return: List of column names.
        """
        try:
            # Connect to the SQLite database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Get the column names of the table
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [column[1] for column in cursor.fetchall()]

            return columns

        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
            return []
        finally:
            conn.close()

    def refresh_powerbi_dataset(self):
        """
        Trigger a refresh for the Power BI dataset.
        """
        try:
            # Use the access token to refresh the dataset
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            }
            url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/datasets/{self.dataset_id}/refreshes"
            response = requests.post(url, headers=headers)

            # Check for successful response
            if response.status_code == 202:
                print("Dataset refresh initiated successfully")
            else:
                print(f"Failed to refresh dataset: {response.status_code}")
                try:
                    # Attempt to parse JSON if available
                    print(f"Response content: {response.json()}")
                except requests.exceptions.JSONDecodeError:
                    # Handle non-JSON responses gracefully
                    print(f"Non-JSON response: {response.text}")

        except Exception as e:
            print(f"An error occurred: {e}")

# This function is outside the class
def get_access_token(client_id, client_secret, tenant_id):
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://analysis.windows.net/powerbi/api/.default"
    }
    response = requests.post(url, data=data)
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        raise Exception(f"Failed to get access token: {response.json()}")
