from db.connection import get_db_connection, close_db_connection
from config.logger import log
import traceback
import os
#################################################################################################
# Remove inconsistent weekly records from price tables.
# Deletes all rows in the weekly timeframe ('1wk') where the date does not fall
# on a Monday, ensuring weekly data aligns with the start of the week.
# If `is_index` is True, cleanup is applied to `index_price_data`;
# otherwise, it is applied to `equity_price_data`.
# This maintains consistent weekly data before further processing.
#################################################################################################
def delete_non_monday_weekly(is_index=False):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # pick table based on flag
        table = "index_price_data" if is_index else "equity_price_data"

        log(f"Deleting non-Monday weekly rows from '{table}'...")

        sql = f"""
            DELETE FROM {table}
            WHERE timeframe = '1wk'
              AND strftime('%w', date) <> '1'
        """

        cur.execute(sql)
        affected = cur.rowcount
        conn.commit()

        log(f"🗑️  Deleted {affected} non-Monday weekly rows from '{table}'")

    except Exception as e:
        log(f"❌ Failed to delete rows from '{table}': {e}")
        traceback.print_exc()

    finally:
        close_db_connection(conn)
#################################################################################################
# Delete files from folder
#################################################################################################  
def delete_files_in_folder(folder_path):
    try:
        if not os.path.exists(folder_path):
            print(f"Folder does not exist: {folder_path}")
            return

        deleted = 0
        for filename in os.listdir(folder_path):
            if filename.lower().endswith(".csv"):   # match .csv or .CSV
                filepath = os.path.join(folder_path, filename)
                if os.path.isfile(filepath):
                    os.remove(filepath)
                    deleted += 1

        print(f"Deleted {deleted} .csv files from: {folder_path}")
    except Exception as e:
        log(f"❌ Failed to delete .csv files from: {folder_path}: {e}")
        traceback.print_exc()