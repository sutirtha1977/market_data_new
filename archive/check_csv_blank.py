import os
import pandas as pd
from helper import log, clear_log

EQUITY_FOLDER = "./nse_bhav_copy/equity/"

def check_and_delete_blank_csv(folder=EQUITY_FOLDER):
    """Check all CSV files in the folder and delete if they have no data."""
    if not os.path.exists(folder):
        log(f"Folder does not exist: {folder}")
        return

    for file in os.listdir(folder):
        if file.endswith(".csv"):
            file_path = os.path.join(folder, file)

            try:
                df = pd.read_csv(file_path)

                # delete file if no data rows OR only headers
                if df.empty or len(df.index) == 0:
                    os.remove(file_path)
                    log(f"[DELETED] Blank CSV removed: {file}")
                # else:
                #     log(f"[OK] {file} contains {len(df)} rows")

            except Exception as e:
                log(f"[ERROR] Could not read {file}: {e}")

if __name__ == "__main__":
    clear_log()
    check_and_delete_blank_csv()