import os
import pandas as pd
import traceback
from datetime import datetime
from config.logger import log
from config.paths import SCANNER_FOLDER

#################################################################################################
# Saves a pandas DataFrame as a timestamped CSV in the scanner folder, 
# ensuring the folder exists and logging success or errors
#################################################################################################  
def export_to_csv(df: pd.DataFrame, folder: str, base_name: str) -> str:
    try:
        # Ensure folder exists
        os.makedirs(folder, exist_ok=True)

        # Generate filename with timestamp
        ts = datetime.now().strftime("%d%b%Y")
        filename = f"{base_name}_{ts}.csv"
        filepath = os.path.join(folder, filename)

        # Save CSV
        df.to_csv(filepath, index=False)
        log(f"✔ CSV saved at {filepath}")

        return os.path.abspath(filepath)

    except Exception as e:
        log(f"❌ CSV export failed | {e}")
        traceback.print_exc()
        return ""




# def export_to_csv(df: pd.DataFrame, name: str) -> str:
#     try:
#         os.makedirs(SCANNER_FOLDER, exist_ok=True)
#         ts = datetime.now().strftime("%d%b%Y")
#         filename = f"Scanner_{name}_{ts}.csv"
#         filepath = os.path.join(SCANNER_FOLDER, filename)
#         df.to_csv(filepath, index=False)
#         # log(f"✔ Scanner saved at {filepath}")
#         print(f"✔ Scanner saved at {filepath}")
#         return filepath
#     except Exception as e:
#         log(f"❌ CSV export failed | {e}")
#         traceback.print_exc()
#         return ""