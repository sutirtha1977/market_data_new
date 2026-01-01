import os
import pandas as pd
import traceback
from datetime import datetime
from config.logger import log
from config.paths import SCANNER_FOLDER

def export_to_csv(df: pd.DataFrame, name: str) -> str:
    """Export DataFrame to a timestamped CSV in the scanner folder."""
    try:
        os.makedirs(SCANNER_FOLDER, exist_ok=True)
        ts = datetime.now().strftime("%d%b%Y")
        filename = f"Scanner_{name}_{ts}.csv"
        filepath = os.path.join(SCANNER_FOLDER, filename)
        df.to_csv(filepath, index=False)
        # log(f"✔ Scanner saved at {filepath}")
        print(f"✔ Scanner saved at {filepath}")
        return filepath
    except Exception as e:
        log(f"❌ CSV export failed | {e}")
        traceback.print_exc()
        return ""