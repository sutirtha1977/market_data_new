import os
import requests
from datetime import datetime, timedelta
from helper import (log, clear_log)
from data_manager import (
    get_db_connection,
    close_db_connection
)
from data_manager import retrieve_equity_symbol

EQUITY_FOLDER = "./nse_bhav_copy/equity/"
os.makedirs(EQUITY_FOLDER, exist_ok=True)
from datetime import datetime

TODAY = datetime.today()

# def download_symbol_history_year(symbol, start_date, end_date):
#     """Download data only for given year range; handles NSE cookie/token logic."""

#     from_str = start_date.strftime("%d-%m-%Y")
#     to_str   = end_date.strftime("%d-%m-%Y")

#     filename = f"{symbol}_{from_str}_{to_str}.csv"
#     output_file = os.path.join(EQUITY_FOLDER, filename)

#     url = (
#         f"https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData"
#         f"?from={from_str}&to={to_str}&symbol={symbol}"
#         f"&type=priceVolumeDeliverable&series=ALL&csv=true"
#     )
#     print(url)
    
#     try:
#         # Headers to mimic a browser
#         headers = {
#             "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
#             "Accept-Language": "en-US,en;q=0.9",
#             "Accept": "text/csv,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
#         }

#         # Make the request
#         response = requests.get(url, headers=headers)

#         # Check if request was successful
#         if response.status_code == 200:
#             # Save the file
#             with open(output_file, "wb") as f:
#                 f.write(response.content)
#             print(f"File downloaded successfully: {output_file}")
#         else:
#             print(f"Failed to download file. Status code: {response.status_code}")
            
#         # --- Check the saved file for gibberish/HTML or invalid CSV ---
#         if os.path.exists(output_file):
#             try:
#                 # Open file with utf-8-sig to remove BOM
#                 with open(output_file, "r", encoding="utf-8-sig") as f:
#                     first_line = f.readline().strip()

#                 # Split headers and normalize
#                 headers = []
#                 for h in first_line.split(","):
#                     clean_h = h.strip().strip('"').replace("\xa0", "").lower()
#                     headers.append(clean_h)

#                 # Required columns
#                 required_cols = ["symbol", "series", "date"]

#                 # Check if each required column exists in any header
#                 valid = all(any(col in h for h in headers) for col in required_cols)

#                 if not valid:
#                     print(f"[LOG] Invalid CSV detected: {filename}")
#                     log(f"INVALID CSV FILE: {filename}")
#                 # else:
#                 #     print(f"[LOG] CSV validated: {filename}")

#             except Exception as e:
#                 # Could be binary/gibberish file
#                 print(f"[LOG] Could not read file (likely gibberish): {filename}")
#                 log(f"INVALID CSV FILE: {filename}")
#     except Exception as e:
#         print(f"[EXCEPTION] {filename}: {e}")
#         log(f"EXCEPTION {filename}: {e}")
def download_symbol_history_year(symbol, start_date, end_date, timeout=20):
    """Download data only for given year range; handles NSE cookie/token logic with timeout handling."""
    from urllib.parse import quote_plus
    from_str = start_date.strftime("%d-%m-%Y")
    to_str   = end_date.strftime("%d-%m-%Y")

    filename = f"{symbol}_{from_str}_{to_str}.csv"
    output_file = os.path.join(EQUITY_FOLDER, filename)
    # URL-encode symbol
    encoded_symbol = quote_plus(symbol)  # converts & to %26 etc.
    url = (
        f"https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData"
        f"?from={from_str}&to={to_str}&symbol={encoded_symbol}"
        f"&type=priceVolumeDeliverable&series=ALL&csv=true"
    )
    print(url)

    
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/csv,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }

        # Make the request with timeout
        response = requests.get(url, headers=headers, timeout=timeout)

        if response.status_code == 200:
            # Save the file
            with open(output_file, "wb") as f:
                f.write(response.content)
            print(f"File downloaded successfully: {output_file}")
        else:
            print(f"[ERROR] Failed to download file. Status code: {response.status_code} | URL: {url}")
            log(f"HTTP ERROR {symbol} {from_str} to {to_str} - Status: {response.status_code} | URL: {url}")

        # --- Check the saved file for gibberish/HTML or invalid CSV ---
        if os.path.exists(output_file):
            try:
                with open(output_file, "r", encoding="utf-8-sig") as f:
                    first_line = f.readline().strip()

                headers = [h.strip().strip('"').replace("\xa0", "").lower() for h in first_line.split(",")]
                required_cols = ["symbol", "series", "date"]
                valid = all(any(col in h for h in headers) for col in required_cols)

                if not valid:
                    print(f"[LOG] Invalid CSV detected: {filename}")
                    log(f"INVALID CSV FILE: {filename}")

            except Exception as e:
                print(f"[LOG] Could not read file (likely gibberish): {filename}")
                log(f"INVALID CSV FILE: {filename}")

    except requests.exceptions.Timeout:
        print(f"[TIMEOUT] Request timed out for {symbol} | URL: {url}")
        log(f"TIMEOUT ERROR {symbol} {from_str} to {to_str} | URL: {url}")
    except requests.exceptions.RequestException as e:
        print(f"[REQUEST ERROR] {symbol} | URL: {url} | {e}")
        log(f"REQUEST ERROR {symbol} {from_str} to {to_str} | URL: {url} | {e}")
    except Exception as e:
        print(f"[EXCEPTION] {filename}: {e}")
        log(f"EXCEPTION {filename}: {e}")
        
from dateutil.relativedelta import relativedelta
import pandas as pd
def download_from_csv(csv_file="./nse_bhav_copy/blank.csv", timeout=20):
    """Download symbols using start_dt and end_dt from CSV file."""
    if not os.path.exists(csv_file):
        log(f"CSV file not found: {csv_file}")
        return

    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        log(f"Failed to read CSV {csv_file}: {e}")
        return

    for _, row in df.iterrows():
        try:
            symbol = str(row["symbol"]).strip().upper()
            start_dt = row["start_dt"]
            end_dt = row["end_dt"]

            # Convert CSV string to datetime
            # if isinstance(start_dt, str):
            #     start_dt = datetime.strptime(start_dt, "%d/%m/%y")
            # if isinstance(end_dt, str):
            #     end_dt = datetime.strptime(end_dt, "%d/%m/%y")

            # # Format for NSE URL
            # from_str = start_dt.strftime("%d-%m-%Y")
            # to_str = end_dt.strftime("%d-%m-%Y")

            # # Build filename
            from_str = start_dt
            to_str = end_dt
            filename = f"{symbol}_{from_str}_{to_str}.csv"
            output_file = os.path.join(EQUITY_FOLDER, filename)

            # URL encode symbol
            from urllib.parse import quote_plus
            encoded_symbol = quote_plus(symbol)
            url = (
                f"https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData"
                f"?from={from_str}&to={to_str}&symbol={encoded_symbol}"
                f"&type=priceVolumeDeliverable&series=ALL&csv=true"
            )
            log(output_file)

            # # Download
            # headers = {
            #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            #                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            #     "Accept-Language": "en-US,en;q=0.9",
            #     "Accept": "text/csv,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            # }
            # response = requests.get(url, headers=headers, timeout=timeout)

            # if response.status_code == 200:
            #     with open(output_file, "wb") as f:
            #         f.write(response.content)
            #     log(f"Downloaded: {filename}")
            # else:
            #     log(f"HTTP ERROR {symbol} {from_str} to {to_str} - Status: {response.status_code} | URL: {url}")

            # # Optional: validate CSV headers
            # if os.path.exists(output_file):
            #     try:
            #         with open(output_file, "r", encoding="utf-8-sig") as f:
            #             first_line = f.readline().strip()

            #         headers = [h.strip().strip('"').replace("\xa0", "").lower() for h in first_line.split(",")]
            #         required_cols = ["symbol", "series", "date"]
            #         valid = all(any(col in h for h in headers) for col in required_cols)

            #         if not valid:
            #             log(f"INVALID CSV FILE: {filename}")
            #     except Exception as e:
            #         log(f"INVALID CSV FILE (could not read): {filename}")

        except Exception as e:
            log(f"EXCEPTION processing {symbol}: {e}")

def download_symbol(symbol, listing_date):
    """Break download into yearly chunks starting from listing date."""
    if listing_date is None:
        print(f"[SKIP] {symbol} has no listing date")
        log(f"SKIP {symbol} - no listing date")
        return

    current_start = listing_date

    while current_start <= TODAY:
        # Add 1 year safely
        current_end = current_start + relativedelta(years=1) - timedelta(days=1)
        if current_end > TODAY:
            current_end = TODAY

        download_symbol_history_year(symbol, current_start, current_end)

        # Next year start
        current_start = current_end + timedelta(days=1)


def download_all_symbols():
    clear_log()
    log("=== DOWNLOAD START ===")

    try:
        conn = get_db_connection()
        # Retrieve all symbols from DB
        df = retrieve_equity_symbol("ALL", conn)
        print(df.head())
        print(f"Total symbols: {len(df)}")
    except Exception as e:
        print("DB ERROR:", e)
        log(f"DB ERROR: {e}")
        return

    # Only process these symbols
    # allowed_symbols = ["ARE&M", "GMRP&UI", "GVT&D", "IL&FSENGG", "IL&FSTRANS", "J&KBANK", "M&M", "M&MFIN", "S&SPOWER", "SURANAT&P"]

    print("\n--- Starting Yearly Downloads for selected symbols ---\n")
    log("--- Starting Yearly Downloads for selected symbols ---")

    for _, row in df.iterrows():
        try:
            symbol = row["symbol"].strip().upper()
            # if symbol not in allowed_symbols:
            #     continue  # skip other symbols

            listing_date = row["listing_date"]

            # Convert listing_date type if needed
            if isinstance(listing_date, str):
                listing_date = datetime.strptime(listing_date, "%Y-%m-%d")

            print(f"\n--- {symbol} from {listing_date.date()} ---")
            log(f"PROCESSING {symbol} from {listing_date.date()}")
            download_symbol(symbol, listing_date)

        except Exception as e:
            print(f"[LOOP ERROR] {symbol}: {e}")
            log(f"LOOP ERROR {symbol}: {e}")

    close_db_connection(conn)
    log("=== DOWNLOAD COMPLETE ===")
    print("\n--- Download Complete ---\n")


if __name__ == "__main__":
    clear_log()
    # download_all_symbols()
    download_from_csv()
