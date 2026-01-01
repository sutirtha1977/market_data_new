from datetime import datetime
from config.paths import LOG_FILE
from pathlib import Path

def log(message: str):
    """
    Append a message to the log file with a timestamp.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    ensure_log_folder()
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def clear_log():
    """
    Clear the log file.
    """
    try:
        ensure_log_folder()
        with open(LOG_FILE, "w") as f:
            f.write("")
        print(f"LOG CLEARED: {LOG_FILE}")
    except Exception as e:
        print(f"FAILED TO CLEAR LOG: {e}")


def ensure_log_folder():
    """
    Ensure the folder for the log file exists.
    """
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)