import pandas as pd
from services.indicators.utils import sma

def atr(high, low, close, period=14):
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)

    # Wilder ATR
    return tr.ewm(alpha=1/period, adjust=False).mean()


def bollinger_bands(close, period=20, std_mult=2):
    mid = sma(close, period)
    std = close.rolling(period).std()

    upper = mid + std_mult * std
    lower = mid - std_mult * std

    return upper, mid, lower