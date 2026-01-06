import pandas as pd
import numpy as np
from services.indicators.utils import ema

def macd(close: pd.Series, fast=12, slow=26, signal=9):
    macd_line = ema(close, fast) - ema(close, slow)
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line

    macd_line.name = "macd"
    signal_line.name = "macd_signal"
    hist.name = "macd_hist"

    return macd_line, signal_line, hist


def supertrend(high, low, close, period=10, multiplier=3):
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)

    atr = tr.ewm(alpha=1/period, adjust=False).mean()
    hl2 = (high + low) / 2

    upperband = hl2 + multiplier * atr
    lowerband = hl2 - multiplier * atr

    final_upper = upperband.copy()
    final_lower = lowerband.copy()

    for i in range(1, len(close)):
        final_upper.iloc[i] = (
            upperband.iloc[i]
            if close.iloc[i-1] <= final_upper.iloc[i-1]
            else min(upperband.iloc[i], final_upper.iloc[i-1])
        )
        final_lower.iloc[i] = (
            lowerband.iloc[i]
            if close.iloc[i-1] >= final_lower.iloc[i-1]
            else max(lowerband.iloc[i], final_lower.iloc[i-1])
        )

    trend = pd.Series(index=close.index, dtype=int)
    st = pd.Series(index=close.index)

    trend.iloc[0] = 1
    st.iloc[0] = final_lower.iloc[0]

    for i in range(1, len(close)):
        if close.iloc[i] > final_upper.iloc[i-1]:
            trend.iloc[i] = 1
        elif close.iloc[i] < final_lower.iloc[i-1]:
            trend.iloc[i] = -1
        else:
            trend.iloc[i] = trend.iloc[i-1]

        st.iloc[i] = final_lower.iloc[i] if trend.iloc[i] == 1 else final_upper.iloc[i]

    st.name = "supertrend"
    trend.name = "supertrend_dir"

    return st, trend