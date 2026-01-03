import pandas as pd
import numpy as np
import traceback
import sys
from helper import (
    log
)
#################################################################################################
# Computes the Relative Strength Index (RSI) for a price series using Wilder’s method.
# Steps:
#   - Calculates period-to-period price changes and separates gains from losses.
#   - Applies exponential smoothing to derive average gains and losses over the given period.
#   - Converts the ratio of smoothed gains to losses into an RSI value on a 0–100 scale.
#   - Handles edge cases by setting RSI to 100 when losses are zero (all gains).
# Returns a pandas Series of rounded RSI values, preserving the original index.
#################################################################################################
def calculate_rsi_series(close, period):
    try:
        # Use Wilder's smoothing (adjust=False) for RSI as commonly expected
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        # avoid division by zero
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        # where avg_loss == 0, RSI should be 100 (all gains)
        rsi = rsi.fillna(100)
        return rsi.round(2)
    except Exception as e:
        log(f"{sys._getframe().f_code.co_name} FAILED | {e}")
        traceback.print_exc()
        return pd.Series(index=close.index, dtype=float)
#################################################################################################
# Calculates Bollinger Bands for a series of closing prices.
# Steps:
#   - Computes a rolling moving average (middle band) over the specified period.
#   - Calculates the rolling standard deviation for the same window.
#   - Derives upper and lower bands by adding/subtracting a chosen multiple of
#     standard deviation from the middle band (default: ±2 SD).
# Returns three rounded Series — upper, middle, and lower bands — aligned to
# the original index. Falls back to empty Series triples if an error occurs.
#################################################################################################
def calculate_bollinger(close, period=20, std_mult=2):
    try:
        mid = close.rolling(period).mean()
        std = close.rolling(period).std()
        upper = mid + std_mult * std
        lower = mid - std_mult * std
        return upper.round(2), mid.round(2), lower.round(2)
    except Exception as e:
        log(f"{sys._getframe().f_code.co_name} FAILED | {e}")
        traceback.print_exc()
        return (pd.Series(index=close.index, dtype=float),) * 3
#################################################################################################
# Computes the Average True Range (ATR) over a given period to measure market volatility.
# Steps:
#   - Calculates True Range (TR) per row as the maximum of:
#       * current high − current low
#       * absolute difference between current high and previous close
#       * absolute difference between current low and previous close
#   - Applies Wilder’s smoothing (EMA with adjust=False) to TR values to obtain ATR.
# Returns a rounded ATR Series aligned with the input DataFrame index, or an empty
# Series on failure.
#################################################################################################
def calculate_atr(df, period=14):
    try:
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        # Use Wilder's smoothing (EMA with adjust=False) for ATR
        atr = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
        return atr.round(2)
    except Exception as e:
        log(f"{sys._getframe().f_code.co_name} FAILED | {e}")
        traceback.print_exc()
        return pd.Series(index=df.index, dtype=float)
#################################################################################################
# Calculates the Moving Average Convergence Divergence (MACD) indicator from a
# closing-price Series to identify trend momentum and potential reversals.
# Steps:
#   - Computes the 12-period and 26-period EMAs of the closing prices.
#   - MACD line = EMA(12) − EMA(26)
#   - Signal line = 9-period EMA of the MACD line
# Returns two rounded Series: the MACD line and the Signal line, or empty Series
# if an error occurs.
#################################################################################################
def calculate_macd(close):
    try:
        ema_12 = close.ewm(span=12, adjust=False).mean()
        ema_26 = close.ewm(span=26, adjust=False).mean()
        macd = ema_12 - ema_26
        signal = macd.ewm(span=9, adjust=False).mean()
        return macd.round(2), signal.round(2)
    except Exception as e:
        log(f"{sys._getframe().f_code.co_name} FAILED | {e}")
        traceback.print_exc()
        return (
            pd.Series(index=close.index, dtype=float),
            pd.Series(index=close.index, dtype=float),
        )
#################################################################################################
# Computes the Supertrend indicator to identify market trend direction and
# dynamic support/resistance levels.
# Steps:
#   - Calculates ATR-based upper and lower bands using the mid-price of each bar
#     and an ATR multiplier.
#   - Smooths band values across periods to prevent abrupt shifts.
#   - Determines the active trend: if price is above the previous supertrend,
#     trend flips to up and uses the lower band; otherwise trend is down and uses
#     the upper band.
# Returns:
#   - supertrend: the active trailing band used as dynamic support/resistance
#   - direction: +1 for uptrend, -1 for downtrend
# Both Series are rounded to 2 decimals; empty Series returned on failure.
#################################################################################################
def calculate_supertrend(df, atr_period=10, multiplier=3):
    try:
        atr = calculate_atr(df, atr_period)
        hl2 = (df["high"] + df["low"]) / 2

        basic_ub = hl2 + multiplier * atr
        basic_lb = hl2 - multiplier * atr

        final_ub = basic_ub.copy()
        final_lb = basic_lb.copy()

        # ---- ADJUST BANDS (correct as-is) ----
        for i in range(1, len(df)):
            if basic_ub.iloc[i] < final_ub.iloc[i - 1] or df["close"].iloc[i - 1] > final_ub.iloc[i - 1]:
                final_ub.iloc[i] = basic_ub.iloc[i]
            else:
                final_ub.iloc[i] = final_ub.iloc[i - 1]

            if basic_lb.iloc[i] > final_lb.iloc[i - 1] or df["close"].iloc[i - 1] < final_lb.iloc[i - 1]:
                final_lb.iloc[i] = basic_lb.iloc[i]
            else:
                final_lb.iloc[i] = final_lb.iloc[i - 1]


        # ---- CORRECT SUPER TREND SELECTION ----
        supertrend = pd.Series(index=df.index, dtype=float)
        direction = pd.Series(index=df.index, dtype=int)

        # initialize
        supertrend.iloc[0] = final_ub.iloc[0]
        direction.iloc[0] = -1   # initial trend is down

        for i in range(1, len(df)):
            prev_st = supertrend.iloc[i - 1]

            # Determine direction based on prev supertrend
            if df["close"].iloc[i] > prev_st:
                direction.iloc[i] = 1      # uptrend
                supertrend.iloc[i] = final_lb.iloc[i]
            else:
                direction.iloc[i] = -1     # downtrend
                supertrend.iloc[i] = final_ub.iloc[i]

        return supertrend.round(2), direction

    except Exception as e:
        log(f"{sys._getframe().f_code.co_name} FAILED | {e}")
        traceback.print_exc()
        return (
            pd.Series(index=df.index, dtype=float),
            pd.Series(index=df.index, dtype=int),
        )
#################################################################################################
# Calculates an Exponential Moving Average (EMA) for the given price series using
# a specified period (Wilder-style with adjust=False); on error, logs the issue
# and returns an empty float Series with matching index.
#################################################################################################
def calculate_ema(series, period):
    try:
        # Keep as-is (Wilder-style EMA behavior with adjust=False)
        return series.ewm(span=period, adjust=False).mean().round(2)
    except Exception as e:
        log(f"{sys._getframe().f_code.co_name} FAILED | {e}")
        traceback.print_exc()
        return pd.Series(index=series.index, dtype=float)
#################################################################################################
# Computes a Weighted Moving Average (WMA) using linear weights over the given period; 
# returns the rounded result or an empty float Series if calculation fails.
#################################################################################################
def calculate_wma(series, period):
    try:
        weights = np.arange(1, period + 1)
        wma = series.rolling(period).apply(lambda x: np.dot(x, weights)/weights.sum(), raw=True)
        return wma.round(2)
    except Exception as e:
        log(f"{sys._getframe().f_code.co_name} FAILED | {e}")
        traceback.print_exc()
        return pd.Series(index=series.index, dtype=float)
#################################################################################################

#################################################################################################