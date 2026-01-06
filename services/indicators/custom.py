import pandas as pd
from services.indicators.utils import ema, wma
from services.indicators.momentum import rsi

def ema_rsi(close: pd.Series, rsi_period=9, ema_period=3):
    r = rsi(close, rsi_period)
    return ema(r, ema_period)

def wma_rsi(close: pd.Series, rsi_period=9, wma_period=21):
    r = rsi(close, rsi_period)
    return wma(r, wma_period)