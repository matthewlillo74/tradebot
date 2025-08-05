import time
import random
import alpaca_trade_api as tradeapi
import pandas as pd
import datetime as dt
import ta
import os

# ========================= CONFIG ==========================
API_KEY = "PKDUZ7BSSSP5XD2JDIR6"
API_SECRET = "fLcymFp1kfeP6RkoW7KF9JGDn0h15C2C0saVz5Pi"
BASE_URL = "https://paper-api.alpaca.markets"

MAX_POSITION_PER_TRADE = 200
COOLDOWN_MINUTES = 10

MIN_INTRADAY_GAIN = 2.5
MIN_5MIN_GAIN = 3.0
MIN_DAILY_VOLUME = 500_000

SAMPLE_UNIVERSE = 700  # total to sample from the full universe
SCAN_TOP_N = 75         # top N by daily % gain to scan deeply

# ========================= INIT ============================
api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL)
cooldowns = {}

# ========================= UTILS ===========================
def get_bars_df(ticker, timeframe, limit=100):
    try:
        bars = api.get_bars(ticker, timeframe, limit=limit, adjustment="raw", feed="iex")
        df = bars.df
        if df.empty:
            return None
        df.columns = [col.capitalize() for col in df.columns]  # Normalize columns
        df = df.rename(columns={"T": "Timestamp"})  # Optional if timestamp used
        return df
    except Exception as e:
        print(f"[{ticker}] Error fetching bars: {e}")
        return None


def get_daily_gain_and_volume(ticker):
    try:
        bars = api.get_bars(ticker, tradeapi.TimeFrame.Day, limit=2, adjustment="raw", feed="iex")
        df = bars.df
        if df is None or len(df) < 2:
            return None, None
        open_price = df["open"].iloc[-1]
        close_price = df["close"].iloc[-1]
        volume = df["volume"].iloc[-1]
        daily_gain = ((close_price - open_price) / open_price) * 100
        return daily_gain, volume
    except:
        return None, None

def get_5min_gain(ticker):
    df = get_bars_df(ticker, tradeapi.TimeFrame.Minute, limit=6)
    if df is None or len(df) < 6:
        return None
    price_5min_ago = df["Close"].iloc[0]
    current_price = df["Close"].iloc[-1]
    return ((current_price - price_5min_ago) / price_5min_ago) * 100

def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def price_above_vwap(df):
    typical_price = (df["High"] + df["Low"] + df["Close"]) / 3
    vwap = (typical_price * df["Volume"]).cumsum() / df["Volume"].cumsum()
    return df["Close"].iloc[-1] > vwap.iloc[-1]

def has_volume_spike(df, threshold=3):
    avg_vol = df["Volume"].rolling(window=10).mean().iloc[-1]
    return df["Volume"].iloc[-1] > threshold * avg_vol

def should_buy(ticker):
    if ticker in cooldowns:
        elapsed = (dt.datetime.now() - cooldowns[ticker]).total_seconds()
        if elapsed < COOLDOWN_MINUTES * 60:
            return False, "Cooldown"

    df = get_bars_df(ticker, tradeapi.TimeFrame.Minute, limit=60)
    if df is None or df.empty:
        return False, "No data"

    # VWAP Calculation
    typical_price = (df["High"] + df["Low"] + df["Close"]) / 3
    vwap = (typical_price * df["Volume"]).cumsum() / df["Volume"].cumsum()
    if df["Close"].iloc[-1] < vwap.iloc[-1]:
        return False, "Price below VWAP"

    # RSI
    df["RSI"] = ta.momentum.RSIIndicator(df["Close"], window=14).rsi()
    latest_rsi = df["RSI"].iloc[-1]
    if latest_rsi > 75:
        return False, f"RSI too high: {latest_rsi:.2f}"

    # 10-bar high breakout
    recent_high = df["High"].iloc[-11:-1].max()
    if df["Close"].iloc[-1] < recent_high:
        return False, f"Not breaking 10-bar high: {df['Close'].iloc[-1]:.2f} < {recent_high:.2f}"

    # 5-min gain
    if len(df) < 6:
        return False, "Insufficient data for 5-min gain"
    price_5min_ago = df["Close"].iloc[-6]
    current_price = df["Close"].iloc[-1]
    gain_5min = ((current_price - price_5min_ago) / price_5min_ago) * 100
    if gain_5min < MIN_5MIN_GAIN:
        return False, f"5-min gain too low: {gain_5min:.2f}%"

    return True, f"Buy conditions met (5min gain: {gain_5min:.2f}%, RSI: {latest_rsi:.2f})"


def place_order(ticker):
    df = get_bars_df(ticker, tradeapi.TimeFrame.Minute, limit=1)
    if df is None or df.empty:
        return
    price = df["Close"].iloc[-1]
    qty = int(MAX_POSITION_PER_TRADE / price)
    if qty <= 0:
        return
    try:
        api.submit_order(symbol=ticker, qty=qty, side="buy", type="market", time_in_force="gtc")
        print(f"[BUY] {ticker} qty={qty} at ${price:.2f}")
        cooldowns[ticker] = dt.datetime.now()
    except Exception as e:
        print(f"[{ticker}] Order error: {e}")

# ========================= CORE ============================
import itertools

# GLOBALS for chunking
all_symbols = []
chunk_size = 150
current_chunk_index = 0

def fetch_bars_with_backoff(symbol, timeframe, limit):
    retries = 5
    wait_time = 1
    for i in range(retries):
        try:
            bars = api.get_bars(symbol, timeframe, limit=limit, feed='iex')
            return bars
        except tradeapi.rest.APIError as e:
            if e.status_code == 429:  # Rate limit
                print(f"Rate limit hit for {symbol}, backing off {wait_time} sec...")
                time.sleep(wait_time)
                wait_time *= 2  # Exponential backoff
            else:
                print(f"Error fetching {symbol}: {e}")
                return None
    print(f"Failed to fetch bars for {symbol} after {retries} retries.")
    return None

def check_positions_to_sell():
    try:
        positions = api.list_positions()
    except Exception as e:
        print(f"Error fetching positions: {e}")
        return

    for position in positions:
        decision = should_sell(position)
        if decision == "partial":
            qty = int(float(position.qty)) // 2
            if qty > 0:
                try:
                    api.submit_order(
                        symbol=position.symbol,
                        qty=qty,
                        side="sell",
                        type="market",
                        time_in_force="gtc"
                    )
                    print(f"[SELL PARTIAL] {position.symbol} qty {qty}")
                except Exception as e:
                    print(f"[{position.symbol}] Partial sell error: {e}")
        elif decision == "full":
            qty = int(float(position.qty))
            try:
                api.submit_order(
                    symbol=position.symbol,
                    qty=qty,
                    side="sell",
                    type="market",
                    time_in_force="gtc"
                )
                print(f"[SELL FULL] {position.symbol} qty {qty}")
            except Exception as e:
                print(f"[{position.symbol}] Full sell error: {e}")
def should_sell(position):
    symbol = position.symbol
    df = get_bars_df(symbol, tradeapi.TimeFrame.Minute, limit=60)
    if df is None or df.empty:
        return None

    entry_price = float(position.avg_entry_price)
    current_price = df["Close"].iloc[-1]
    gain_pct = (current_price - entry_price) / entry_price

    trailing_max = df["Close"].max()
    drop_pct = (trailing_max - current_price) / trailing_max

    # RSI
    df["RSI"] = ta.momentum.RSIIndicator(df["Close"], window=14).rsi()
    latest_rsi = df["RSI"].iloc[-1] if len(df["RSI"]) > 0 else 50

    # Partial sell
    if gain_pct >= 0.05 and int(float(position.qty)) > 1 and latest_rsi >= 70:
        return "partial"

    # Full sell triggers
    if drop_pct > 0.04:
        return "full"
    if gain_pct < -0.035:
        return "full"

    return None

def find_and_trade_movers():
    global all_symbols, current_chunk_index

    if not all_symbols:
        try:
            assets = api.list_assets(status="active")
            all_symbols = [a.symbol for a in assets if a.tradable and a.exchange in ["NYSE", "NASDAQ"]]
            print(f"Total filtered universe size: {len(all_symbols)}")
        except Exception as e:
            print(f"❌ Error fetching symbols: {e}")
            return

    # Get current chunk of symbols
    start = current_chunk_index * chunk_size
    end = start + chunk_size
    symbols_chunk = all_symbols[start:end]

    if not symbols_chunk:
        current_chunk_index = 0
        symbols_chunk = all_symbols[:chunk_size]

    movers = []
    print(f"🔍 Scanning chunk {current_chunk_index + 1} with {len(symbols_chunk)} symbols...")

    for symbol in symbols_chunk:
        try:
            bars_day = fetch_bars_with_backoff(symbol, tradeapi.TimeFrame.Day, limit=1)
            if bars_day is None or bars_day.df.empty or "open" not in bars_day.df.columns or "close" not in bars_day.df.columns:
                continue

            df_day = bars_day.df
            open_price = df_day["open"].iloc[0]
            close_price = df_day["close"].iloc[0]
            intraday_pct = ((close_price - open_price) / open_price) * 100
            if intraday_pct < MIN_INTRADAY_GAIN:
                continue

            bars_1m = fetch_bars_with_backoff(symbol, tradeapi.TimeFrame.Minute, limit=6)
            if bars_1m is None or bars_1m.df.empty or "open" not in bars_1m.df.columns or "close" not in bars_1m.df.columns:
                continue

            df_1m = bars_1m.df
            open_5min = df_1m["open"].iloc[0]
            close_now = df_1m["close"].iloc[-1]
            pct_5min = ((close_now - open_5min) / open_5min) * 100

            if pct_5min >= MIN_5MIN_GAIN:
                movers.append((symbol, intraday_pct, pct_5min))

        except Exception as e:
            print(f"[{symbol}] Error during scan: {e}")
            continue

        time.sleep(0.5)  # rate limit management

    movers.sort(key=lambda x: x[2], reverse=True)
    top_movers = movers[:5]

    print(f"📈 Found {len(top_movers)} movers in chunk {current_chunk_index + 1}")

    for symbol, day_gain, five_min_gain in top_movers:
        try:
            ok, reason = should_buy(symbol)
            if ok:
                print(f"✅ Buy Signal: {symbol} ({reason})")
                place_order(symbol)
            else:
                print(f"⏩ {symbol}: {reason}")
        except Exception as e:
            print(f"[{symbol}] Error during buy logic: {e}")

    current_chunk_index += 1
    if current_chunk_index * chunk_size >= len(all_symbols):
        current_chunk_index = 0



# ========================= MAIN ============================
if __name__ == "__main__":
    while True:
        try:
            find_and_trade_movers()
            check_positions_to_sell()
        except Exception as e:
            print(f"Cycle error: {e}")
        print("Waiting 1 minutes before next chunk scan...")
        time.sleep(60)  # 1 minutes

