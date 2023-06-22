from threading import Thread
from dotenv import load_dotenv # pip install python-dotenv
from os import getenv
from sqlalchemy import create_engine, text # pip install sqlalchemy, pip install mysqlclient
from requests import request
import pandas as pd
import numpy as np
from pybit.unified_trading import WebSocket, HTTP # pip install pybit
from time import sleep, time
from decimal import Decimal
from urllib.parse import quote_plus
from datetime import datetime


def connect_to_database(): # Connect to database @planetscale.com
    load_dotenv() # Load .env file
    
    try:
        engine = create_engine(f"mysql+mysqlconnector://{getenv('USER')}:{getenv('PASSWORD')}@{getenv('HOST')}/{getenv('DATABASE')}", pool_pre_ping = True)
        with engine.connect():
            return engine
    except Exception as e:
        send_telegram_notification(f"[!] Error while connecting to MySQL.\n{e}")
        quit()

def send_telegram_notification(message): # Send telegram notification
    token = getenv("TOKEN")
    chat_id = getenv("CHAT_ID")
    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={datetime.now().time()} {quote_plus(message)}"
    request("GET", url = url).json()
    print(message)

class Strategy:
    def __init__(self, symbol, interval, usd, row_limit):
        self.symbol = symbol
        self.interval = interval
        self.usd = usd
        self.row_limit = row_limit
        
        # Setup database connection and table (if not already exists)
        self.engine = connect_to_database()
        with self.engine.connect() as connection:
            connection.execute(text("CREATE TABLE IF NOT EXISTS trades(order_id NVARCHAR(255) NOT NULL, symbol NVARCHAR(255), time_expected NVARCHAR(255), time_executed NVARCHAR(255), price_expected NVARCHAR(255), price_executed NVARCHAR(255), PRIMARY KEY(order_id));")) # Create table that holds all trades
            connection.execute(text(f"DELETE FROM trades WHERE symbol = '{self.symbol}'")) # Delete data of trades table for the current symbol only (reset)
            connection.execute(text("CREATE TABLE IF NOT EXISTS kline(start_time NVARCHAR(255) NOT NULL, symbol NVARCHAR(255), price_change TEXT, PRIMARY KEY(start_time, symbol));")) # Create table that holds all kline data
            connection.execute(text(f"DELETE FROM kline WHERE symbol = '{self.symbol}'")) # Delete data of kline table for the current symbol only (reset)
            connection.commit()

        # Get existing kline data for standard deviation calculation
        self.get_historic_kline()

        # Setup API sessions for websocket and HTTP
        load_dotenv()
        self.ws_session = WebSocket(
            testnet = False
            , channel_type = "linear"
        )        
        self.http_session = HTTP(
            testnet = False
            , api_key = getenv("API_KEY")
            , api_secret = getenv("API_SECRET")
        )

        self.previous_start_time = None
        self.confirm = None
        self.start_time = None
        self.open = None
        self.close = None
        self.time_expected = None
    
    def get_historic_kline(self): # Get historical kline data of symbol
        interval_size = self.interval * 60 * 1000 # Time between two candles
        unix_last = int(int(time()) * 1000 / interval_size) * interval_size
        unix_start = unix_last - self.row_limit * interval_size # Set first unix far in the past
        df = pd.DataFrame(columns = ["start_time", "open", "high", "low", "close", "volume", "turnover"])
        
        while unix_start < unix_last - interval_size:
            if self.interval == 1440: self.interval = "D" # API only accepts "D" and not 1440
            response = request("GET", f"https://api.bybit.com/v5/market/kline?category=linear&symbol={self.symbol}&interval={self.interval}&start={unix_start}").json()
            
            returnCode = response["retCode"]
            if returnCode != 0: # Quit if returnCode is not 0 (OK)
                send_telegram_notification(f"[!] {self.symbol} | {returnCode}: {response['retMsg']}")
                quit()
            
            kline = response["result"]["list"]
            df = pd.concat([df, pd.DataFrame(columns = ["start_time", "open", "high", "low", "close", "volume", "turnover"], data = kline)]) # Append all new data to old data
            unix_start = int(kline[0][0]) + interval_size
        
        df["start_time"] = df["start_time"].apply(np.int64)
        df["symbol"] = self.symbol
        df["price_change"] = (df["close"].apply(Decimal) / df["open"].apply(Decimal) - 1).apply(str)
        df = df[["start_time", "symbol", "price_change"]].sort_values(by = ["start_time"])

        try:
            written_rows = df.to_sql(
                name = "kline"
                , con = self.engine
                , if_exists = "append"
                , index = False
                , chunksize = 1000
            )
        except Exception as e:
            send_telegram_notification(f"[!] {self.symbol}\n{e}")
            quit()

        if written_rows != df.shape[0]:
            send_telegram_notification(f"[!] {self.symbol} | Rows written to SQL do not equal rows in dataframe.")
            quit()
        
        print(f"[{self.symbol}] {written_rows} rows written to database.")

    def get_kline(self): # Get kline data of symbol with a websocket stream
        self.ws_session.kline_stream(
            interval = self.interval
            , symbol = self.symbol
            , callback = self.action
        )
        
        while True:
            sleep(1)
    
    def action(self, message): # Handle data from the websocket stream
        for data in message["data"]:
            self.confirm = data["confirm"]
            self.start_time = int(data["start"])

            if self.confirm or (self.previous_start_time != None and self.previous_start_time < self.start_time):
                self.previous_start_time = self.start_time + self.interval * 60 * 1000
                self.open = Decimal(data["open"])
                self.close = Decimal(data["close"])

                # Close trade if open
                position = self.get_position_info()
                if position["side"] != "None":
                    self.close_position(position)

                # Get standard deviation
                price_change = self.close / self.open - 1
                print(f"[{self.symbol}] {self.start_time}: {round(price_change * 100, 4)}%")

                with self.engine.connect() as connection:
                    connection.execute(text(f"INSERT INTO kline(start_time, symbol, price_change) VALUES ('{int(self.start_time)}', '{str(self.symbol)}', '{str(price_change)}');")) # Add new kline data to database
                    connection.execute(text(f"DELETE FROM kline WHERE symbol = '{self.symbol}' ORDER BY start_time ASC LIMIT 1")) # Delete the oldest row to keep the same size
                    connection.commit()
                    df = pd.read_sql(text(f"SELECT * FROM kline WHERE symbol = '{self.symbol}';"), connection)
                
                standard_deviation = df["price_change"].apply(float).std()

                # Long/short if change > standard deviation/change < standard deviation
                if price_change >= standard_deviation:
                    self.open_position("Buy")
                elif price_change <= -standard_deviation:
                    self.open_position("Sell")
                
                break

    def get_instruments_info(self): # Get instruments info
        return self.http_session.get_instruments_info(
            category = "linear"
            , symbol = self.symbol
        )["result"]["list"][0]

    def get_position_info(self): # Get latest position
        return self.http_session.get_positions(
            category = "linear",
            symbol = self.symbol,
        )["result"]["list"][0]
    
    def get_order_history(self, order_id): # Get latest order
        return self.http_session.get_order_history(
            category = "linear"
            , orderId = order_id
        )["result"]["list"][0]

    def convert_usd_to_qty(self): # Convert usd to qty
        qty_step = float(self.get_instruments_info()["lotSizeFilter"]["qtyStep"])
        return round((self.usd / float(self.close)) / qty_step, 0) * qty_step

    def close_position(self, position): # Close current position
        if position["side"] == "Buy":
            side = "Sell"
        elif position["side"] == "Sell":
            side = "Buy"
        
        qty = position["size"]
        self.time_expected = time() * 1000
        result = self.http_session.place_order(
            category = "linear"
            , symbol = self.symbol
            , side = side
            , orderType = "Market"
            , qty = qty
            , reduceOnly = True
        )

        self.log_data(result)
    
    def open_position(self, side): # Open position
        qty = self.convert_usd_to_qty()
        self.time_expected = time() * 1000
        result = self.http_session.place_order(
            category = "linear"
            , symbol = self.symbol
            , side = side
            , orderType = "Market"
            , qty = qty
        )

        self.log_data(result)

    def log_data(self, result): # Log data to database
        sleep(1)
        order_id = result["result"]["orderId"]
        order = self.get_order_history(order_id)
        print(order_id, self.symbol, int(self.time_expected), int(result['time']), str(self.close), str(order['avgPrice']))

        with self.engine.connect() as connection:
            connection.execute(text(f"INSERT INTO trades(order_id, symbol, time_expected, time_executed, price_expected, price_executed) VALUES ('{order_id}', '{self.symbol}', '{int(self.time_expected)}', '{int(result['time'])}', '{str(self.close)}', '{str(order['avgPrice'])}');"))
            connection.commit()

def main():
    symbols = [
        "BTCUSDT"
        , "SCUSDT"
    ]

    interval = 15
    usd = 1000
    row_limit = 10000 # Row limit for database and standard deviation calculation

    for symbol in symbols:
        Thread(target = Strategy(symbol, interval, usd, row_limit).get_kline).start()

if __name__ == "__main__":
    main()