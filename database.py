from dotenv import load_dotenv # pip install python-dotenv
from os import getenv
from sqlalchemy import create_engine, text # pip install sqlalchemy, pip install mysqlclient
import pandas as pd

def connect_to_database(): # Connect to database @planetscale.com
    load_dotenv() # Load .env file
    
    return create_engine(f"mysql+mysqlconnector://{getenv('USER')}:{getenv('PASSWORD')}@{getenv('HOST')}/{getenv('DATABASE')}", pool_pre_ping = True)

def get_sql():
    engine = connect_to_database()
    with engine.connect() as connection:
        print(pd.read_sql(text(f"SELECT * FROM trades;"), connection))
        print(pd.read_sql(text(f"SELECT * FROM kline;"), connection))

def main():
    get_sql()

if __name__ == "__main__":
    main()