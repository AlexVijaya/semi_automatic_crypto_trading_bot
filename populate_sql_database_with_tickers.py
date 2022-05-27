import sqlite3
from get_asset_list import get_asset_list
import os
#import pprint
#import alpaca_trade_api as tradeapi
import alpaca_config

def populate_sql_database():

    conn=sqlite3.connect(alpaca_config.path_to_database)

    cur=conn.cursor()

    #cur.execute('drop table if exists stocks;')
    cur.execute ( 'drop table if exists stock_prices;' )
    cur.execute('''CREATE TABLE if not exists stocks (
                    id INTEGER PRIMARY KEY, 
                    symbol TEXT NOT NULL UNIQUE, 
                    name TEXT NOT NULL,
                    exchange TEXT NOT NULL
                    );''')
    cur.execute('''create table if not exists stock_prices (
                    id integer primary key,
                    stock_id not null,
                    date not null,
                    open not null,
                    high not null,
                    low not null,
                    close not null,
                    volume not null,
                    symbol not null,
                    foreign key (stock_id) references stocks (id)
                    );''')
    from fetch_list_of_stocks_from_database import fetch_list_of_stocks_from_database
    list_of_tickers=fetch_list_of_stocks_from_database(alpaca_config.path_to_database)
    assets=get_asset_list()
    i=0
    for index, asset in enumerate(assets):
        if asset.status=='active' and asset.tradable and asset.symbol not in list_of_tickers:
            print(f'added a new symbol to database {asset.symbol}{asset.name}')
            cur.execute('insert or ignore into stocks (symbol,name,exchange) values (?,?,?)',
                        (asset.symbol,asset.name,asset.exchange))
            i=i+1
            print (asset.symbol," ",i, "\n")
        else:
            print(f'{index} I added nothing to the database for {asset.symbol} is either '
                  f'already in the database not tradable or not active')
    print(f"number of stocks is equal to {i}")
    conn.commit()
    conn.close()

populate_sql_database()