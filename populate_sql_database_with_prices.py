import sqlite3
import alpaca_trade_api as tradeapi
import alpaca_config
from datetime import datetime
import pandas as pd
import pprint
import time

def populate_sql_database_with_prices():
    start_time=pd.Timestamp ( year = 2021 , month = 5 , day = 1, tz='America/New_York').isoformat()
    end_time = pd.Timestamp ( year = 2021 , month = 10 , day = 21, tz='America/New_York').isoformat ()
    connection=sqlite3.connect(alpaca_config.path_to_database)
    connection.row_factory = sqlite3.Row

    cur=connection.cursor()
    cur.execute('select id,symbol,name from stocks')
    rows_in_stocks_table=cur.fetchall()
    symbols=[]
    stock_dict={}
    for row in rows_in_stocks_table:
        symbol=row['symbol']
        symbols.append(symbol)
        stock_dict[symbol]=row['id']
    pprint.pprint (stock_dict)
    api = tradeapi.REST ( alpaca_config.api_key , alpaca_config.secret_key,
                          alpaca_config.api_url)
    chunk_size=200
    for i in range(0,len(symbols),chunk_size):
        symbol_chunk=symbols[i:i+chunk_size]

        barsets=api.get_barset(symbol_chunk,'day',start=None, end = None, limit=1000)
        print(i)
        for symbol in barsets:

            #print(f"processing {symbol} whose number is {stock_dict[symbol]}")
            for bar in barsets[symbol]:
                #print(symbol, bar)
                #time.sleep(30)
                stock_id=stock_dict[symbol]
                cur.execute('''insert into stock_prices (stock_id, date, open,
                high, low,close,volume, symbol) values (?,?,?,?,?,?,?,?)''',
                            (stock_id,bar.t.date(),bar.o, bar.h, bar.l, bar.c, bar.v, symbol))
                #print(bar.t, bar.o, bar.h, bar.l, bar.c, bar.v)
            #print ( len ( barsets[symbol] ) )
    connection.commit()
populate_sql_database_with_prices ()