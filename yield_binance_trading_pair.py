import os

import sqlite3
import pandas as pd

def yield_binance_trading_pair():
    conn=sqlite3.connect(os.path.join(os.curdir,"datasets","sql_databases","binance_trading_pairs.db"))
    cur=conn.cursor()
    dist_tickers_df=pd.read_sql('''select trading_pair from trading_pairs_tickers where trading_pair like "%usdt";''', conn)
    dist_tickers_series=dist_tickers_df['trading_pair']
    dist_tickers_list=list ( dist_tickers_series )
    for binance_trading_pair in dist_tickers_list:

        binance_trading_pair=binance_trading_pair.replace("\n","")
        #print(binance_trading_pair)
        yield binance_trading_pair
        continue
# s=next(yield_binance_trading_pair())
# print(s)
# s=next(yield_binance_trading_pair())
# print(s)
#
# symbols=[ticker for ticker in yield_binance_trading_pair()]
# print(symbols)

