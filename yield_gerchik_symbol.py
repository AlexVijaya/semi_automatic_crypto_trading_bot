import os
import time
import sqlite3
import pandas as pd
from plot_levels import plot_levels
def yield_gerchik_symbol():
    conn=sqlite3.connect( os.path.join( os.curdir , 'datasets','sql_databases','yf_db_historical_data3.db' ) )
    cur=conn.cursor()
    dist_tickers_df=pd.read_sql('''select dist_tickers from dist_tickers;''', conn)
    dist_tickers_series=dist_tickers_df['dist_tickers']
    dist_tickers_list=list ( dist_tickers_series )
    with open(os.path.join(os.curdir,"list_of_gerchik_tickers.txt"),"r") as gerchik_tickers:
        gerchik_symbols=gerchik_tickers.readlines()
        #print ( gerchik_symbols )
        #clear_list_of_gerchik_symbols=[]

        for gerchik_symbol in dist_tickers_list:

            gerchik_symbol=gerchik_symbol.replace("\n","")
            #print(gerchik_symbol)
            yield gerchik_symbol
            continue

#yield_gerchik_symbol()

