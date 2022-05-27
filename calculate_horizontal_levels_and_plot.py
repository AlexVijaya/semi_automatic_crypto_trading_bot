import os
import time
import sqlite3
import pandas as pd
from plot_levels import plot_levels
def calculate_horizontal_levels_and_plot():
    conn=sqlite3.connect( os.path.join( os.curdir ,
                                        'datasets',
                                        'sql_databases',
                                        'yf_db_historical_data3.db' ) )
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
            print(gerchik_symbol)
        #clear_list_of_gerchik_symbols.append(gerchik_symbol)
    ##clear_list_of_gerchik_symbols_series = pd.Series ( clear_list_of_gerchik_symbols, name='gerchik_tickers' )
    #print(list(dist_tickers_series))
            #time.sleep(30)
    #clear_list_of_gerchik_symbols_series.to_sql('gerchik_symbols',con = conn, if_exists = 'append')
    #time.sleep ( 20 )

            try:
                print ( "gerchik_symbol=" , gerchik_symbol )
                plot_levels(gerchik_symbol)
            except Exception as e:
                print(f'something is wrong with {gerchik_symbol}')
                print ( e )
            #time.sleep ( 20 )
calculate_horizontal_levels_and_plot()

