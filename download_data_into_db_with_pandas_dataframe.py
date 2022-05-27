#import yfinance as yf
import pandas_datareader as pdr
import sqlite3
import time
import os
import pandas as pd
def download_yf_data_into_sql_df():
    start_time=time.time()
    path_to_db=os.path.join(os.curdir, 'datasets','sql_databases', "yf_db_historical_data_new.db")
    conn=sqlite3.connect( path_to_db )

    cur=conn.cursor()

    conn1 = sqlite3.connect ( os.path.join ( os.curdir , 'datasets','sql_databases','yf_db_historical_data3.db' ) )
    cur1 = conn1.cursor ()
    dist_tickers_df = pd.read_sql ( '''select dist_tickers from dist_tickers;''' , conn1 )
    dist_tickers_series = dist_tickers_df['dist_tickers']
    dist_tickers_list = list ( dist_tickers_series )


    cur.execute('drop table if exists yf_gerchik_tickers_and_prices;')
    number_of_stocks=0
    with open('list_of_gerchik_tickers.txt','r') as gerchik_file:
        symbols_list=gerchik_file.read().splitlines()

        #print(symbols_list)
        for symbol in dist_tickers_list:
            symbol = symbol.replace ( "\n" , "" )
            #symbol_dataframe=yf.download(symbol,threads= False)
            try:
                symbol_dataframe=pdr.get_data_yahoo (symbol)
                print(symbol_dataframe)

                #print(type(symbol_dataframe))
                #print ( symbol_dataframe )
                symbol_dataframe['symbol']=symbol
                symbol_dataframe.reset_index()
                symbol_dataframe.rename(columns={"Adj Close":"Adj_Close"})
                print ( symbol_dataframe )


                symbol_dataframe.to_sql('yf_gerchik_tickers_and_prices',conn,if_exists = 'append')
                number_of_stocks=number_of_stocks+1
                print(f"{number_of_stocks} out of {len(symbols_list)} have been added to db")
            except Exception as e:
                print(e)

            finally:
                continue
    end_time=time.time()
    execution_time=end_time-start_time
    print("execution_time in seconds:", execution_time)
    print ("execution_time in minutes:", execution_time/60 )
    print ("execution_time in hours:", execution_time/3600 )
download_yf_data_into_sql_df()