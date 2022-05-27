import yfinance as yf
import sqlite3
import time
import os
def download_yf_data_into_sql_df():
    start_time=time.time()
    path_to_db=os.path.join(os.curdir, 'datasets','sql_databases',
                            "yf_db_historical_data_new.db")
    conn=sqlite3.connect( path_to_db )
    cur=conn.cursor()

    cur.execute('drop table if exists yf_gerchik_tickers_and_prices;')
    number_of_stocks=0
    with open('list_of_gerchik_tickers.txt','r') as gerchik_file:
        symbols_list=gerchik_file.read().splitlines()

        #print(symbols_list)
        for symbol in symbols_list:
            symbol = symbol.replace ( "\n" , "" )
            symbol_dataframe=yf.download(symbol,threads= False)
            print(symbol)
            #print(type(symbol_dataframe))
            #print ( symbol_dataframe )
            symbol_dataframe['symbol']=symbol
            symbol_dataframe.reset_index()
            symbol_dataframe.rename(columns={"Adj Close":"Adj_Close"})
            #print ( symbol_dataframe )

            try:
                symbol_dataframe.to_sql('yf_gerchik_tickers_and_prices',
                                        conn,if_exists = 'append')
                number_of_stocks=number_of_stocks+1
                print(f"{number_of_stocks} out of {len(symbols_list)} have been added to db")
            except Exception as e:
                print(e)
    end_time=time.time()
    execution_time=end_time-start_time
    print(execution_time)
download_yf_data_into_sql_df()