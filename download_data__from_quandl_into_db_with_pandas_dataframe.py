
import pandas_datareader as pdr
import sqlite3
import time
import os
import pandas as pd
import quandl_config
def download_quandl_data_into_sql_df():
    start_time=time.time()
    conn=sqlite3.connect(os.path.join ( os.curdir , 'datasets','sql_databases',"quandl_tickers_companies_prices.db"))

    cur=conn.cursor()

    conn1 = sqlite3.connect ( os.path.join ( os.curdir , 'datasets','sql_databases','yf_db_historical_data3.db' ) )
    cur1 = conn1.cursor ()
    gerchik_tickers_df = pd.read_sql ( '''select gerchik_tickers from gerchik_symbols;''' , conn1 )
    gerchik_tickers_series = gerchik_tickers_df['gerchik_tickers']
    gerchik_tickers_list = list ( gerchik_tickers_series )


    cur.execute('drop table if exists quandl_tickers;')
    number_of_stocks=0
    with open('list_of_gerchik_tickers.txt','r') as gerchik_file:
        symbols_list=gerchik_file.read().splitlines()

        #print(symbols_list)
        for symbol in symbols_list:
            symbol = symbol.replace ( "\n" , "" )
            #symbol_dataframe=yf.download(symbol,threads= False)
            try:
                symbol_dataframe=pdr.get_data_quandl (symbol+".US",api_key=quandl_config.API)
                print(symbol_dataframe)

                #print(type(symbol_dataframe))
                #print ( symbol_dataframe )
                symbol_dataframe['symbol']=symbol
                symbol_dataframe.reset_index()
                #symbol_dataframe.rename(columns={"Adj Close":"Adj_Close"})
                print ( symbol_dataframe )


                symbol_dataframe.to_sql('quandl_tickers',conn,if_exists = 'append')
                ##########################

                symbol_dataframe = pdr.get_data_quandl ( "XNAS/"+symbol , api_key = quandl_config.API )
                print ( symbol_dataframe )

                # print(type(symbol_dataframe))
                # print ( symbol_dataframe )
                symbol_dataframe['symbol'] = symbol
                symbol_dataframe.reset_index ()
                # symbol_dataframe.rename(columns={"Adj Close":"Adj_Close"})
                print ( symbol_dataframe )

                symbol_dataframe.to_sql ( 'quandl_tickers' , conn , if_exists = 'append' )

                ########################
                symbol_dataframe = pdr.get_data_quandl ("XNYS/"+ symbol, api_key = quandl_config.API )
                print ( symbol_dataframe )

                # print(type(symbol_dataframe))
                # print ( symbol_dataframe )
                symbol_dataframe['symbol'] = symbol
                symbol_dataframe.reset_index ()
                # symbol_dataframe.rename(columns={"Adj Close":"Adj_Close"})
                print ( symbol_dataframe )

                symbol_dataframe.to_sql ( 'quandl_tickers' , conn , if_exists = 'append' )


                number_of_stocks=number_of_stocks+1
                print(f"{number_of_stocks} out of {len(symbols_list)} have been added to db")
                print("----------------------------------------------------")
            except Exception as e:
                print("+++++++++++++++++++++++++++++++++++++++++++++=")
                print(e)

            finally:
                continue
    end_time=time.time()
    execution_time=end_time-start_time
    print("execution_time in seconds:", execution_time)
    print ("execution_time in minutes:", execution_time/60 )
    print ("execution_time in hours:", execution_time/3600 )
download_quandl_data_into_sql_df()