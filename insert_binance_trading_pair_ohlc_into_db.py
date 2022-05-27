from binance_config import api_secret
from binance_config import api_key
from binance.client import Client
import urllib3
import requests
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import os
import pprint
import time
from pathlib import Path
import sqlite3
from drop_table_from_database import drop_table_from_database
def insert_binance_trading_pair_ohlc_into_db(interval = '1d',connection_to_prices=sqlite3.connect(os.path.join(os.getcwd(),
                                                          "datasets",
                                                          "sql_databases",
                                                          "binance_historical_data.db")),
                                            path_to_database_table_in_which_to_be_dropped=os.path.join(os.getcwd(),"datasets",
                                            "sql_databases","binance_historical_data.db"),
                                            start_moment="1 Jan,2017"):
    start_time=time.time()
    client = Client (  api_key , api_secret, {"timeout": 20} )

    # path_to_databases = os.path.join ( os.getcwd () , "datasets" , "sql_databases" )
    # Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )

    # path_to_database_table_in_which_to_be_dropped=os.path.join(os.getcwd(),"datasets",
    #                                             "sql_databases","binance_historical_data.db")

    connection_to_tickers = sqlite3.connect ( os.path.join ( os.getcwd () ,
                                                             "datasets" ,
                                                             "sql_databases" ,
                                                             "binance_trading_pairs.db" ) )
    #uncomment if  you want to select all trading pairs from binance
    # sql_query = pd.read_sql_query ( '''select trading_pair from trading_pairs_tickers''' ,
    #                                 connection_to_tickers )


    sql_query = pd.read_sql_query ( '''select trading_pair from trading_pairs_tickers 
                                        where trading_pair like "%usdt"''' ,
                                    connection_to_tickers )

    tickers_df = pd.DataFrame ( sql_query )
    print ( tickers_df )
    print (len(tickers_df))

    #drop the table from the database with prices
    drop_table_from_database('crypto_assets_ohlc', path_to_database_table_in_which_to_be_dropped)


    i=0

    # get historical data from binance for a particual symbol into a dataframe
    for symbol in tickers_df['trading_pair']:
        try:
            i=i+1
            print ('symbol is', symbol, i, "/",len(tickers_df))
            klines = client.get_historical_klines ( symbol , interval , start_moment )
            klines_df = pd.DataFrame ( klines )
            # create colums names
            klines_df.columns = ['open_time' , 'open' , 'high' , 'low' , 'close' ,
                                 'volume' , 'close_time' , 'qav' , 'num_trades' ,
                                 'taker_base_vol' , 'taker_quote_vol' , 'ignore']
            #change unix timestamp into normal time

            klines_df.index = [dt.datetime.fromtimestamp ( x / 1000.0 ) for x in klines_df.open_time]
            print ( klines_df )

            # change string into float numbers
            klines_df = klines_df.astype ( float )

            klines_df.open_time=pd.to_datetime(klines_df["open_time"],unit='ms')
            klines_df.close_time=pd.to_datetime(klines_df["close_time"],unit='ms')




            #add another column to the dataframe with historical prices and name it symbol
            klines_df['Trading_pair']=symbol
            print ( klines_df )
            #insert the list of trading pairs into a database





            klines_df.to_sql("crypto_assets_ohlc",connection_to_prices,if_exists = 'append')
            #
            all_time_high = klines_df['high'].max ()
            all_time_low = klines_df['low'].min ()
            # print ( historical_data_for_trading_pair_df )
            print ( f'{symbol} all time high is' , all_time_high )
            print ( f'{symbol} all time low is ' , all_time_low )
            # Prevent exceeding rate limit:
            if int ( client.response.headers['x-mbx-used-weight-1m'] ) > 1_000:
                print ( 'Pausing for 30 seconds...' )
                time.sleep ( 30 )
        except Exception as e:
            if e==requests.exceptions.ReadTimeout:
                print ( f"\nRead_Time_out_error problem with {symbol}" , e )
                pass
            elif e==requests.exceptions.ConnectionError:
                print ( f"\nConnectionError problem with {symbol}" , e )
                pass
            elif e==urllib3.exceptions.MaxRetryError:
                print ( f"\nMaxRetryError problem with {symbol}" , e )
                pass

            # if e==requests.exceptions.RequestException:
            #     print ( f"\nRequestException problem with {symbol}" , e )
            #     pass

            else:

                print (f"\nAnother problem with {symbol}",e)
                continue
    connection_to_prices.close ()
    end_time=time.time()
    overall_time=end_time-start_time
    print('overall time in minutes=', overall_time/60.0)

#comment out the following  lines if you want to download only daily historical klines

# interval='1m'
# start_moment='1 day ago, UTC'
# path_to_databases=os.path.join(os.getcwd(),"datasets","sql_databases")
# Path(path_to_databases).mkdir(parents=True, exist_ok=True)
# connection_to_prices=sqlite3.connect(os.path.join(os.getcwd(),
#                                                           "datasets",
#                                                           "sql_databases",
#                                                           "binance_1_minute_historical_data.db"))
# path_to_database_table_in_which_to_be_dropped=os.path.join(os.getcwd(),"datasets",
#                                             "sql_databases","binance_1_minute_historical_data.db")
# insert_binance_trading_pair_ohlc_into_db(interval,
#                                          connection_to_prices,
#                                          path_to_database_table_in_which_to_be_dropped,
#                                          start_moment)

insert_binance_trading_pair_ohlc_into_db()