import traceback
from Heiken_Ashi_calculation import HA
import talib
from binance_config import api_secret
from binance_config import api_key
from binance.client import Client
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import os
import pprint
import time
from pathlib import Path
import sqlite3
from drop_table_from_database import drop_table_from_database
def insert_only_one_binance_trading_pair_ohlc_into_db(symbol,interval = '1d',connection_to_prices=sqlite3.connect(os.path.join(os.getcwd(),
                                                          "datasets",
                                                          "sql_databases",
                                                          "binance_historical_data.db")),
                                            path_to_database_table_in_which_to_be_dropped=os.path.join(os.getcwd(),"datasets",
                                            "sql_databases","binance_historical_data.db"),
                                            start_moment="1 Jan,2017"):
    start_time=time.time()
    client = Client ( api_key = api_key , api_secret = api_secret )

    # path_to_databases = os.path.join ( os.getcwd () , "datasets" , "sql_databases" )
    # Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )

    # path_to_database_table_in_which_to_be_dropped=os.path.join(os.getcwd(),"datasets",
    #                                             "sql_databases","binance_historical_data.db")

    # connection_to_tickers = sqlite3.connect ( os.path.join ( os.getcwd () ,
    #                                                          "datasets" ,
    #                                                          "sql_databases" ,
    #                                                          "binance_trading_pairs.db" ) )
    #uncomment if  you want to select all trading pairs from binance
    # sql_query = pd.read_sql_query ( '''select trading_pair from trading_pairs_tickers''' ,
    #                                 connection_to_tickers )


    # sql_query = pd.read_sql_query ( '''select trading_pair from trading_pairs_tickers
    #                                     where trading_pair like "%usdt"''' ,
    #                                 connection_to_tickers )
    #
    # tickers_df = pd.DataFrame ( sql_query )
    # print ( tickers_df )
    # print (len(tickers_df))

    #drop the table from the database with prices
    drop_table_from_database(f"ohlc_for_{symbol}", path_to_database_table_in_which_to_be_dropped)


    i=0

    # get historical data from binance for a particual symbol into a dataframe

    try:
        i=i+1

        klines = client.get_historical_klines ( symbol , interval , start_moment )
        klines_df = pd.DataFrame ( klines )
        #print ( klines_df.to_string() )
        # create colums names
        klines_df.columns = ['open_time' , 'open' , 'high' , 'low' , 'close' ,
                             'volume' , 'close_time' , 'qav' , 'num_trades' ,
                             'taker_base_vol' , 'taker_quote_vol' , 'ignore']
        #change unix timestamp into normal time


        #klines_df.open_time = [dt.datetime.fromtimestamp ( x / 1000.0 ) for x in klines_df.open_time]
        #print ( klines_df )

        # change string into float numbers

        #klines_df.open_time=pd.to_datetime(klines_df["open_time"],unit='ms')
        klines_df.close_time=pd.to_datetime(klines_df["close_time"],unit='ms')




        #add another column to the dataframe with historical prices and name it symbol
        klines_df['trading_pair']=symbol
        print ("klines_df\n", klines_df.tail(1).to_string() )
        #insert the list of trading pairs into a database

        klines_df.drop(["close_time","qav","num_trades","taker_base_vol","taker_quote_vol"
                       ,"ignore"],axis=1,inplace=True)

        try:
            print("klines_df.dtypes\n",klines_df.dtypes)
            klines_df=klines_df.iloc[:,0:].apply(pd.to_numeric,errors='ignore')
            klines_df.open_time = [dt.datetime.fromtimestamp ( x / 1000.0 ) for x in klines_df.open_time]
            klines_df.open_time = pd.to_datetime ( klines_df["open_time"] , unit = 'ms' )
            print ( "klines_df.dtypes",klines_df.dtypes)
            #klines_df = klines_df.loc[:,klines_df.columns!='open_time'].astype ( float )
        except Exception as e:
            print(e)
            traceback.print_exc()


        heiken_ashi_df = HA ( klines_df )

        print (heiken_ashi_df)
        heiken_ashi_df = heiken_ashi_df.rename (
            columns = {'open': 'HA_open' , 'high': 'HA_high' , 'low': 'HA_low' , 'close': 'HA_close'} )
        klines_df = klines_df.join ( heiken_ashi_df )

        # try:
        #     print ( "\n\n\n\nheiken_ashi_df.dtypes\n",heiken_ashi_df.dtypes )
        #     # klines_df = klines_df.apply ( pd.to_numeric , errors = 'ignore' )
        #     # print ( klines_df.dtypes )
        #     # klines_df = klines_df.loc[:,klines_df.columns!='open_time'].astype ( float )
        # except Exception as e:
        #     print ( e )
        #     traceback.print_exc ()
        klines_df["sar"] = talib.SAR ( klines_df.high , klines_df.low ,
                                       acceleration = 0.02 ,
                                       maximum = 0.2)
        klines_df['id']=klines_df.index
        klines_df.to_sql(f"ohlc_for_{symbol}",connection_to_prices,if_exists = 'replace')
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
        print (f"\nproblem with {symbol}",e)
        traceback.print_exc()

    #connection_to_prices.close ()
    end_time=time.time()
    overall_time=end_time-start_time
    print('overall time in minutes=', overall_time/60.0)

#comment out the following  lines if you want to download only daily historical klines

# interval='1m'
# start_moment='1 day ago, UTC'
# symbol='BTCUSDT'
# path_to_databases=os.path.join(os.getcwd(),"datasets","sql_databases")
# Path(path_to_databases).mkdir(parents=True, exist_ok=True)
# connection_to_prices=sqlite3.connect(os.path.join(os.getcwd(),
#                                                           "datasets",
#                                                           "sql_databases",
#                                                           "binance_1_minute_historical_data.db"))
# path_to_database_table_in_which_to_be_dropped=os.path.join(os.getcwd(),"datasets",
#                                             "sql_databases","binance_1_minute_historical_data.db")
# insert_only_one_binance_trading_pair_ohlc_into_db(symbol,interval,
#                                          connection_to_prices,
#                                          path_to_database_table_in_which_to_be_dropped,
#                                          start_moment)