import talib

import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import os
import pprint
import time
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import sqlite3
from drop_table_from_database import drop_table_from_database
import matplotlib.pyplot as plt
def psar_calculate():
    path_to_databases = os.path.join ( os.getcwd () , "datasets" , "sql_databases" )
    Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )

    #get binance trading pairs into a dataframe
    connection_to_tickers = sqlite3.connect ( os.path.join ( os.getcwd () , "datasets" ,
                                                             "sql_databases" ,
                                                             "binance_trading_pairs.db" ) )
    sql_query_trading_pairs = pd.read_sql_query ( '''select trading_pair from trading_pairs_tickers 
                                            where trading_pair like "%usdt"''' ,
                                    connection_to_tickers )

    tickers_df = pd.DataFrame ( sql_query_trading_pairs )


    #select a trading pair from  crypto_assets_ohlc table
    connection_to_prices = sqlite3.connect ( os.path.join ( os.getcwd () , "datasets" ,
                                                            "sql_databases" ,
                                                            "binance_historical_data.db" ) )

    path_to_databases = os.path.join ( os.getcwd () , "datasets" , "sql_databases" )
    Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )
    connection_to_db_to_include_sar = sqlite3.connect ( os.path.join ( os.getcwd () , "datasets" ,
                                                            "sql_databases" ,
                                                            "binance_historical_data_with_sar.db" ) )
    drop_table_from_database("crypto_assets_ohlc_plus_sar",os.path.join ( os.getcwd () , "datasets" ,
                                                            "sql_databases" ,
                                                            "binance_historical_data_with_sar.db" ))
    for trading_pair in tickers_df['trading_pair']:
        print(trading_pair)
        sql_query_ohlc = pd.read_sql_query ( f'''select * from crypto_assets_ohlc 
                                                    where trading_pair="{trading_pair}"''' ,
                                                      connection_to_prices )
        historical_data_for_trading_pair_df=pd.DataFrame(sql_query_ohlc)
        print ( historical_data_for_trading_pair_df )
        # historical_data_for_trading_pair_df[['index','open_time','close_time']]=\
        #     historical_data_for_trading_pair_df[['index','open_time','close_time']].apply(pd.to_datetime)
        historical_data_for_trading_pair_df['index']=pd.to_datetime(historical_data_for_trading_pair_df['index'],format="%Y-%m-%d %H:%M:%S")
        print ( historical_data_for_trading_pair_df )
        #historical_data_for_trading_pair_df.set_index('index', inplace = True)
        #print ( historical_data_for_trading_pair_df )
        historical_data_for_trading_pair_df['sar']=talib.SAR(historical_data_for_trading_pair_df.high,
                                                             historical_data_for_trading_pair_df.low,
                                                             acceleration=0.02, maximum=0.2)
        print ( "*******\n",historical_data_for_trading_pair_df )

        # ax1=historical_data_for_trading_pair_df.loc[1000:].plot( figsize=(10,5), kind='line',x='index',y='close' )
        # historical_data_for_trading_pair_df.loc[1000:].plot( figsize=(10,5), kind='scatter',x='index',y='sar',ax=ax1,s=0.1,color='red' )
        # plt.show()



        historical_data_for_trading_pair_df.to_sql ( "crypto_assets_ohlc_plus_sar" , connection_to_db_to_include_sar , if_exists = 'append',index=False )
    connection_to_prices.close ()
    connection_to_db_to_include_sar.close()

psar_calculate()