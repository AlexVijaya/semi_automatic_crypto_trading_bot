import talib
from TAcharts.indicators.td_sequential import td_sequential
from drop_table_from_database import  drop_table_from_database
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import os
import pprint
import time
from pathlib import Path
import sqlite3
import matplotlib.pyplot as plt
def td_sequential_calculate():
    path_to_databases = os.path.join ( os.getcwd () , "datasets" , "sql_databases" )
    Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )

    # get binance trading pairs into a dataframe
    connection_to_tickers = sqlite3.connect ( os.path.join ( os.getcwd () , "datasets" ,
                                                             "sql_databases" ,
                                                             "binance_trading_pairs.db" ) )
    sql_query_trading_pairs = pd.read_sql_query ( '''select trading_pair from trading_pairs_tickers 
                                                where trading_pair like "%usdt"''' ,
                                                  connection_to_tickers )

    tickers_df = pd.DataFrame ( sql_query_trading_pairs )

    # select a trading pair from  crypto_assets_ohlc table
    connection_to_prices_with_sar = sqlite3.connect ( os.path.join ( os.getcwd () , "datasets" ,
                                                            "sql_databases" ,
                                                            "binance_historical_data_with_sar.db" ) )

    path_to_databases = os.path.join ( os.getcwd () , "datasets" , "sql_databases" )
    Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )
    connection_to_db_to_include_sar_and_td = sqlite3.connect ( os.path.join ( os.getcwd () , "datasets" ,
                                                                        "sql_databases" ,
                                                                        "binance_historical_data_with_sar_and_td.db" ) )
    drop_table_from_database ( "crypto_assets_ohlc_plus_sar_and_td" , os.path.join ( os.getcwd () , "datasets" ,
                                                                        "sql_databases" ,
                                                                        "binance_historical_data_with_sar_and_td.db" ) )
    for trading_pair in tickers_df['trading_pair']:
        print ( trading_pair )
        sql_query_ohlc = pd.read_sql_query ( f'''select * from crypto_assets_ohlc_plus_sar 
                                                        where trading_pair="{trading_pair}"''' ,
                                             connection_to_prices_with_sar )
        historical_data_for_trading_pair_df = pd.DataFrame ( sql_query_ohlc )
        # historical_data_for_trading_pair_df[['index','open_time','close_time']]=\
        #     historical_data_for_trading_pair_df[['index','open_time','close_time']].apply(pd.to_datetime)
        print ( historical_data_for_trading_pair_df )
        historical_data_for_trading_pair_df['index'] = pd.to_datetime ( historical_data_for_trading_pair_df['index'] ,
                                                                        format = "%Y-%m-%d %H:%M:%S" )
        print ( historical_data_for_trading_pair_df )

        # historical_data_for_trading_pair_df.set_index('index', inplace = True)
        # print ( historical_data_for_trading_pair_df )
        historical_data_for_trading_pair_df['td'] = 'test'
        print ( historical_data_for_trading_pair_df )
        # ax1=historical_data_for_trading_pair_df.loc[1000:].plot( figsize=(10,5), kind='line',x='index',y='close' )
        # historical_data_for_trading_pair_df.loc[1000:].plot( figsize=(10,5), kind='scatter',x='index',y='sar',ax=ax1,s=0.1,color='red' )
        # plt.show()

        historical_data_for_trading_pair_df.to_sql ( "crypto_assets_ohlc_plus_sar_and_td" , connection_to_db_to_include_sar_and_td ,
                                                     if_exists = 'append',index=False )
    connection_to_prices_with_sar.close ()
    connection_to_db_to_include_sar_and_td.close ()

td_sequential_calculate()