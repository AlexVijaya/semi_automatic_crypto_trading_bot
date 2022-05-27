import talib
from TAcharts.indicators.td_sequential import td_sequential
from drop_table_from_database import  drop_table_from_database
import pandas as pd
import datetime as dt
from datetime import datetime
import matplotlib.pyplot as plt
import os
import pprint
import time
from pathlib import Path
import sqlite3
import matplotlib.pyplot as plt
def find_asset_close_to_hh_and_ll():
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
    connection_to_binance_prices_db = sqlite3.connect ( os.path.join ( os.getcwd () , "datasets" ,
                                                            "sql_databases" ,
                                                            "binance_historical_data.db" ) )


    potential_lower_low_assets=[]
    potential_higher_high_assets=[]
    datetime_format = '%d-%m-%Y'
    datetime_now = datetime.now ()
    binance_foundation_day = datetime.strptime ( "11-07-2017" , datetime_format )

    binance_was_founded_these_number_of_days_ago = datetime_now - binance_foundation_day
    potential_higher_high_assets_df=pd.DataFrame(index=[days_ago for days_ago in range(binance_was_founded_these_number_of_days_ago.days,0,-50)])
    potential_lower_low_assets_df=pd.DataFrame(index=[days_ago for days_ago in range(binance_was_founded_these_number_of_days_ago.days,0,-50)])
    print([days_ago for days_ago in range(binance_was_founded_these_number_of_days_ago.days,0,-50)])
    for trading_pair in tickers_df['trading_pair']:
        #print ( trading_pair )
        sql_query_ohlc = pd.read_sql_query ( f'''select * from crypto_assets_ohlc 
                                                        where trading_pair="{trading_pair}"''' ,
                                             connection_to_binance_prices_db )
        historical_data_for_trading_pair_df = pd.DataFrame ( sql_query_ohlc )
        # historical_data_for_trading_pair_df[['index','open_time','close_time']]=\
        #     historical_data_for_trading_pair_df[['index','open_time','close_time']].apply(pd.to_datetime)
        #print ( historical_data_for_trading_pair_df )
        historical_data_for_trading_pair_df['index'] = pd.to_datetime ( historical_data_for_trading_pair_df['index'] ,
                                                                        format = "%Y-%m-%d %H:%M:%S" )
        #print ( historical_data_for_trading_pair_df )

        print ( binance_was_founded_these_number_of_days_ago )
        # historical_data_for_trading_pair_df.set_index('index', inplace = True)
        # print ( historical_data_for_trading_pair_df )
        periods_for_ll_and_hh=[days_ago for days_ago in range(binance_was_founded_these_number_of_days_ago.days,0,-50)]
        try:
            for period in periods_for_ll_and_hh:
                all_time_high=historical_data_for_trading_pair_df.iloc[-period:,
                              historical_data_for_trading_pair_df.columns.get_loc("high")].max()
                all_time_low = historical_data_for_trading_pair_df.iloc[-period:,
                               historical_data_for_trading_pair_df.columns.get_loc("low")].min()
                #print ( historical_data_for_trading_pair_df )
                # print (f'{trading_pair} all time high is', all_time_high)
                # print (f'{trading_pair} all time low is ', all_time_low)

                last_high=historical_data_for_trading_pair_df.loc[historical_data_for_trading_pair_df.index[-1],"high"]
                last_low = historical_data_for_trading_pair_df.loc[historical_data_for_trading_pair_df.index[-1] , "low"]
                last_open = historical_data_for_trading_pair_df.loc[historical_data_for_trading_pair_df.index[-1] , "open"]
                last_close = historical_data_for_trading_pair_df.loc[historical_data_for_trading_pair_df.index[-1] , "close"]

                last_but_one_high = historical_data_for_trading_pair_df.loc[historical_data_for_trading_pair_df.index[-2] , "high"]
                last_but_one_low = historical_data_for_trading_pair_df.loc[historical_data_for_trading_pair_df.index[-2] , "low"]
                last_but_one_open = historical_data_for_trading_pair_df.loc[historical_data_for_trading_pair_df.index[-2] , "open"]
                last_but_one_close = historical_data_for_trading_pair_df.loc[historical_data_for_trading_pair_df.index[-2] , "close"]

                # print(f'last high of {trading_pair} is ', last_high)
                # print ( f'last low of {trading_pair} is ' , last_low )
                # and last_low>all_time_low
                if (last_close-all_time_low)<((last_but_one_high-last_but_one_low)/1.0) :
                    # print ( f"in {trading_pair} last_close-all_time_low is ", last_close-all_time_low )

                    print ( f"in {trading_pair} the condition for being close "
                            f"to {period} day time low" )
                    potential_lower_low_assets.append(trading_pair)
                    potential_lower_low_assets_df.at[period,f"{trading_pair}"]=True
                    # print('\npotential_lower_low_assets_df.at[period,f"{trading_pair}"]=\n',
                    #       potential_lower_low_assets_df.to_string())
                    # print(f'last high of {trading_pair} is ', last_high)
                    # print ( f'last low of {trading_pair} is ' , last_low )
                    # print ( f'{trading_pair} all time low is ' , all_time_low )
                    print("-----------------------------------------------")

                if (all_time_high-last_close)<((last_but_one_high-last_but_one_low)/1.0) :

                    print ( f"in {trading_pair} the condition for being close to"
                            f" {period} day time high")
                    potential_higher_high_assets.append ( trading_pair )
                    potential_higher_high_assets_df.at[period , f"{trading_pair}"] = True
                    # print ( '\npotential_lower_low_assets_df.at[period,f"{trading_pair}"]=\n' ,
                    #         potential_higher_high_assets_df.to_string() )
                    # print(f'last high of {trading_pair} is ', last_high)
                    # print ( f'last low of {trading_pair} is ' , last_low )
                    # print ( f'{trading_pair} all_time_high ' , all_time_high )
                    print("+++++++++++++++++++++++++++++++++++++++++++++")
                # ax1=historical_data_for_trading_pair_df.loc[1000:].plot( figsize=(10,5), kind='line',x='index',y='close' )
                # historical_data_for_trading_pair_df.loc[1000:].plot( figsize=(10,5), kind='scatter',x='index',y='sar',ax=ax1,s=0.1,color='red' )
                # plt.show()



        except Exception as e:
            print(e)
            continue
        potential_higher_high_assets = list ( dict.fromkeys ( potential_higher_high_assets ) )
        potential_lower_low_assets = list ( dict.fromkeys ( potential_lower_low_assets ) )



        # print ( "potential_higher_highs_assets=" , potential_higher_high_assets ,
        #         "\n[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[\n",
        #         "potential_lower_low_assets=" , potential_lower_low_assets )
        # print ( "\n====================================\n"
        #         "\n====================================\n"
        #         "\n------------------------------------\n" )
    connection_to_binance_prices_db.close ()
    # print ( '\npotential_higher_high_assets_df\n' ,
    #         potential_higher_high_assets_df.to_string () )
    #
    # print ( '\npotential_lower_low_assets_df]=\n' ,
    #         potential_lower_low_assets_df.to_string () )

    return potential_higher_high_assets,potential_lower_low_assets

find_asset_close_to_hh_and_ll()