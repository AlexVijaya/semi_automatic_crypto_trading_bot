import os
import pandas as pd
import time
import datetime
import sqlite3
#import ccxt.async_support as ccxt
import ccxt
from find_keltner_channel import create_empty_database
def check_if_current_bar_closed_within_acceptable_range_to_yesterdays_mirror_level():
    start = time.perf_counter ()
    current_timestamp=time.time()
    #################################
    path_to_database = os.path.join ( os.getcwd () ,
                                      "datasets" ,
                                      "sql_databases" ,
                                      "multiple_tables_historical_data_for_usdt_trading_pairs_with_mirror_levels_with_highs_or_lows_yesterday.db" )
    connection_to_usdt_trading_pairs_daily_ohlcv = \
        sqlite3.connect ( path_to_database )

    cursor = connection_to_usdt_trading_pairs_daily_ohlcv.cursor ()
    cursor.execute ( "SELECT name FROM sqlite_master WHERE type='table';" )
    list_of_ohlcv_with_recent_mirror_highs_and_lows_with_tuples = cursor.fetchall ()
    list_of_ohlcv_with_recent_mirror_highs_and_lows = []
    for index , tuple_of_names in enumerate ( list_of_ohlcv_with_recent_mirror_highs_and_lows_with_tuples ):
        # print(tuple_of_names[0])
        # print(index)

        list_of_ohlcv_with_recent_mirror_highs_and_lows.append ( tuple_of_names[0] )
    print ( "list_of_ohlcv_with_recent_mirror_highs_and_lows\n" ,
            list_of_ohlcv_with_recent_mirror_highs_and_lows )


    ##################################
    path_to_db_with_USDT_and_btc_pairs = os.path.join ( os.getcwd () , "datasets" ,
                                                        "sql_databases" ,
                                                        "btc_and_usdt_pairs_from_all_exchanges.db" )

    connection_to_btc_and_usdt_trading_pairs = \
        sqlite3.connect ( path_to_db_with_USDT_and_btc_pairs )

    mirror_levels_df = pd.read_sql ( f'''select * from mirror_levels_without_duplicates ;''' ,
                                     connection_to_btc_and_usdt_trading_pairs )
    ###################################3
    path_to_usdt_trading_pairs_ohlcv_ready_for_rebound = os.path.join ( os.getcwd () ,
                                                      "datasets" ,
                                                      "sql_databases" ,
                                                      "ohlcv_tables_with_pairs_ready_to_rebound_from_mirror_level.db" )

    if os.path.exists(path_to_usdt_trading_pairs_ohlcv_ready_for_rebound):
        os.remove(path_to_usdt_trading_pairs_ohlcv_ready_for_rebound)
    print ( "before removal of db" )
    #time.sleep(20)
    print ( "after removal of db" )
    create_empty_database(path_to_usdt_trading_pairs_ohlcv_ready_for_rebound)

    connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound = \
        sqlite3.connect ( path_to_usdt_trading_pairs_ohlcv_ready_for_rebound )
    for row_number in range(0,len(mirror_levels_df)):
        try:
            usdt_trading_pair =mirror_levels_df.loc[row_number,'USDT_pair']
            exchange = mirror_levels_df.loc[row_number,'exchange']
            #print ("usdt_trading_pair=",usdt_trading_pair)
            #print ("exchange=",exchange)
            mirror_level = mirror_levels_df.loc[row_number , 'mirror_level']
            open_time_of_candle_with_legit_low = mirror_levels_df.loc[row_number ,
                                                                      'open_time_of_candle_with_legit_low']
            open_time_of_candle_with_legit_high = mirror_levels_df.loc[row_number ,
                                                                       'open_time_of_candle_with_legit_high']
            timestamp_for_low = (mirror_levels_df.loc[row_number ,
                                                      'timestamp_for_low'])/1000.0
            timestamp_for_high = (mirror_levels_df.loc[row_number ,
                                                       'timestamp_for_high'])/1000.0
            if (current_timestamp-timestamp_for_low)<86400*2:
                #last mirror level bar confirming level was was formed by low
                print(f"recently mirror level (low) was hit for {usdt_trading_pair} on {exchange}")
                exchange_object = getattr ( ccxt , exchange ) ()
                exchange_object.enableRateLimit = True
                exchange_object.load_markets()
                data =exchange_object.fetch_ohlcv ( usdt_trading_pair , '1d' )
                header = ['Timestamp' , 'open' , 'high' , 'low' , 'close' , 'volume']
                data_df = pd.DataFrame ( data , columns = header ).set_index ( 'Timestamp' )
                print("data_df\n",data_df)
                last_price=data_df.iloc[-1]["close"]

                penultimate_high = data_df.iloc[-2]["high"]
                penultimate_low = data_df.iloc[-2]["low"]
                print ( "last_price\n" , last_price )

                print ( "penultimate_high\n" , penultimate_high )
                print ( "penultimate_low\n" , penultimate_low )
                print ( "mirror_level\n" , mirror_level )
                true_range = penultimate_high - penultimate_low
                last_high = data_df.iloc[-1]["high"]
                last_low = data_df.iloc[-1]["low"]
                backlash=true_range*0.2
                print ( "backlash\n" , backlash )
                print ( "mirror_level+backlash\n" , mirror_level+backlash )
                print ( "true_range\n" , true_range )
                print ( "last_low\n" , last_low )
                if last_low>=mirror_level:
                    if last_low<=(mirror_level+backlash):
                        print(f"last low for {usdt_trading_pair} on {exchange}"
                              f" is within backlash")
                        data_df.to_sql ( f"{usdt_trading_pair}_on_{exchange}_at_{mirror_level}" ,
                                         connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound ,
                                         if_exists = 'replace' )




            if (current_timestamp-timestamp_for_high)<86400*2:
                # last mirror level bar confirming level was was formed by high
                print(f"recently mirror level (high) was hit for {usdt_trading_pair} on {exchange}")
                exchange_object = getattr ( ccxt , exchange ) ()
                exchange_object.enableRateLimit = True
                exchange_object.load_markets ()
                data = exchange_object.fetch_ohlcv ( usdt_trading_pair , '1d' )
                header = ['Timestamp' , 'open' , 'high' , 'low' , 'close' , 'volume']
                data_df = pd.DataFrame ( data , columns = header ).set_index ( 'Timestamp' )
                print ( "data_df\n" , data_df )
                last_price = data_df.iloc[-1]["close"]
                penultimate_high = data_df.iloc[-2]["high"]
                penultimate_low = data_df.iloc[-2]["low"]
                print ( "last_price\n" , last_price )
                print ( "penultimate_high\n" , penultimate_high )
                print ( "penultimate_low\n" , penultimate_low )
                print ( "last_price\n" , last_price )
                print ( "mirror_level\n" , mirror_level )
                true_range=penultimate_high-penultimate_low
                last_high = data_df.iloc[-1]["high"]
                last_low = data_df.iloc[-1]["low"]

                backlash = true_range*0.2
                print ( "backlash\n" , backlash )
                print ( "mirror_level-backlash\n" , mirror_level - backlash )
                print ( "true_range\n" , true_range )
                print ( "last_high\n" , last_high )
                if last_high <= mirror_level:
                    if last_high >= (mirror_level - backlash):
                        print ( f"last high for {usdt_trading_pair} on {exchange} "
                                f" is within backlash"  )
                        data_df.to_sql ( f"{usdt_trading_pair}_on_{exchange}_at_{mirror_level}" ,
                                         connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound ,
                                         if_exists = 'replace' )
        except Exception as e:
            print(e)
        finally:
            continue
    connection_to_usdt_trading_pairs_daily_ohlcv.close ()
    connection_to_btc_and_usdt_trading_pairs.close()
    connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound.close()
    end = time.perf_counter ()
    print ( "time in seconds is " , end - start )
    print ( "time in minutes is " , (end - start) / 60.0 )
    print ( "time in hours is " , (end - start) / 60.0 / 60.0 )


if __name__=="__main__":
    check_if_current_bar_closed_within_acceptable_range_to_yesterdays_mirror_level()