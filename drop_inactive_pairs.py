import sqlite3
import pandas as pd
import datetime
import time
import os

def drop_inactive_pairs():
    start = time.perf_counter ()
    path_to_database=os.path.join ( os.getcwd () ,
                                         "datasets" ,
                                         "sql_databases" ,
                                         "async_all_exchanges_multiple_tables_historical_data_for_usdt_trading_pairs_copy.db" )
    connection_to_usdt_trading_pairs_daily_ohlcv = \
        sqlite3.connect (path_to_database )

    cursor = connection_to_usdt_trading_pairs_daily_ohlcv.cursor ()
    cursor.execute ( "SELECT name FROM sqlite_master WHERE type='table';" )
    list_of_ohlcv_with_recent_mirror_highs_and_lows_with_tuples = cursor.fetchall ()
    list_of_ohlcv_with_recent_mirror_highs_and_lows=[]
    for index, tuple_of_names in enumerate(list_of_ohlcv_with_recent_mirror_highs_and_lows_with_tuples):
        #print(tuple_of_names[0])
        #print(index)

        list_of_ohlcv_with_recent_mirror_highs_and_lows.append(tuple_of_names[0])
    print("list_of_ohlcv_with_recent_mirror_highs_and_lows\n",
          list_of_ohlcv_with_recent_mirror_highs_and_lows)
    not_active_pair_counter=0

    for number_of_table,table_name in enumerate(list_of_ohlcv_with_recent_mirror_highs_and_lows):
        data_df = \
            pd.read_sql_query ( f'''select * from "{table_name}"''' ,
                                connection_to_usdt_trading_pairs_daily_ohlcv )
        trading_pair=table_name.split("_on_")[0]
        exchange=table_name.split("_on_")[1]
        print ( f"{trading_pair} on {exchange} is {number_of_table} "
                f"out of {len ( list_of_ohlcv_with_recent_mirror_highs_and_lows )}" )
        print(f"{table_name} is {number_of_table} "
              f"out of {len(list_of_ohlcv_with_recent_mirror_highs_and_lows)}")

        data_df.set_index ( "Timestamp" , inplace = True )
        print ( "data_df\n" , data_df )

        last_date_with_time = data_df["open_time"].iloc[-1]
        print ( "type(last_date_with_time)\n" , type ( last_date_with_time ) )
        print ( "last_date_with_time\n" , last_date_with_time )
        last_date_without_time = last_date_with_time.split ( " " )
        print ( "last_date_with_time\n" , last_date_without_time[0] )
        last_date_without_time = last_date_without_time[0]

        current_timestamp = time.time ()
        last_timestamp_in_df = data_df.tail ( 1 ).index.item () / 1000.0
        print ( "current_timestamp=" , current_timestamp )
        print ( "data_df.tail(1).index.item()=" , data_df.tail ( 1 ).index.item () / 1000.0 )

        #check if the pair is active
        if abs(current_timestamp-last_timestamp_in_df)>(86400*2):
            print("last_date_without_time=",last_date_without_time)
            print ( "current_timestamp=" , current_timestamp )
            print ( "last_timestamp_in_df=" , last_timestamp_in_df )
            print ( "current_timestamp-last_timestamp_in_df=" ,
                    current_timestamp-last_timestamp_in_df )
            print(f" not active trading pair {trading_pair} on {exchange}")
            not_active_pair_counter=not_active_pair_counter+1
            print("not_active_pair_counter=",not_active_pair_counter)
            print(type(table_name))
            cursor.execute(f"drop table if exists '{table_name}'")
            continue

        print ( "final_not_active_pair_counter=" , not_active_pair_counter )
    # print("list_of_ohlcv_with_recent_mirror_highs_and_lows\n",
    #       list_of_ohlcv_with_recent_mirror_highs_and_lows)

    connection_to_usdt_trading_pairs_daily_ohlcv.close()
    end = time.perf_counter ()
    print ( "time in seconds is " , end - start )
    print ( "time in minutes is " , (end - start) / 60.0 )
    print ( "time in hours is " , (end - start) / 60.0 / 60.0 )


    pass
if __name__=="__main__":
    drop_inactive_pairs()