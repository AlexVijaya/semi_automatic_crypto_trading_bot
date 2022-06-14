import sqlite3
import pandas as pd
import os
import time
import datetime
import traceback
import datetime as dt
import tzlocal
def drop_table_from_database(table_name,path_to_database):
    conn=sqlite3.connect(path_to_database)
    cur=conn.cursor()
    cur.execute('drop table if exists {}'.format(table_name))
    conn.commit()
    conn.close()

def find_mirror_levels_in_database(async_var):
    start_time = time.time ()
    connection_to_usdt_trading_pairs_ohlcv = \
        sqlite3.connect ( os.path.join ( os.getcwd () ,
                                         "datasets" ,
                                         "sql_databases" ,
                                         "all_exchanges_multiple_tables_historical_data_for_usdt_trading_pairs.db" ) )

    #async_var = True
    if async_var==True:
        connection_to_usdt_trading_pairs_ohlcv = \
            sqlite3.connect ( os.path.join ( os.getcwd () ,
                                             "datasets" ,
                                             "sql_databases" ,
                                             "async_all_exchanges_multiple_tables_historical_data_for_usdt_trading_pairs.db" ) )

    cursor=connection_to_usdt_trading_pairs_ohlcv.cursor()
    cursor.execute ( "SELECT name FROM sqlite_master WHERE type='table';" )

    list_of_tables_from_sql_query=cursor.fetchall ()
    list_of_tables=[]
    #open_connection to database with mirror levels
    path_to_db_with_USDT_and_btc_pairs = os.path.join ( os.getcwd () , "datasets" ,
                                                        "sql_databases" ,
                                                        "btc_and_usdt_pairs_from_all_exchanges.db" )

    connection_to_btc_and_usdt_trading_pairs = \
        sqlite3.connect ( path_to_db_with_USDT_and_btc_pairs )
    mirror_level_df = pd.DataFrame ( columns = ['USDT_pair' , 'exchange' , 'mirror_level' ,
                                                'timestamp_for_low' , 'timestamp_for_high' ,
                                                'low' , 'high' , 'open_time_of_candle_with_legit_low' ,
                                                'open_time_of_candle_with_legit_high','average_volume'] )
    try:
        drop_table_from_database ( "mirror_levels_calculated_separately" ,
                                   path_to_db_with_USDT_and_btc_pairs )
    except:
        print ( "cant drop table from db" )


    for table_in_db in list_of_tables_from_sql_query:
        #print(table_in_db[0])
        list_of_tables.append(table_in_db[0])

    print(list_of_tables)
    counter=0
    discard_pair_by_volume_counter = 0
    this_many_last_days=90
    calculate_average_volume_for_this_many_days=30
    volume_limit=100000
    for table_in_db in list_of_tables:
        try:
            counter=counter+1
            data_df=\
                pd.read_sql_query(f'''select * from "{table_in_db}"''' ,
                                  connection_to_usdt_trading_pairs_ohlcv)
            print ( "data_df\n",data_df )

            #data_df.set_index("Timestamp")
            #print ( "data_df\n" , data_df )
            print("---------------------------")
            print(f'{table_in_db} is number {counter} out of {len(list_of_tables)}\n' )
            #print("usdt_ohlcv_df\n",data_df )
            usdt_pair=data_df.loc[0,"trading_pair"]
            exchange=data_df.loc[0,"exchange"]
            #data_df.reset_index()
            data_df.set_index("Timestamp", inplace = True)
            data_df["volume_by_close"] = data_df["volume"] * data_df["close"]

            #calculate mirror levels and insert them into db
            last_several_days_slice_df = data_df.tail ( this_many_last_days )
            average_volume=last_several_days_slice_df[
                "volume_by_close"].tail(calculate_average_volume_for_this_many_days).mean()
            print("average volume=", average_volume)

            #discard pairs with low volume
            if average_volume<volume_limit:
                print(f"average volume is less than {volume_limit}"
                      f" for {usdt_pair} on {exchange}  ")
                discard_pair_by_volume_counter=discard_pair_by_volume_counter+1
                print("discard_pair_by_volume_counter=",
                      discard_pair_by_volume_counter)
                continue
            #last_several_days_slice_df.set_index('open_time')

            if last_several_days_slice_df.duplicated ( subset = 'high' ,
                                                       keep = False ).sum () == len ( last_several_days_slice_df ):
                print ( f"all duplicated highs are found in {usdt_pair} on {exchange}" )
                continue
            # print ( "last_several_days_slice_df=\n" , last_several_days_slice_df.to_string () )
            #
            #print ( "last_several_days_slice_df=\n" , last_several_days_slice_df.to_string () )
            last_several_days_highs_slice_Series = \
                last_several_days_slice_df['high'].squeeze ()
            last_several_days_lows_slice_Series = \
                last_several_days_slice_df['low'].squeeze ()
            # print (type(last_several_days_lows_slice_Series))
            # print ( "last_several_days_lows_slice_Series=\n" ,
            #         last_several_days_lows_slice_Series.to_string () )


            for row_number_in_highs in range ( last_several_days_highs_slice_Series.size ):
                for row_number_in_lows in range ( last_several_days_lows_slice_Series.size ):
                    daily_high = last_several_days_highs_slice_Series.iloc[row_number_in_highs - 1]
                    daily_low = last_several_days_lows_slice_Series.iloc[row_number_in_lows - 1]
                    if daily_high == daily_low:
                        # print ( "daily_low\n" ,
                        #         daily_low )
                        # print ( "daily_high\n" ,
                        #         daily_high )
                        if row_number_in_lows > 1 and row_number_in_lows < last_several_days_lows_slice_Series.size:
                            print ( "found not boundary low" )

                        if row_number_in_highs > 1 and row_number_in_highs < last_several_days_highs_slice_Series.size:
                            print ( "found not boundary high" )
                            if row_number_in_lows > 1 and row_number_in_lows < last_several_days_lows_slice_Series.size:
                                prev_daily_high = last_several_days_highs_slice_Series.iloc[row_number_in_highs - 2]
                                next_daily_high = last_several_days_highs_slice_Series.iloc[row_number_in_highs]
                                prev_daily_low = last_several_days_lows_slice_Series.iloc[
                                    row_number_in_lows - 2]
                                next_daily_low = last_several_days_lows_slice_Series.iloc[row_number_in_lows]

                                #print("last_several_days_lows_slice_Series.iloc[row_number_in_lows]=\n",
                                #      last_several_days_lows_slice_Series.iloc[row_number_in_lows])

                                # print ( "prev_daily_high\n" , prev_daily_high )
                                # print ( "next_daily_high\n" , next_daily_high )
                                # print ( "prev_daily_low\n" , prev_daily_low )
                                # print ( "next_daily_low\n" , next_daily_low )
                                if prev_daily_low > daily_low and next_daily_low > daily_low:
                                    print ( "found legit low" )

                                if prev_daily_high < daily_high and next_daily_high < daily_high:
                                    print ( "found legit high" )
                                    if prev_daily_low > daily_low and next_daily_low > daily_low:
                                        print ( "level is legit\n" )

                                        #print ( "last_several_days_lows_slice_Series\n" ,
                                        #        last_several_days_lows_slice_Series )

                                        list_of_tuples_of_lows = list ( last_several_days_lows_slice_Series.items () )
                                        #print ( "list_of_tuples_of_lows\n",list_of_tuples_of_lows )
                                        tuple_of_legit_low_level = list_of_tuples_of_lows[row_number_in_lows - 1]
                                        #print ( "tuple_of_legit_low_level\n" ,
                                        #        tuple_of_legit_low_level )

                                        list_of_tuples_of_highs = list (
                                            last_several_days_highs_slice_Series.items () )
                                        #print ( "list_of_tuples_of_highs\n" , list_of_tuples_of_highs )
                                        tuple_of_legit_high_level = list_of_tuples_of_highs[
                                            row_number_in_highs - 1]
                                        #print ("tuple_of_legit_high_level\n", tuple_of_legit_high_level)

                                        # print ( "dt.datetime.fromtimestamp ( tuple_of_legit_low_level[0] )\n" ,
                                        #         dt.datetime.fromtimestamp ( tuple_of_legit_low_level[0] / 1000.0 ) )
                                        #
                                        # print ( "dt.datetime.fromtimestamp ( tuple_of_legit_high_level[0] )" ,
                                        #         dt.datetime.fromtimestamp ( tuple_of_legit_high_level[0] / 1000.0 ) )

                                        mirror_level_df.loc[0 , 'USDT_pair'] = usdt_pair
                                        mirror_level_df.loc[0 , 'exchange'] = exchange
                                        mirror_level_df.loc[0 , 'mirror_level'] = daily_low
                                        mirror_level_df.loc[0 , 'timestamp_for_low'] = \
                                            tuple_of_legit_low_level[0]
                                        mirror_level_df.loc[0 , 'timestamp_for_high'] = \
                                            tuple_of_legit_high_level[0]
                                        mirror_level_df.loc[0 , 'low'] = daily_low
                                        mirror_level_df.loc[0 , 'high'] = daily_high
                                        mirror_level_df.loc[0 , 'open_time_of_candle_with_legit_low'] = \
                                            dt.datetime.fromtimestamp ( tuple_of_legit_low_level[0] / 1000.0 )
                                        mirror_level_df.loc[0 , 'open_time_of_candle_with_legit_high'] = \
                                            dt.datetime.fromtimestamp ( tuple_of_legit_high_level[0] / 1000.0 )
                                        mirror_level_df.loc[0 , 'average_volume'] = average_volume

                                        print ( 'mirror_level_df\n' , mirror_level_df )



                                        mirror_level_df.to_sql ( "mirror_levels_calculated_separately" ,
                                                                 connection_to_btc_and_usdt_trading_pairs ,
                                                                 if_exists = 'append' , index = False )

                        pass
        except Exception as e:
            print(f"problem with {table_in_db}", e)
            traceback.print_exc ()
            continue
    connection_to_usdt_trading_pairs_ohlcv.close()
    connection_to_btc_and_usdt_trading_pairs.close()
    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 3600.0 )
    print ( 'overall time=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time=' , start_time )
    print ( 'end_time=' , end_time )
if __name__=="__main__":
    async_var=False
    find_mirror_levels_in_database(async_var)

#print("__name__",__name__)
# find_mirror_levels_in_database ()
#
# def drop_duplicates_in_db():
#     path_to_db_with_USDT_and_btc_pairs = os.path.join ( os.getcwd () , "datasets" ,
#                                                         "sql_databases" ,
#                                                         "btc_and_usdt_pairs_from_all_exchanges.db" )
#
#     connection_to_btc_and_usdt_trading_pairs = \
#         sqlite3.connect ( path_to_db_with_USDT_and_btc_pairs )
#     mirror_levels_df=pd.read_sql_query("mirror_levels_calculated_separately",
#                                        connection_to_btc_and_usdt_trading_pairs)
#     print("number of trading pairs with duplicates=",
#           len(mirror_levels_df))
#     mirror_levels_df.drop_duplicates(subset = ["USDT_pair","mirror_level"],
#                                      keep = "first",
#                                      inplace = True)
#     mirror_levels_df.to_sql("mirror_levels_without_duplicates",
#                             connection_to_btc_and_usdt_trading_pairs)
#     print ( "number of trading pairs without duplicates=" ,
#             len ( mirror_levels_df ) )
#     pass
# drop_duplicates_in_db()