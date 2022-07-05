import sqlite3
import pandas as pd
import os
import time
import datetime
import traceback
import datetime as dt
import tzlocal
import numpy as np
from collections import Counter
def drop_table_from_database(table_name,path_to_database):
    conn=sqlite3.connect(path_to_database)
    cur=conn.cursor()
    cur.execute('drop table if exists {}'.format(table_name))
    conn.commit()
    conn.close()

def find_asc_desc_triangles_in_database(async_var):
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
    counter_for_usdt_pair_with_low = 0
    counter_for_usdt_pair_with_high = 0
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
        drop_table_from_database ( "asc_desc_triangles_levels" ,
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
    list_of_pairs_and_exchanges_with_touches_of_level_by_low=[]
    list_of_pairs_and_exchanges_with_touches_of_level_by_high=[]
    not_recent_pair_counter=0
    for table_in_db in list_of_tables:
        try:
            counter=counter+1
            data_df=\
                pd.read_sql_query(f'''select * from "{table_in_db}"''' ,
                                  connection_to_usdt_trading_pairs_ohlcv)
            #print ( "data_df\n",data_df )

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

            current_timestamp = time.time ()
            last_timestamp_in_df = data_df.tail ( 1 ).index.item () / 1000.0
            if (current_timestamp-last_timestamp_in_df)>(86400*3):
                not_recent_pair_counter=not_recent_pair_counter+1
                print("not_recent_pair_counter=",not_recent_pair_counter)
                continue




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
            # last_several_days_highs_slice_Series = \
            #     last_several_days_slice_df['high'].squeeze ()
            # last_several_days_lows_slice_Series = \
            #     last_several_days_slice_df['low'].squeeze ()
            # print (type(last_several_days_lows_slice_Series))
            # print ( "last_several_days_lows_slice_Series=\n" ,
            #         last_several_days_lows_slice_Series.to_string () )

            #iterate over each date and check if highs coincide
            # for row_number_in_highs in range ( last_several_days_highs_slice_Series.size ):
            #     #for row_number_in_lows in range ( last_several_days_lows_slice_Series.size ):
            #     daily_high = last_several_days_highs_slice_Series.iloc[row_number_in_highs - 1]

            #print("last_several_days_highs_slice_Series.duplicated()")
            #print(last_several_days_highs_slice_Series.duplicated().to_string())
            #print ( 'data_df["high"].value_counts()' )
            #data_df["number_of_equal_highs"]=data_df.groupby(data_df["high"].count)
            #print(data_df["high"].value_counts().to_string())
            #print ( "data_df\n",data_df )
            data_df.reset_index(inplace=True)
            # count_equal_highs_dict=Counter(data_df["high"].to_list())
            # count_equal_highs_df=pd.DataFrame(count_equal_highs_dict.items(),
            #                                   columns=["high","number_of_equal_highs"])
            #
            # count_equal_lows_dict = Counter ( data_df["low"].to_list () )
            # count_equal_lows_df = pd.DataFrame ( count_equal_lows_dict.items () ,
            #                                       columns = ["low" , "number_of_equal_lows"] )
            #
            # data_df_plus_count_high_values_merged_full_df = pd.merge ( data_df ,count_equal_highs_df,
            #                                               on = 'high' , how = 'left' )
            # data_df_plus_count_low_values_merged_full_df = pd.merge ( data_df , count_equal_lows_df ,
            #                                               on = 'low' , how = 'left' )
            #
            # data_df_plus_number_of_equal_lows_and_highs = pd.merge ( data_df_plus_count_high_values_merged_full_df ,
            #                                                       count_equal_lows_df,
            #                                               on = 'low' , how = 'left' )
            #
            # print(data_df_plus_count_low_values_merged_full_df)
            # print ( data_df_plus_count_high_values_merged_full_df )
            # print ( data_df_plus_number_of_equal_lows_and_highs )
            # #print(type(data_df["high"].value_counts()))
            # #data_df.assign(counts_of_highs=data_df["high"].value_counts())
            # #data_df["counts_of_highs"]=data_df["high"].value_counts()
            # #data_df["counts_of_lows"] = data_df["low"].value_counts ()
            # print("data_df\n", data_df)
            #
            so_many_last_days_for_level_calculation=30
            so_many_number_of_touches_of_level_by_highs=2
            so_many_number_of_touches_of_level_by_lows = 2
            #
            #
            # last_n_days_slice_from_ohlcv_data_df=\
            #     data_df_plus_number_of_equal_lows_and_highs.tail(so_many_last_days_for_level_calculation)
            #
            # for row in range(len(last_n_days_slice_from_ohlcv_data_df)):
            #     print ( "len ( last_n_days_slice_from_ohlcv_data_df )=" )
            #     print(len(last_n_days_slice_from_ohlcv_data_df))
            #     print ( "row=",row )
            #     print ( 'last_n_days_slice_from_ohlcv_data_df["number_of_equal_lows"].iloc[row]' )
            #     print ( last_n_days_slice_from_ohlcv_data_df["number_of_equal_lows"].iloc[row] )
            #     # print ( 'last_n_days_slice_from_ohlcv_data_df\n' )
            #     # print ( last_n_days_slice_from_ohlcv_data_df )
            #     if last_n_days_slice_from_ohlcv_data_df[
            #         "number_of_equal_lows"].iloc[row]==so_many_number_of_touches_of_level_by_lows:
            #         print("found row with the necessary number of touches of the level by lows")
            #
            #
            #
            # for row in range ( len ( last_n_days_slice_from_ohlcv_data_df ) ):
            #     # print ( 'last_n_days_slice_from_ohlcv_data_df\n' )
            #     # print ( last_n_days_slice_from_ohlcv_data_df )
            #     if last_n_days_slice_from_ohlcv_data_df[
            #         "number_of_equal_highs"].iloc[row] == so_many_number_of_touches_of_level_by_highs:
            #         print ( "found row with the necessary number of touches of the level by highs" )
            #
            #     print ( 'last_n_days_slice_from_ohlcv_data_df["number_of_equal_highs"].iloc[row]' )
            #     print ( last_n_days_slice_from_ohlcv_data_df["number_of_equal_highs"].iloc[row] )
            #

            last_n_days_from_data_df=data_df.tail(so_many_last_days_for_level_calculation)
            # print("last_n_days_from_data_df\n")
            # print ( last_n_days_from_data_df )

            count_equal_highs_dict=Counter(last_n_days_from_data_df["high"].to_list())
            count_equal_highs_df=pd.DataFrame(count_equal_highs_dict.items(),
                                              columns=["high","number_of_equal_highs"])

            count_equal_lows_dict = Counter ( last_n_days_from_data_df["low"].to_list () )
            count_equal_lows_df = pd.DataFrame ( count_equal_lows_dict.items () ,
                                                  columns = ["low" , "number_of_equal_lows"] )

            data_df_plus_count_high_values_merged_full_df = pd.merge ( last_n_days_from_data_df ,count_equal_highs_df,
                                                          on = 'high' , how = 'left' )
            data_df_plus_count_low_values_merged_full_df = pd.merge ( last_n_days_from_data_df , count_equal_lows_df ,
                                                          on = 'low' , how = 'left' )

            data_df_slice_plus_number_of_equal_lows_and_highs = pd.merge ( data_df_plus_count_high_values_merged_full_df ,
                                                                  count_equal_lows_df,
                                                          on = 'low' , how = 'left' )
            print ( f'{table_in_db} is number {counter} out of {len ( list_of_tables )}\n' )
            data_df_slice_plus_number_of_equal_lows_and_highs["low_level"] = np.nan
            data_df_slice_plus_number_of_equal_lows_and_highs["high_level"] = np.nan
            lower_closes_for_that_many_last_days=5
            higher_closes_for_that_many_last_days = 5
            for number_of_touches_by_low in range(2,so_many_number_of_touches_of_level_by_lows+1):
                try:
                    if number_of_touches_by_low in \
                            data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_lows"].values:
                        counter_for_usdt_pair_with_low = counter_for_usdt_pair_with_low + 1
                        print ( "counter_for_usdt_pair_with_low=",
                                counter_for_usdt_pair_with_low)
                        # list_of_pairs_and_exchanges_with_touches_of_level_by_low.append (
                        #     f"{usdt_pair} on {exchange}" )
                        #print ( data_df_slice_plus_number_of_equal_lows_and_highs.to_string () )
                        print (f"in trading pair {usdt_pair} on {exchange} the number"
                               f" of touches by lows is equal to {number_of_touches_by_low}")
                        print ( "data_df_slice_plus_number_of_equal_lows_and_highs\n" )
                        print ( data_df_slice_plus_number_of_equal_lows_and_highs.to_string () )

                        for row in range(0,len(data_df_slice_plus_number_of_equal_lows_and_highs)) :
                            if data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_lows"].iloc[row]>1:
                                print (
                                    'data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_lows"].iloc[row]\n' )
                                print ( data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_lows"].iloc[
                                            row] )
                                data_df_slice_plus_number_of_equal_lows_and_highs["low_level"].iat[row]=\
                                    data_df_slice_plus_number_of_equal_lows_and_highs["low"].iat[row]
                                print(data_df_slice_plus_number_of_equal_lows_and_highs)

                            else:
                                data_df_slice_plus_number_of_equal_lows_and_highs["low_level"].iat[row] = np.nan



                        pass
                except Exception as e:
                    print(f"error with {usdt_pair} on {exchange}",e)

            for number_of_touches_by_high in range ( 2 , so_many_number_of_touches_of_level_by_highs + 1 ):
                try:
                    if number_of_touches_by_high in \
                            data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_highs"].values:
                        counter_for_usdt_pair_with_high = counter_for_usdt_pair_with_high + 1
                        print ( "counter_for_usdt_pair_with_high=" ,
                                counter_for_usdt_pair_with_high )
                        print ( f"in trading pair {usdt_pair} on {exchange} the number"
                                f" of touches by highs is equal to {number_of_touches_by_high}" )
                        # list_of_pairs_and_exchanges_with_touches_of_level_by_high.append(f"{usdt_pair} on {exchange}")
                        # print ( "data_df_slice_plus_number_of_equal_lows_and_highs\n" )
                        # print ( data_df_slice_plus_number_of_equal_lows_and_highs.to_string () )
                        #counter_for_usdt_pair_with_high=counter_for_usdt_pair_with_high+1
                        #data_df_slice_plus_number_of_equal_lows_and_highs["high_level"] =  np.nan

                        for row in range ( 0 , len ( data_df_slice_plus_number_of_equal_lows_and_highs ) ):
                            if data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_highs"].iloc[row] > 1:
                                # print('data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_highs"].iloc[row]\n')
                                # print(data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_highs"].iloc[row])
                                data_df_slice_plus_number_of_equal_lows_and_highs["high_level"].iat[row] = \
                                    data_df_slice_plus_number_of_equal_lows_and_highs["high"].iat[row]
                                # print ( data_df_slice_plus_number_of_equal_lows_and_highs.to_string () )

                                # print ( "data_df_slice_plus_number_of_equal_lows_and_highs_outside\n" )
                                # print ( data_df_slice_plus_number_of_equal_lows_and_highs.to_string () )
                                if row==len ( data_df_slice_plus_number_of_equal_lows_and_highs )-1:

                                    print ( "data_df \n" , data_df )
                                    print ( "data_df_slice_plus_number_of_equal_lows_and_highs_outside\n" )
                                    print ( data_df_slice_plus_number_of_equal_lows_and_highs.to_string () )

                                    data_df_slice_plus_number_of_equal_lows_without_NaNs=\
                                        data_df_slice_plus_number_of_equal_lows_and_highs.dropna(subset=
                                                                                             ["low_level"],
                                                                                             how="all")

                                    data_df_slice_plus_number_of_equal_highs_without_NaNs = \
                                        data_df_slice_plus_number_of_equal_lows_and_highs.dropna ( subset =
                                                                                                   ["high_level"] ,
                                                                                                   how = "all" )

                                    dict_of_lows_legit_or_not_legit={}
                                    dict_of_highs_legit_or_not_legit = {}
                                    for row_of_low_levels in range(len(data_df_slice_plus_number_of_equal_lows_without_NaNs)):
                                        low_level=\
                                            data_df_slice_plus_number_of_equal_lows_without_NaNs["low_level"].iat[row_of_low_levels]
                                        timestamp_for_low_level=\
                                            data_df_slice_plus_number_of_equal_lows_without_NaNs["Timestamp"].iat[row_of_low_levels]
                                        row_from_data_df_with_low=data_df[data_df["Timestamp"]==timestamp_for_low_level]
                                        try:
                                            current_index_of_low=row_from_data_df_with_low.index.item()
                                            print ( "row_from_data_df_with_low\n" ,
                                                    row_from_data_df_with_low.to_string () )
                                            print ( "current_index_of_low\n" , current_index_of_low )
                                            prev_index_of_low=current_index_of_low-1
                                            next_index_of_low = current_index_of_low +1
                                            last_index_of_data_df=data_df.tail(1).index.item()
                                            first_index_of_data_df = data_df.head ( 1 ).index.item ()


                                            if not (next_index_of_low>last_index_of_data_df or\
                                                    prev_index_of_low<first_index_of_data_df):
                                                prev_low = data_df["low"].iat[prev_index_of_low]
                                                current_low = data_df["low"].iat[current_index_of_low]


                                                next_low = data_df["low"].iat[next_index_of_low]
                                                print(f"found non boundary low {current_low} "
                                                      f"for {usdt_pair} on {exchange}")
                                                print ( "first_index_of_data_df=",first_index_of_data_df)
                                                print ( "last_index_of_data_df=" , last_index_of_data_df )
                                                print ( "prev_index_of_low=" , prev_index_of_low )
                                                print ( "next_index_of_low=" , next_index_of_low )

                                                ################################
                                                #################################
                                                #################################




                                                if (prev_low>=current_low) and (current_low<=next_low):
                                                    print(f"{current_low} of {usdt_pair} on {exchange} is partly legit")
                                                    if current_low not in dict_of_lows_legit_or_not_legit:
                                                        dict_of_lows_legit_or_not_legit[current_low]=[f'legit_at_{timestamp_for_low_level}']
                                                    else:
                                                        dict_of_lows_legit_or_not_legit[current_low].append(f'legit_at_{timestamp_for_low_level}')
                                                else:
                                                    print (
                                                        f"{current_low} of {usdt_pair} on {exchange} is partly not legit" )
                                                    if current_low not in dict_of_lows_legit_or_not_legit:
                                                        dict_of_lows_legit_or_not_legit[current_low] = [f'not_legit_at_{timestamp_for_low_level}']
                                                    else:
                                                        dict_of_lows_legit_or_not_legit[current_low].append ( f'not_legit_at_{timestamp_for_low_level}' )



                                        except Exception as e:
                                            print ("error=",e)
                                            pass
                                    print ( "dict_of_lows_legit_or_not_legit\n" ,
                                            dict_of_lows_legit_or_not_legit )


                                    for row_of_high_levels in range(len(data_df_slice_plus_number_of_equal_highs_without_NaNs)):
                                        high_level=\
                                            data_df_slice_plus_number_of_equal_highs_without_NaNs["high_level"].iat[row_of_high_levels]
                                        timestamp_for_high_level=\
                                            data_df_slice_plus_number_of_equal_highs_without_NaNs["Timestamp"].iat[row_of_high_levels]
                                        row_from_data_df_with_high=data_df[data_df["Timestamp"]==timestamp_for_high_level]
                                        try:
                                            current_index_of_high=row_from_data_df_with_high.index.item()
                                            print ( "row_from_data_df_with_high\n" ,
                                                    row_from_data_df_with_high.to_string () )
                                            print ( "current_index_of_high\n" , current_index_of_high )
                                            prev_index_of_high=current_index_of_high-1
                                            next_index_of_high = current_index_of_high +1
                                            last_index_of_data_df=data_df.tail(1).index.item()
                                            first_index_of_data_df = data_df.head ( 1 ).index.item ()


                                            if not (next_index_of_high>last_index_of_data_df or\
                                                    prev_index_of_high<first_index_of_data_df):
                                                prev_high = data_df["high"].iat[prev_index_of_high]
                                                current_high = data_df["high"].iat[current_index_of_high]


                                                next_high = data_df["high"].iat[next_index_of_high]
                                                print(f"found non boundary high {current_high} "
                                                      f"for {usdt_pair} on {exchange}")
                                                print ( "first_index_of_data_df=",first_index_of_data_df)
                                                print ( "last_index_of_data_df=" , last_index_of_data_df )
                                                print ( "prev_index_of_high=" , prev_index_of_high )
                                                print ( "next_index_of_high=" , next_index_of_high )

                                                ################################
                                                #################################
                                                #################################




                                                if (prev_high<=current_high) and (current_high>=next_high):
                                                    print(f"{current_high} of {usdt_pair} on {exchange} is partly legit")
                                                    if current_high not in dict_of_highs_legit_or_not_legit:
                                                        dict_of_highs_legit_or_not_legit[current_high]=[f'legit_at_{timestamp_for_high_level}']
                                                    else:
                                                        dict_of_highs_legit_or_not_legit[current_high].append(f'legit_at_{timestamp_for_high_level}')
                                                else:
                                                    print (
                                                        f"{current_high} of {usdt_pair} on {exchange} is partly not legit" )
                                                    if current_high not in dict_of_highs_legit_or_not_legit:
                                                        dict_of_highs_legit_or_not_legit[current_high] = [f'not_legit_at_{timestamp_for_high_level}']
                                                    else:
                                                        dict_of_highs_legit_or_not_legit[current_high].append ( f'not_legit_at_{timestamp_for_high_level}' )

                                                pass

                                        except Exception as e:
                                            print ("error=",e)
                                            pass
                                    print ( "dict_of_highs_legit_or_not_legit\n" ,
                                            dict_of_highs_legit_or_not_legit )





                                    data_df_slice_plus_number_of_equal_highs_without_NaNs = \
                                        data_df_slice_plus_number_of_equal_lows_and_highs.dropna ( subset =
                                                                                                   ["high_level"] ,
                                                                                                   how = "all" )

                                    print ( "data_df_slice_plus_number_of_equal_lows_without_NaNs\n" )
                                    print ( data_df_slice_plus_number_of_equal_lows_without_NaNs.to_string () )

                                    print ( "data_df_slice_plus_number_of_equal_highs_without_NaNs\n" )
                                    print ( data_df_slice_plus_number_of_equal_highs_without_NaNs.to_string () )



                                    boolian_list_if_all_closes_are_lower_than_prev_closes=[]
                                    boolian_list_if_all_closes_are_higher_than_prev_closes = []
                                    for row_backward in range(1,lower_closes_for_that_many_last_days):
                                        print("row_backward=",row_backward)
                                        current_close=data_df_slice_plus_number_of_equal_lows_and_highs["close"].iat[-row_backward]
                                        prev_close = data_df_slice_plus_number_of_equal_lows_and_highs["close"].iat[
                                            -row_backward-1]
                                        # print ("current_close=",current_close)
                                        # print ( "prev_close=" , prev_close )
                                        if current_close<prev_close:
                                            print ("current_close=",current_close)
                                            print ( "prev_close=" , prev_close )
                                            boolian_list_if_all_closes_are_lower_than_prev_closes.append(True)
                                        else:
                                            print("current_close > prev_close")
                                            boolian_list_if_all_closes_are_lower_than_prev_closes.append ( False)


                                        if current_close>prev_close:
                                            print("current_close>=prev_close")
                                            print ("current_close=",current_close)

                                            print ( "prev_close=" , prev_close )  
                                            boolian_list_if_all_closes_are_higher_than_prev_closes.append(True)
                                        else:
                                            print("current_close < prev_close")
                                            boolian_list_if_all_closes_are_higher_than_prev_closes.append ( False )

                                    print("boolian_list_if_all_closes_are_lower_than_prev_closes=",
                                          boolian_list_if_all_closes_are_lower_than_prev_closes)


                                    if_pair_has_all_closes_lower_than_prev_closes=all(
                                        boolian_list_if_all_closes_are_lower_than_prev_closes)
                                    print("if_pair_has_all_closes_lower_than_prev_closes",
                                          if_pair_has_all_closes_lower_than_prev_closes)
                                    # print(f"for trading pair {usdt_pair} on {exchange} closes_are_lower_than_prev_closes",
                                    #       if_pair_has_all_closes_lower_than_prev_closes)
                                    if if_pair_has_all_closes_lower_than_prev_closes:
                                        # print (f"outer for trading pair {usdt_pair} on {exchange} "
                                        #        f"closes_are_lower_than_prev_closes ready for break down" )
                                        if not all(data_df_slice_plus_number_of_equal_lows_and_highs["low_level"].isnull()):
                                            print (
                                                'data_df_slice_plus_number_of_equal_lows_and_highs["low_level"]\n' )
                                            print ( data_df_slice_plus_number_of_equal_lows_and_highs["low_level"] )
                                            print (
                                                'data_df_slice_plus_number_of_equal_lows_and_highs["low_level"].isnull()\n' )
                                            print ( data_df_slice_plus_number_of_equal_lows_and_highs[
                                                        "low_level"].isnull () )
                                            print (
                                                'all(data_df_slice_plus_number_of_equal_lows_and_highs["low_level"].isnull())\n' )
                                            print ( all ( data_df_slice_plus_number_of_equal_lows_and_highs[
                                                              "low_level"].isnull () ) )

                                            print ( f"for trading pair {usdt_pair} on {exchange} "
                                                    f"closes_are_lower_than_prev_closes ready for break down" )


                                    print ( "boolian_list_if_all_closes_are_higher_than_prev_closes=" ,
                                            boolian_list_if_all_closes_are_higher_than_prev_closes )

                                    if_pair_has_all_closes_higher_than_prev_closes = all (
                                        boolian_list_if_all_closes_are_higher_than_prev_closes )

                                    if if_pair_has_all_closes_higher_than_prev_closes:
                                        print (f"outer for trading pair {usdt_pair} on {exchange} "
                                               f"closes_are_higher_than_prev_closes ready for break up" )
                                        if not all(data_df_slice_plus_number_of_equal_lows_and_highs["high_level"].isnull()):
                                            print('data_df_slice_plus_number_of_equal_lows_and_highs["high_level"]\n')
                                            print(data_df_slice_plus_number_of_equal_lows_and_highs["high_level"])
                                            print('data_df_slice_plus_number_of_equal_lows_and_highs["high_level"].isnull()\n')
                                            print(data_df_slice_plus_number_of_equal_lows_and_highs["high_level"].isnull())
                                            print('all(data_df_slice_plus_number_of_equal_lows_and_highs["high_level"].isnull())\n')
                                            print(all(data_df_slice_plus_number_of_equal_lows_and_highs["high_level"].isnull()))

                                            print ( f"for trading pair {usdt_pair} on {exchange} "
                                                    f"closes_are_higher_than_prev_closes ready for break up" )

                                    # print (
                                    #     f"for trading pair {usdt_pair} on {exchange} closes_are_higher_than_prev_closes" ,
                                    #     if_pair_has_all_closes_higher_than_prev_closes )

                                    # if if_pair_has_all_closes_higher_than_prev_closes:
                                    #     print (f"for trading pair {usdt_pair} on {exchange} "
                                    #            f"closes_are_higher_than_prev_closes ready for break up" )


                                    # data_df_slice_plus_number_of_equal_lows_and_highs.dropna(subset=
                                    #                                                          ["low_level","high_level"],
                                    #                                                          how="all",
                                    #                                                          inplace=True)

                                    # print("after_drop_data_df_slice_plus_number_of_equal_lows_and_highs\n",
                                    #       data_df_slice_plus_number_of_equal_lows_and_highs)
                                    set_of_low_levels=\
                                        set(data_df_slice_plus_number_of_equal_lows_and_highs["low_level"].dropna().to_list())
                                    print ( "set_of_low_levels\n" ,
                                            list(set_of_low_levels) )
                                    if len(set_of_low_levels) != 0:
                                        list_of_pairs_and_exchanges_with_touches_of_level_by_low.append (
                                            f"{usdt_pair} on {exchange} with low levels at {set_of_low_levels} " )

                                    set_of_high_levels = \
                                        set ( data_df_slice_plus_number_of_equal_lows_and_highs[
                                                  "high_level"].dropna ().to_list () )
                                    print ( "set_of_high_levels\n" ,
                                            list(set_of_high_levels) )
                                    if len ( set_of_high_levels ) != 0:
                                        list_of_pairs_and_exchanges_with_touches_of_level_by_high.append (
                                            f"{usdt_pair} on {exchange} with high levels at {set_of_high_levels} " )


                            else:
                                data_df_slice_plus_number_of_equal_lows_and_highs["high_level"].iat[row] = np.nan

                except Exception as e:
                    print ( f"error with {usdt_pair} on {exchange}" , e )




                #daily_low = last_several_days_lows_slice_Series.iloc[row_number_in_lows - 1]
                #if daily_high == daily_low:
                    # print ( "daily_low\n" ,
                    #         daily_low )
                    # print ( "daily_high\n" ,
                    #         daily_high )
                # if row_number_in_lows > 1 and row_number_in_lows < last_several_days_lows_slice_Series.size:
                #     print ( "found not boundary low" )
                #
                # if row_number_in_highs > 1 and row_number_in_highs < last_several_days_highs_slice_Series.size:
                #     print ( "found not boundary high" )
                #     if row_number_in_lows > 1 and row_number_in_lows < last_several_days_lows_slice_Series.size:
                #         prev_daily_high = last_several_days_highs_slice_Series.iloc[row_number_in_highs - 2]
                #         next_daily_high = last_several_days_highs_slice_Series.iloc[row_number_in_highs]
                #         prev_daily_low = last_several_days_lows_slice_Series.iloc[
                #             row_number_in_lows - 2]
                #         next_daily_low = last_several_days_lows_slice_Series.iloc[row_number_in_lows]
                #
                #         #print("last_several_days_lows_slice_Series.iloc[row_number_in_lows]=\n",
                #         #      last_several_days_lows_slice_Series.iloc[row_number_in_lows])
                #
                #         # print ( "prev_daily_high\n" , prev_daily_high )
                #         # print ( "next_daily_high\n" , next_daily_high )
                #         # print ( "prev_daily_low\n" , prev_daily_low )
                #         # print ( "next_daily_low\n" , next_daily_low )
                #         if prev_daily_low > daily_low and next_daily_low > daily_low:
                #             print ( "found legit low" )
                #
                #         if prev_daily_high < daily_high and next_daily_high < daily_high:
                #             print ( "found legit high" )
                #             if prev_daily_low > daily_low and next_daily_low > daily_low:
                #                 print ( "level is legit\n" )
                #
                #                 #print ( "last_several_days_lows_slice_Series\n" ,
                #                 #        last_several_days_lows_slice_Series )
                #
                #                 list_of_tuples_of_lows = list ( last_several_days_lows_slice_Series.items () )
                #                 #print ( "list_of_tuples_of_lows\n",list_of_tuples_of_lows )
                #                 tuple_of_legit_low_level = list_of_tuples_of_lows[row_number_in_lows - 1]
                #                 #print ( "tuple_of_legit_low_level\n" ,
                #                 #        tuple_of_legit_low_level )
                #
                #                 list_of_tuples_of_highs = list (
                #                     last_several_days_highs_slice_Series.items () )
                #                 #print ( "list_of_tuples_of_highs\n" , list_of_tuples_of_highs )
                #                 tuple_of_legit_high_level = list_of_tuples_of_highs[
                #                     row_number_in_highs - 1]
                #                 #print ("tuple_of_legit_high_level\n", tuple_of_legit_high_level)
                #
                #                 # print ( "dt.datetime.fromtimestamp ( tuple_of_legit_low_level[0] )\n" ,
                #                 #         dt.datetime.fromtimestamp ( tuple_of_legit_low_level[0] / 1000.0 ) )
                #                 #
                #                 # print ( "dt.datetime.fromtimestamp ( tuple_of_legit_high_level[0] )" ,
                #                 #         dt.datetime.fromtimestamp ( tuple_of_legit_high_level[0] / 1000.0 ) )
                #
                #                 mirror_level_df.loc[0 , 'USDT_pair'] = usdt_pair
                #                 mirror_level_df.loc[0 , 'exchange'] = exchange
                #                 mirror_level_df.loc[0 , 'mirror_level'] = daily_low
                #                 mirror_level_df.loc[0 , 'timestamp_for_low'] = \
                #                     tuple_of_legit_low_level[0]
                #                 mirror_level_df.loc[0 , 'timestamp_for_high'] = \
                #                     tuple_of_legit_high_level[0]
                #                 mirror_level_df.loc[0 , 'low'] = daily_low
                #                 mirror_level_df.loc[0 , 'high'] = daily_high
                #                 mirror_level_df.loc[0 , 'open_time_of_candle_with_legit_low'] = \
                #                     dt.datetime.fromtimestamp ( tuple_of_legit_low_level[0] / 1000.0 )
                #                 mirror_level_df.loc[0 , 'open_time_of_candle_with_legit_high'] = \
                #                     dt.datetime.fromtimestamp ( tuple_of_legit_high_level[0] / 1000.0 )
                #                 mirror_level_df.loc[0 , 'average_volume'] = average_volume
                #
                #                 print ( 'mirror_level_df\n' , mirror_level_df )
                #
                #
                #
                #                 mirror_level_df.to_sql ( "mirror_levels_calculated_separately" ,
                #                                          connection_to_btc_and_usdt_trading_pairs ,
                #                                          if_exists = 'append' , index = False )
                #
                # pass
        except Exception as e:
            print(f"problem with {table_in_db}", e)
            traceback.print_exc ()
            continue
    print("list_of_pairs_and_exchanges_with_touches_of_level_by_low\n",
          list_of_pairs_and_exchanges_with_touches_of_level_by_low)
    print ( "list_of_pairs_and_exchanges_with_touches_of_level_by_high\n" ,
            list_of_pairs_and_exchanges_with_touches_of_level_by_high )

    print ( "len(list_of_pairs_and_exchanges_with_touches_of_level_by_low)\n" ,
            len(list_of_pairs_and_exchanges_with_touches_of_level_by_low) )
    print ( "len(list_of_pairs_and_exchanges_with_touches_of_level_by_high)\n" ,
            len(list_of_pairs_and_exchanges_with_touches_of_level_by_high) )

    print ( "not_recent_pair_counter=" , not_recent_pair_counter )

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
    async_var=True
    find_asc_desc_triangles_in_database(async_var)