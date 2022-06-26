import sqlite3
import pandas as pd
import os
import time
import tzlocal
import datetime
import traceback
import datetime as dt

from collections import Counter

def create_empty_database(path_to_db):
    '''create empty salite db for a given path'''
    conn=None
    try:
        conn=sqlite3.connect(path_to_db)
        print('connection_established')
    except Exception as e:
        print('Exception with creating db\n', e)
    finally:
        if conn:
            conn.close()
    pass


def drop_table_from_database(table_name,path_to_database):
    conn=sqlite3.connect(path_to_database)
    cur=conn.cursor()
    cur.execute('drop table if exists {}'.format(table_name))
    conn.commit()
    conn.close()



def find_if_asset_is_approaching_mirror_level_percentage_wise(async_var):
    start_time = time.time ()

    # open_connection to database with mirror levels
    path_to_db_with_USDT_and_btc_pairs = os.path.join ( os.getcwd () , "datasets" ,
                                                        "sql_databases" ,
                                                        "btc_and_usdt_pairs_from_all_exchanges.db" )

    connection_to_btc_and_usdt_trading_pairs = \
        sqlite3.connect ( path_to_db_with_USDT_and_btc_pairs )

    #connection to async database with ohlcv
    path_to_db_with_async_ohlcv=os.path.join ( os.getcwd () ,
                                         "datasets" ,
                                         "sql_databases" ,
                                         "async_all_exchanges_multiple_tables_historical_data_for_usdt_trading_pairs.db" )

    connection_to_usdt_trading_pairs_ohlcv =sqlite3.connect ( path_to_db_with_async_ohlcv )

    #connection to the new data base with approaching price to mirror level
    path_to_ohlcv_db_with_approaching_price = os.path.join ( os.getcwd () ,
                                              "datasets" ,
                                              "sql_databases" ,
                                              "multiple_ohlcv_tables_with_price_approaching_mirror_level.db" )

    #read one row at a time from df of mirror level from db
    list_of_table_names = []
    try:
        mirror_df = \
            pd.read_sql_query ( f'''select * from mirror_levels_without_duplicates''' ,
                                connection_to_btc_and_usdt_trading_pairs )
        print("mirror_df\n",mirror_df)
        for row in range(0,len(mirror_df)):
            joint_string_for_table_name = mirror_df.loc[row , "USDT_pair"] + \
                                          '_on_' + mirror_df.loc[row , "exchange"] + \
                                          '_at_' + f'{mirror_df.loc[row , "mirror_level"]}'
            print ( "row=" , row )
            print ( "joint_string_for_table_name=" , joint_string_for_table_name )
            list_of_table_names.append ( joint_string_for_table_name )

            pass
    except Exception as e:
        print("problem")
    print ( "list_of_table_names=\n" , list_of_table_names )

    if os.path.exists ( path_to_ohlcv_db_with_approaching_price ):
        os.remove ( path_to_ohlcv_db_with_approaching_price )
    print ( "before removal of db" )
    # time.sleep(20)
    print ( "after removal of db" )
    create_empty_database ( path_to_ohlcv_db_with_approaching_price )
    connection_to_usdt_trading_pairs_ohlcv_with_approaching_price=sqlite3.connect(path_to_ohlcv_db_with_approaching_price)



    counter=0
    for table_in_db_with_ml in list_of_table_names:
        try:
            counter=counter+1

            table_in_db=table_in_db_with_ml.split('_at_')[0]
            print("table_in_db_with_ml=",table_in_db_with_ml)
            mirror_level=float(table_in_db_with_ml.split('_at_')[1])
            print ( "type(mirror_level)=" , type(mirror_level) )

            trading_pair=table_in_db.split("_on_")[0]
            exchange=table_in_db.split ( "_on_" )[1]


            data_df=\
                pd.read_sql_query(f'''select * from "{table_in_db}"''' ,
                                  connection_to_usdt_trading_pairs_ohlcv)

            last_row_df=data_df.tail ( 1 )
            last_close_price=last_row_df.iloc[0]["close"]
            #print("data_df.tail(1)\n",last_row_df)
            #print ( "last_close_price\n" , last_close_price)
            distance_percent=100.0*abs(mirror_level-last_close_price)/last_close_price
            #print("distance_percent=",distance_percent)
            allowed_distance=2.0
            #print("type(allowed_distance)=",type(allowed_distance))
            if distance_percent < allowed_distance:
                print(f"distance in {trading_pair} on {exchange} "
                      f"is less than {allowed_distance}")

                data_df.to_sql ( f"{trading_pair}_on_{exchange}_at_{mirror_level}" ,
                                 connection_to_usdt_trading_pairs_ohlcv_with_approaching_price ,
                                 if_exists = 'replace' )
            # data_df.to_sql(f'''{table_in_db}''',connection_to_test_db)

            print(f"mirror_level_for_{table_in_db}_is", mirror_level)

            print ( f"\ndata_df_for_{table_in_db}\n",data_df )

            #check if last close price in db with ohlcv is near the mirror level
            last_row_in_df=data_df.tail(1)
            print ( "last_row_in_df\n" , last_row_in_df )
            #data_df.set_index("Timestamp")
            #print ( "data_df\n" , data_df )
            print("---------------------------")
            print(f'{table_in_db} is this number {counter} out of {len(list_of_table_names)}\n' )

            usdt_pair=data_df.loc[0,"trading_pair"]
            exchange=data_df.loc[0,"exchange"]

            #get data frame with ohlcv
            data_df.set_index("Timestamp", inplace = True)
            pair_and_exchange_from_data_df=usdt_pair+exchange

        except Exception as e:
            print(f"problem with {table_in_db_with_ml}", e)
            traceback.print_exc ()
            continue




    connection_to_usdt_trading_pairs_ohlcv_with_approaching_price.close()
    connection_to_usdt_trading_pairs_ohlcv.close()
    connection_to_btc_and_usdt_trading_pairs.close()
    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 3600.0 )
    print ( 'overall time=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time=' , start_time )
    print ( 'end_time=' , end_time )
    unix_timestamp_start = float ( start_time )
    unix_timestamp_end = float ( end_time )
    local_timezone = tzlocal.get_localzone ()  # get pytz timezone
    local_time_start = dt.datetime.fromtimestamp ( unix_timestamp_start , local_timezone )
    local_time_end = dt.datetime.fromtimestamp ( unix_timestamp_end , local_timezone )
    print ( 'local_time_start=' , local_time_start )
    print ( 'local_time_end=' , local_time_end )
if __name__=="__main__":
    async_var=True
    find_if_asset_is_approaching_mirror_level_percentage_wise(async_var)
