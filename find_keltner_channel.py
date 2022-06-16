import sqlite3
import pandas as pd
import os
import time
import tzlocal
import datetime
import traceback
import datetime as dt
from ta.volatility import KeltnerChannel

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

def calculate_keltner_channels(df):
    # Initialize Keltner Channel Indictor
    indicator_keltner = KeltnerChannel(high=df['high'], low=df['low'], close=df["close"], window=20)
    # Add Keltner Channel features
    df['keltner_mband'] = indicator_keltner.keltner_channel_mband()
    df['keltner_hband'] = indicator_keltner.keltner_channel_hband()
    df['keltner_lband'] = indicator_keltner.keltner_channel_lband()
    return df


def find_mirror_levels_in_database_and_add_kc_to_db(async_var):
    start_time = time.time ()
    connection_to_usdt_trading_pairs_ohlcv = \
        sqlite3.connect ( os.path.join ( os.getcwd () ,
                                         "datasets" ,
                                         "sql_databases" ,
                                         "all_exchanges_multiple_tables_historical_data_for_usdt_trading_pairs.db" ) )

    if async_var==True:
        connection_to_usdt_trading_pairs_ohlcv = \
            sqlite3.connect ( os.path.join ( os.getcwd () ,
                                             "datasets" ,
                                             "sql_databases" ,
                                             "async_all_exchanges_multiple_tables_historical_data_for_usdt_trading_pairs.db" ) )
        print("got into if async_var==True")
    cursor=connection_to_usdt_trading_pairs_ohlcv.cursor()
    cursor.execute ( "SELECT name FROM sqlite_master WHERE type='table';" )

    path_to_ohlcv_db_with_kc= os.path.join ( os.getcwd () ,
                                         "datasets" ,
                                         "sql_databases" ,
                                         "multiple_tables_historical_data_for_usdt_trading_pairs_with_mirror_levels_and_kc.db" )

    if os.path.exists(path_to_ohlcv_db_with_kc):
        os.remove(path_to_ohlcv_db_with_kc)
    print ( "before removal of db" )
    #time.sleep(20)
    print ( "after removal of db" )
    create_empty_database(path_to_ohlcv_db_with_kc)


    connection_to_usdt_trading_pairs_ohlcv_with_kc = \
        sqlite3.connect ( path_to_ohlcv_db_with_kc)
    kc_cursor = connection_to_usdt_trading_pairs_ohlcv_with_kc.cursor ()




    list_of_tables_from_sql_query=cursor.fetchall ()
    list_of_tables=[]
    #open_connection to database with mirror levels
    path_to_db_with_USDT_and_btc_pairs = os.path.join ( os.getcwd () , "datasets" ,
                                                        "sql_databases" ,
                                                        "btc_and_usdt_pairs_from_all_exchanges.db" )

    connection_to_btc_and_usdt_trading_pairs = \
        sqlite3.connect ( path_to_db_with_USDT_and_btc_pairs )

    # path_to_test_db = os.path.join ( os.getcwd () , "datasets" ,
    #                                                     "sql_databases" ,
    #                                                     "test_db.db" )

    # if os.path.exists(path_to_test_db):
    #     os.remove(path_to_test_db)

    # connection_to_test_db = \
    #     sqlite3.connect ( path_to_test_db )

    mirror_level_df = pd.DataFrame ( columns = ['USDT_pair' , 'exchange' , 'mirror_level' ,
                                                'timestamp_for_low' , 'timestamp_for_high' ,
                                                'low' , 'high' , 'open_time_of_candle_with_legit_low' ,
                                                'open_time_of_candle_with_legit_high'] )


    list_of_table_names=[]
    try:
        mirror_df = \
            pd.read_sql_query ( f'''select * from mirror_levels_without_duplicates''' ,
                                connection_to_btc_and_usdt_trading_pairs )
        print("mirror_df\n",mirror_df)
        for row in range(0,len(mirror_df)):
            joint_string_for_table_name=mirror_df.loc[row,"USDT_pair"]+'_on_'+mirror_df.loc[row,"exchange"]
            print("row=",row)
            print ( "joint_string_for_table_name=" , joint_string_for_table_name )
            list_of_table_names.append(joint_string_for_table_name)
        #time.sleep(30000)
        pass
    except Exception as e:
        print(e)
        #time.sleep ( 30000 )
        pass
    print("list_of_table_names=\n",list_of_table_names)
    #time.sleep ( 30000 )
    # try:
    #     drop_table_from_database ( "mirror_levels_calculated_separately" ,
    #                                path_to_db_with_USDT_and_btc_pairs )
    # except:
    #     print ( "cant drop table from db" )

    #
    # for table_in_db in list_of_tables_from_sql_query:
    #     #print(table_in_db[0])
    #     list_of_tables.append(table_in_db[0])

    #print(list_of_tables)
    counter=0
    this_many_last_days=90
    for table_in_db in list_of_table_names:
        try:
            counter=counter+1
            data_df=\
                pd.read_sql_query(f'''select * from "{table_in_db}"''' ,
                                  connection_to_usdt_trading_pairs_ohlcv)
            # data_df.to_sql(f'''{table_in_db}''',connection_to_test_db)
            print ( "++"*80 )
            print ( "++" * 80 )
            print ( "++" * 80 )
            print ( f"data_df_for_{table_in_db}\n",data_df )

            #data_df.set_index("Timestamp")
            #print ( "data_df\n" , data_df )
            print("---------------------------")
            print(f'{table_in_db} is this number {counter} out of {len(list_of_table_names)}\n' )

            usdt_pair=data_df.loc[0,"trading_pair"]
            exchange=data_df.loc[0,"exchange"]

            #get data frame with ohlcv
            data_df.set_index("Timestamp", inplace = True)
            pair_and_exchange_from_data_df=usdt_pair+exchange
            ##############################



            ################################
            data_df_with_kc=calculate_keltner_channels ( data_df )
            # print(data_df_with_kc.columns)
            # print ( "data_df_with_kc.loc[:,'trading_pair'].iloc[0]" ,
            #         data_df_with_kc.loc[:,"trading_pair"].iloc[0] )
            print(f"data_df_with_kc_for_{usdt_pair}_on_{exchange}\n",data_df_with_kc)
            pair_and_exchange_from_data_df_with_kc=\
                data_df_with_kc.loc[:,"trading_pair"].iloc[0]+\
                data_df_with_kc.loc[:,"exchange"].iloc[0]
            print(pair_and_exchange_from_data_df,"\n",
                  pair_and_exchange_from_data_df_with_kc,"\n")

            print(f"counter={counter} ")
            if not (pair_and_exchange_from_data_df == pair_and_exchange_from_data_df_with_kc):
                print(f"pair is {pair_and_exchange_from_data_df_with_kc}")
            data_df_with_kc.to_sql(f'''{table_in_db}''',
                                   connection_to_usdt_trading_pairs_ohlcv_with_kc,
                                   if_exists = "append")


            #print("data_df_with_kc\n",data_df_with_kc )
        except Exception as e:
            print(f"problem with {table_in_db}", e)
            traceback.print_exc ()
            continue
    connection_to_usdt_trading_pairs_ohlcv.close()
    connection_to_btc_and_usdt_trading_pairs.close()
    connection_to_usdt_trading_pairs_ohlcv_with_kc.close()
    # connection_to_test_db.close()
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
    find_mirror_levels_in_database_and_add_kc_to_db(async_var)
