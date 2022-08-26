import sqlite3
import pandas as pd
import os
import time
import tzlocal
import datetime
import traceback
import datetime as dt
import shutil
from ta.volatility import KeltnerChannel
#from drop_table_from_database import drop_table_from_database


import db_config
# from sqlalchemy import MetaData
from sqlalchemy import inspect
import logging
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import create_database,database_exists

def drop_table(table_name,engine):
    engine.execute (
        f"DROP TABLE IF EXISTS {table_name};" )
    # base = declarative_base()
    # metadata = MetaData(engine)
    #
    # table = metadata.tables.get(table_name)
    # if table is not None:
    #     logging.info(f'Deleting {table_name} table')
    #     base.metadata.drop_all(engine, [table], checkfirst=True)




def connect_to_postres_db(database = "btc_and_usdt_pairs_from_all_exchanges"):
    dialect = db_config.dialect
    driver = db_config.driver
    password = db_config.password
    user = db_config.user
    host = db_config.host
    port = db_config.port

    dummy_database = db_config.dummy_database

    engine = create_engine ( f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}" ,
                             isolation_level = 'AUTOCOMMIT' , echo = True )
    print ( f"{engine} created successfully" )

    # Create database if it does not exist.
    if not database_exists ( engine.url ):
        create_database ( engine.url )
        print ( f'new database created for {engine}' )
        connection=engine.connect ()
        print ( f'Connection to {engine} established after creating new database' )

    connection = engine.connect ()

    print ( f'Connection to {engine} established. Database already existed.'
            f' So no new db was created' )
    return engine , connection


# def create_empty_database(path_to_db):
#     '''create empty salite db for a given path'''
#     conn=None
#     try:
#         conn=sqlite3.connect(path_to_db)
#         print('connection_established')
#     except Exception as e:
#         print('Exception with creating db\n', e)
#     finally:
#         if conn:
#             conn.close()
#     pass


# def drop_table_from_database(table_name,path_to_database):
#     conn=sqlite3.connect(path_to_database)
#     cur=conn.cursor()
#     cur.execute('drop table if exists {}'.format(table_name))
#     conn.commit()
#     conn.close()

# def calculate_keltner_channels(df):
#     # Initialize Keltner Channel Indictor
#     indicator_keltner = KeltnerChannel(high=df['high'], low=df['low'], close=df["close"], window=20)
#     # Add Keltner Channel features
#     df['keltner_mband'] = indicator_keltner.keltner_channel_mband()
#     df['keltner_hband'] = indicator_keltner.keltner_channel_hband()
#     df['keltner_lband'] = indicator_keltner.keltner_channel_lband()
#     return df
#


def find_if_high_or_low_yesterday_coincides_with_mirror_level(data_df,
                                                              mirror_level_df,
                                                              usdt_pair,
                                                              exchange,
                                                              row):

    #print('data_df_from_func\n',data_df)


    #find if yesterday or today high or low equals mirror level
    last_2_days_slice_of_highs = data_df['high'].tail ( 2 )
    last_2_days_slice_of_lows = data_df['low'].tail ( 2 )
    print ( "last_2_days_slice_of_highs=\n" , last_2_days_slice_of_highs )
    print ( "last_2_days_slice_of_lows=\n" , last_2_days_slice_of_lows )
    print("mirror_level_df_from_func\n",mirror_level_df.to_string())
    mirror_level_df.set_index("index", inplace=True)
    print ( 'mirror_level_df.loc[0 , "mirror_level"]\n' , mirror_level_df.loc[row, "mirror_level"] )
    mirror_level=mirror_level_df.loc[row , "mirror_level"]
    print("last_2_days_slice_of_highs.iloc[-2]=",last_2_days_slice_of_highs.iloc[-2])

    if (last_2_days_slice_of_highs.iloc[-2] == mirror_level_df.loc[row, "mirror_level"]):
        print ( "last_2_days_slice_of_highs.iloc[-2]=" , last_2_days_slice_of_highs.iloc[-2] )
        print ( f'{usdt_pair} on {exchange} had a high at '
                f'{last_2_days_slice_of_highs.iloc[-2]} which is equal to mirror level '
                f'{mirror_level_df.loc[row, "mirror_level"]}' )

        return usdt_pair,exchange
        #time.sleep ( 10 )

    if (last_2_days_slice_of_highs.iloc[-1] == mirror_level_df.loc[row, "mirror_level"]):
        print ( "last_2_days_slice_of_highs.iloc[-1]=" , last_2_days_slice_of_highs.iloc[-1] )
        print ( f'{usdt_pair} on {exchange} had a high at '
                f'{last_2_days_slice_of_highs.iloc[-1]} which is equal to mirror level '
                f'{mirror_level_df.loc[row, "mirror_level"]}' )
        return usdt_pair , exchange
        #time.sleep ( 10 )

    if (last_2_days_slice_of_lows.iloc[-2] == mirror_level_df.loc[row, "mirror_level"]):
        print ( "last_2_days_slice_of_lows.iloc[-2]=" , last_2_days_slice_of_lows.iloc[-2] )
        print ( f'{usdt_pair} on {exchange} had a low at '
                f'{last_2_days_slice_of_lows.iloc[-2]} which is equal to mirror level '
                f'{mirror_level_df.loc[row, "mirror_level"]}' )
        return usdt_pair , exchange
        #time.sleep ( 10 )

    if (last_2_days_slice_of_lows.iloc[-1] == mirror_level_df.loc[row, "mirror_level"]):
        print ( "last_2_days_slice_of_lows.iloc[-1]=" , last_2_days_slice_of_lows.iloc[-1] )
        print ( f'{usdt_pair} on {exchange} had a low at '
                f'{last_2_days_slice_of_lows.iloc[-1]} which is equal to mirror level '
                f'{mirror_level_df.loc[row, "mirror_level"]} ' )
        return usdt_pair , exchange

    return None, None
        #time.sleep ( 10 )
    #

def find_mirror_levels_in_database():
    start_time = time.time ()

    engine_for_usdt_trading_pairs_ohlcv_db , connection_to_usdt_trading_pairs_ohlcv = \
        connect_to_postres_db ( "async_ohlcv_data_for_usdt_trading_pairs" )

    # path_to_db_with_USDT_and_btc_pairs = os.path.join ( os.getcwd () , "datasets" ,
    #                                                     "sql_databases" ,
    #                                                     "btc_and_usdt_pairs_from_all_exchanges.db" )

    # connection_to_usdt_pair_levels_formed_by_high = \
    #     sqlite3.connect ( path_to_db_with_USDT_and_btc_pairs )

    engine_for_btc_and_usdt_trading_pairs_levels_formed_by_high_db , connection_to_btc_and_usdt_trading_pairs = \
        connect_to_postres_db ( "btc_and_usdt_pairs_from_all_exchanges" )

    try:
        drop_table ( "usdt_pairs_with_recent_low_or_high_equal_to_mirror_level" ,
                                   engine_for_btc_and_usdt_trading_pairs_levels_formed_by_high_db )

        print ("\ntable dropped\n")
        #time.sleep ( 1000 )
    except Exception as e:
        print ( "cant drop table from db\n",e )
        #time.sleep(1000)


    try:
        mirror_df = \
            pd.read_sql_query ( f'''select * from mirror_levels_without_duplicates''' ,
                                connection_to_btc_and_usdt_trading_pairs )
        print("mirror_df\n",mirror_df.to_string())
        list_or_tables_with_recent_highs_and_lows_which_coincide_with_mirror_level=[]
        usdt_pair_row_counter=0
        usdt_pair=None
        exchange=None
        for row in range(0,len(mirror_df)):
            try:
                joint_string_for_table_name=mirror_df.loc[row,"USDT_pair"]+'_on_'+mirror_df.loc[row,"exchange"]
                mirror_level=mirror_df.loc(axis=0)[row,"mirror_level"]
                open_time_of_low = mirror_df.loc ( axis = 0 )[row , "open_time_of_candle_with_legit_low"]
                open_time_of_high = mirror_df.loc ( axis = 0 )[row , "open_time_of_candle_with_legit_high"]

                mirror_df_row_slice=mirror_df.iloc[[row]]
                #mirror_df_row_slice.reset_index(drop=True,inplace=True)
                print("type(mirror_df_row_slice)=",type(mirror_df_row_slice))
                print ( "mirror_df_row_slice=\n" , mirror_df_row_slice.to_string()  )
                usdt_pair=mirror_df.loc[row , "USDT_pair"]
                exchange=mirror_df.loc[row , "exchange"]
                usdt_pair_name_and_exchange_df=pd.DataFrame()


                print("row=",row)
                print ( "joint_string_for_table_name=" , joint_string_for_table_name )
                data_df = \
                    pd.read_sql_query ( f'''select * from "{joint_string_for_table_name}"''' ,
                                        connection_to_usdt_trading_pairs_ohlcv )
                print ( "data_df\n" , data_df )
                print('mirror level=',mirror_level)
                # usdt_pair_recent=None
                # exchange_recent=None
                #data_df.loc[-1,"low"]=mirror_level

                usdt_pair_recent, exchange_recent=\
                    find_if_high_or_low_yesterday_coincides_with_mirror_level ( data_df ,
                                                                            mirror_df_row_slice ,
                                                                            usdt_pair ,
                                                                            exchange,
                                                                            row)
                if usdt_pair_recent==None:
                    continue
                usdt_pair_row_counter=usdt_pair_row_counter+1
                print ( "usdt_pair_recent\n" , usdt_pair_recent )
                #time.sleep ( 1000 )

                last_date_with_time = data_df["open_time"].iloc[-1]
                last_date_with_time = last_date_with_time.strftime ( "%Y/%m/%d %H:%M:%S" )
                print ( "type(last_date_with_time)\n" , type ( last_date_with_time ) )
                print ( "last_date_with_time\n" , last_date_with_time )
                last_date_without_time = last_date_with_time.split ( " " )
                print ( "last_date_with_time\n" , last_date_without_time[0] )
                last_date_without_time = last_date_without_time[0]


                #add names of usdt pairs and exchanges to a data base
                usdt_pair_name_and_exchange_df.loc[usdt_pair_row_counter-1,"usdt_pair"]=usdt_pair_recent
                usdt_pair_name_and_exchange_df.loc[usdt_pair_row_counter-1,"exchange"] = exchange_recent
                usdt_pair_name_and_exchange_df.loc[usdt_pair_row_counter - 1 , "next_date_after_low_or_high_coincided_with_mirror_level"] = last_date_without_time
                usdt_pair_name_and_exchange_df.loc[
                    usdt_pair_row_counter - 1 , "mirror_level"] = mirror_level
                usdt_pair_name_and_exchange_df.loc[
                    usdt_pair_row_counter - 1 , "open_time_of_low"] = open_time_of_low
                usdt_pair_name_and_exchange_df.loc[
                    usdt_pair_row_counter - 1 , "open_time_of_high"] = open_time_of_high

                usdt_pair_name_and_exchange_df.loc[
                    usdt_pair_row_counter - 1 , "table_name"] = joint_string_for_table_name
                # usdt_pair_name_and_exchange_df.loc[
                #     usdt_pair_row_counter - 1 , "table_name1"] = joint_string_for_table_name
                print("usdt_pair_name_and_exchange_df\n",usdt_pair_name_and_exchange_df)
                #time.sleep(1000)
                usdt_pair_name_and_exchange_df.to_sql ( '''usdt_pairs_with_recent_low_or_high_equal_to_mirror_level''' ,
                                 connection_to_btc_and_usdt_trading_pairs ,
                                 if_exists = "append" )




                recent_string_for_table=f"{usdt_pair_recent}_on_{exchange_recent}"
                list_or_tables_with_recent_highs_and_lows_which_coincide_with_mirror_level.append(recent_string_for_table)

            except Exception as e:
                print(f"problem with {usdt_pair} on {exchange}", e)
                traceback.print_exc()
            finally:
                continue


        print("list_or_tables_with_recent_highs_and_lows_which_coincide_with_mirror_level\n",
              list_or_tables_with_recent_highs_and_lows_which_coincide_with_mirror_level)
        #time.sleep(30000)

        engine_for_usdt_trading_pairs_ohlcv_db , connection_to_usdt_trading_pairs_ohlcv = \
            connect_to_postres_db ( "async_ohlcv_data_for_usdt_trading_pairs" )

        engine_for_usdt_trading_pairs_ohlcv_yesterday_high_or_low_db , connection_to_usdt_trading_pairs_ohlcv_yesterday_high_or_low = \
            connect_to_postres_db ( "many_ohlcv_tables_yesterdays_high_or_low_equal_to_mirror_level" )

        counter = 0
        for recent_table_name in list_or_tables_with_recent_highs_and_lows_which_coincide_with_mirror_level:
            try:
                counter = counter + 1
                data_df = \
                    pd.read_sql_query ( f'''select * from "{recent_table_name}"''' ,
                                        connection_to_usdt_trading_pairs_ohlcv )

                print ( "---------------------------" )
                print ( f'{recent_table_name} is number {counter} out'
                        f' of {len ( list_or_tables_with_recent_highs_and_lows_which_coincide_with_mirror_level )}\n' )
                data_df.set_index ( "Timestamp" , inplace = True )
                #data_df = calculate_keltner_channels ( data_df )

                data_df.to_sql ( f'''{recent_table_name}''' ,
                                 connection_to_usdt_trading_pairs_ohlcv_yesterday_high_or_low ,
                                 if_exists = "replace" )


            except Exception as e:
                print ( f"problem with {recent_table_name}" , e )
                traceback.print_exc ()
                continue




    except Exception as e:
        print(e)
        traceback.print_exc()




    connection_to_usdt_trading_pairs_ohlcv.close()
    connection_to_btc_and_usdt_trading_pairs.close()
    #connection_to_usdt_trading_pairs_ohlcv_yesterday_high_or_low.close()
    #connection_to_usdt_trading_pairs_ohlcv_with_kc.close()
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

    find_mirror_levels_in_database()
