import pandas as pd
import os
import asyncio
import time
import datetime
import traceback
import datetime as dt
import tzlocal
import ccxt.async_support as ccxt
from sqlalchemy_utils import create_database,database_exists
import db_config
# from sqlalchemy import MetaData
from sqlalchemy import inspect
import logging
from sqlalchemy import MetaData
from sqlalchemy import create_engine

def drop_table(table_name,engine):
    engine.execute (
        f"DROP TABLE IF EXISTS {table_name};" )

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

async def fetch_lower_timeframe_ohlcv_data(usdt_pair,
                                    exchange,
                                    mirror_level,
                                    timestamp_for_low,
                                    timestamp_for_high,
                                    open_time_of_low,
                                    open_time_of_high,
                                    connection_to_usdt_trading_pairs_ohlcv_mirror_levels_some_timeframe,
                                    connection_to_usdt_trading_pairs_ohlcv,
                                    lower_timeframe_for_mirror_level_rebound_trading='1h'):



    exchange_object = getattr ( ccxt , exchange ) ()
    exchange_object.enableRateLimit = True
    try:

        await exchange_object.load_markets ()
        #await asyncio.sleep ( exchange_object.rateLimit / 1000 )
        #earliest_timestamp = exchange_object.milliseconds ()
        #await asyncio.sleep ( exchange_object.rateLimit / 1000 )
        #timeframe_duration_in_seconds = exchange_object.parse_timeframe ( lower_timeframe_for_mirror_level_rebound_trading )
        #timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
        #how_many_bars_before_mirror_level_occured_to_collect_on_lower_timeframe=20
        # timedelta=timeframe_duration_in_ms*\
        #           how_many_bars_before_mirror_level_occured_to_collect_on_lower_timeframe
        # print("exchange_object.rateLimit")
        # print ( exchange_object.rateLimit )
        # await asyncio.sleep ( exchange_object.rateLimit/1000 )
        #
        await asyncio.sleep ( exchange_object.rateLimit/1000 )


        data = await exchange_object.fetch_ohlcv ( usdt_pair ,
                                                   lower_timeframe_for_mirror_level_rebound_trading
                                                   #since=timestamp_for_low-timedelta
                                                   )


        header = ['Timestamp' , 'open' , 'high' , 'low' , 'close' , 'volume']
        data_df = pd.DataFrame ( data , columns = header ).set_index ( 'Timestamp' )
        print("data_df")
        print ( data_df )


    except:
        traceback.print_exc()

    finally:
        await exchange_object.close ()



def find_mirror_levels_in_database(lower_timeframe_for_mirror_level_rebound_trading='1h'):
    start_time = time.time ()

    engine_for_usdt_trading_pairs_ohlcv_db , connection_to_usdt_trading_pairs_ohlcv = \
        connect_to_postres_db ( "async_ohlcv_data_for_usdt_trading_pairs" )

    # path_to_db_with_USDT_and_btc_pairs = os.path.join ( os.getcwd () , "datasets" ,
    #                                                     "sql_databases" ,
    #                                                     "btc_and_usdt_pairs_from_all_exchanges.db" )

    # connection_to_usdt_pair_levels_formed_by_high = \
    #     sqlite3.connect ( path_to_db_with_USDT_and_btc_pairs )

    engine_for_btc_and_usdt_trading_pairs ,\
    connection_to_btc_and_usdt_trading_pairs = \
        connect_to_postres_db ( "btc_and_usdt_pairs_from_all_exchanges" )




    try:
        mirror_df = \
            pd.read_sql_query ( f'''select * from mirror_levels_without_duplicates''' ,
                                connection_to_btc_and_usdt_trading_pairs )
        print("mirror_df\n",mirror_df.to_string())
        list_or_tables_with_recent_highs_and_lows_which_coincide_with_mirror_level=[]
        usdt_pair_row_counter=0
        usdt_pair=None
        exchange=None
        tasks=[]
        loop = asyncio.get_event_loop ()
        engine_for_usdt_trading_pairs_ohlcv_mirror_levels_db_some_timeframe , \
        connection_to_usdt_trading_pairs_ohlcv_mirror_levels_some_timeframe = \
            connect_to_postres_db (
                f"ohlcv_{lower_timeframe_for_mirror_level_rebound_trading}_tables_for_each_mirror_level" )

        # connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_some_timeframe = \
        #     connect_to_postres_db (
        #         f"async_ohlcv_data_for_usdt_trading_pairs" )

        for row in range(0,len(mirror_df)):
            try:
                joint_string_for_table_name = mirror_df.loc[row , "USDT_pair"] + '_on_' + mirror_df.loc[
                    row , "exchange"]
                mirror_level=mirror_df.loc(axis=0)[row,"mirror_level"]
                open_time_of_low = mirror_df.loc ( axis = 0 )[row , "open_time_of_candle_with_legit_low"]
                open_time_of_high = mirror_df.loc ( axis = 0 )[row , "open_time_of_candle_with_legit_high"]

                timestamp_for_low = mirror_df.loc ( axis = 0 )[row , "timestamp_for_low"]
                timestamp_for_high = mirror_df.loc ( axis = 0 )[row , "timestamp_for_high"]

                mirror_df_row_slice=mirror_df.iloc[[row]]
                #mirror_df_row_slice.reset_index(drop=True,inplace=True)
                print("type(mirror_df_row_slice)=",type(mirror_df_row_slice))
                print ( "mirror_df_row_slice=\n" , mirror_df_row_slice.to_string()  )
                usdt_pair=mirror_df.loc[row , "USDT_pair"]
                exchange=mirror_df.loc[row , "exchange"]
                usdt_pair_name_and_exchange_df=pd.DataFrame()
                tasks.append(loop.create_task(fetch_lower_timeframe_ohlcv_data(usdt_pair,
                                                 exchange,
                                                 mirror_level,
                                                 timestamp_for_low,
                                                 timestamp_for_high,
                                                 open_time_of_low,
                                                 open_time_of_high,
                                                 connection_to_usdt_trading_pairs_ohlcv_mirror_levels_some_timeframe,
                                                 connection_to_usdt_trading_pairs_ohlcv,
                                                 lower_timeframe_for_mirror_level_rebound_trading)
                                                ))





                print("row=",row)
                # print ( "joint_string_for_table_name=" , joint_string_for_table_name )
                # data_df = \
                #     pd.read_sql_query ( f'''select * from "{joint_string_for_table_name}"''' ,
                #                         connection_to_usdt_trading_pairs_ohlcv )
                # print ( "data_df\n" , data_df )
                print('mirror level=',mirror_level)
                # usdt_pair_recent=None
                # exchange_recent=None
                #data_df.loc[-1,"low"]=mirror_level

            except Exception as e:
                print(f"problem with {usdt_pair} on {exchange}", e)
                traceback.print_exc()
            finally:
                continue

        loop.run_until_complete ( asyncio.wait ( tasks ) )
        loop.close ()
    except Exception as e:
        print (  e )
        traceback.print_exc ()



if __name__=='__main__':
    lower_timeframe_for_mirror_level_rebound_trading = '1h'
    find_mirror_levels_in_database(lower_timeframe_for_mirror_level_rebound_trading)
