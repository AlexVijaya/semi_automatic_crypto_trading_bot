import numpy as np
import pandas as pd
from multiprocessing import Process
import os
import asyncio
import talib
import time
import datetime
import traceback
import datetime as dt
import tzlocal
import multiprocessing
from multiprocessing.pool import Pool
import ccxt
from sqlalchemy_utils import create_database,database_exists
import db_config
# from sqlalchemy import MetaData
from sqlalchemy import inspect
import logging
from sqlalchemy import MetaData
from sqlalchemy import create_engine

def drop_table(table_name,engine):
    engine.execute (
        f'DROP TABLE IF EXISTS "{table_name}";' )

def connect_to_postres_db(database = "btc_and_usdt_pairs_from_all_exchanges_12h"):
    dialect = db_config.dialect
    driver = db_config.driver
    password = db_config.password
    user = db_config.user
    host = db_config.host
    port = db_config.port

    dummy_database = db_config.dummy_database

    engine = create_engine ( f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}" ,
                             isolation_level = 'AUTOCOMMIT' ,
                             echo = True,
                             pool_pre_ping = True,
                             pool_size = 20 , max_overflow = 0,
                             connect_args={'connect_timeout': 10} )
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



def fetch_lower_timeframe_ohlcv_data(usdt_pair,
                                    exchange,
                                    mirror_level,
                                    timestamp_for_low,
                                    timestamp_for_high,
                                    open_time_of_low,
                                    open_time_of_high,
                                    connection_to_usdt_trading_pairs_ohlcv_mirror_levels_with_lower_timeframes,
                                    connection_to_usdt_trading_pairs_ohlcv,
                                    lower_timeframe_for_mirror_level_rebound_trading='1h'):



    exchange_object = getattr ( ccxt , exchange ) ()
    exchange_object.enableRateLimit = True


    try:

        exchange_object.load_markets ()

        earliest_timestamp = exchange_object.milliseconds ()

        # timeframe_duration_in_seconds = exchange_object.parse_timeframe ( lower_timeframe_for_mirror_level_rebound_trading )
        # timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
        # how_many_bars_before_mirror_level_occured_to_collect_on_lower_timeframe=20
        # timedelta=timeframe_duration_in_ms*\
        #           how_many_bars_before_mirror_level_occured_to_collect_on_lower_timeframe
        # print("exchange_object.rateLimit")
        # print ( exchange_object.rateLimit )
        # await asyncio.sleep ( exchange_object.rateLimit/1000 )
        #
        #await asyncio.sleep ( exchange_object.rateLimit/1000 )


        print("timestamp_for_low=",timestamp_for_low)

        print ( "exchange_object.timeframes " )
        lower_timeframe_for_mirror_level_rebound_trading_list=['5m','15m','1h','4h','6h','8h']
        print(exchange_object.timeframes)
        header = ['Timestamp' , 'open' , 'high' , 'low' , 'close' , 'volume']
        final_df=pd.DataFrame(columns = header)
        counter=0
        for lower_timeframe_for_mirror_level_rebound_trading in lower_timeframe_for_mirror_level_rebound_trading_list:
            try:
                timeframe_duration_in_seconds = exchange_object.parse_timeframe (
                    lower_timeframe_for_mirror_level_rebound_trading )
                timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
                how_many_bars_before_mirror_level_occured_to_collect_on_lower_timeframe = 20
                timedelta = timeframe_duration_in_ms * \
                            how_many_bars_before_mirror_level_occured_to_collect_on_lower_timeframe
                print ( "timedelta=" , timedelta )

                data =exchange_object.fetch_ohlcv ( usdt_pair ,
                                                           lower_timeframe_for_mirror_level_rebound_trading,
                                                           since=timestamp_for_low-timedelta
                                                           )

                header = ['Timestamp' , 'open' , 'high' , 'low' ,
                          'close' , 'volume']

                data_df = pd.DataFrame ( data , columns = header )
                data_df['timestamp_for_low'] = np.NAN
                data_df['open_time_for_low'] = np.NAN
                data_df['timestamp_for_high'] = np.NAN
                data_df['open_time_for_high'] = np.NAN

                data_df['timeframe']=lower_timeframe_for_mirror_level_rebound_trading
                data_df['exchange'] = exchange
                data_df['is_low'] = True
                data_df['is_high'] = False
                data_df['mirror_level'] = mirror_level

                for number_of_row in range (0,len(data_df)):
                    if data_df["low"].iat[number_of_row]==mirror_level:

                        data_df['timestamp_for_low'].iat[number_of_row] =\
                            data_df["Timestamp"].iat[number_of_row]
                        data_df['open_time_for_low'].iat[number_of_row]=\
                            dt.datetime.fromtimestamp ( data_df["Timestamp"].iat[number_of_row] / 1000.0 )
                    # else:
                    #     data_df['timestamp_for_low'].iat[number_of_row] =None
                    #     data_df['open_time_for_low'].iat[number_of_row] = None

                for number_of_row in range (0,len(data_df)):
                    if data_df["high"].iat[number_of_row]==mirror_level:

                        data_df['timestamp_for_high'].iat[number_of_row] =\
                            data_df["Timestamp"].iat[number_of_row]
                        data_df['open_time_for_high'].iat[number_of_row]=\
                            dt.datetime.fromtimestamp ( data_df["Timestamp"].iat[number_of_row] / 1000.0 )
                    # else:
                    #     data_df['timestamp_for_low'].iat[number_of_row] =None
                    #     data_df['open_time_for_low'].iat[number_of_row] = None


                data_df['open_time'] = \
                    [dt.datetime.fromtimestamp ( x / 1000.0 ) for x in data_df.Timestamp]
                #data_df['open_time_for_low'] = dt.datetime.fromtimestamp ( timestamp_for_low / 1000.0 )
                data_df['psar'] = talib.SAR ( data_df.high ,
                                              data_df.low ,
                                              acceleration = 0.02 ,
                                              maximum = 0.2 )
                counter=counter+1
                data_df['counter'] = int(counter)
                final_df=pd.concat([final_df,data_df],axis=0)
                #final_df=final_df.append(data_df,ignore_index=False)
                #print(f"{lower_timeframe_for_mirror_level_rebound_trading}_data_df")
                #print ( data_df.to_string() )
            except Exception as e:
                print (e)
                traceback.print_exc()
            finally:
                continue

        for lower_timeframe_for_mirror_level_rebound_trading in lower_timeframe_for_mirror_level_rebound_trading_list:
            try:

                timeframe_duration_in_seconds = exchange_object.parse_timeframe (
                    lower_timeframe_for_mirror_level_rebound_trading )
                timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
                how_many_bars_before_mirror_level_occured_to_collect_on_lower_timeframe = 20
                timedelta = timeframe_duration_in_ms * \
                            how_many_bars_before_mirror_level_occured_to_collect_on_lower_timeframe
                print ( "timedelta=" , timedelta )

                data =exchange_object.fetch_ohlcv ( usdt_pair ,
                                                           lower_timeframe_for_mirror_level_rebound_trading,
                                                           since=timestamp_for_high-timedelta
                                                           )

                header = ['Timestamp' , 'open' , 'high' , 'low' , 'close' , 'volume']
                data_df = pd.DataFrame ( data , columns = header )
                data_df['timeframe'] = lower_timeframe_for_mirror_level_rebound_trading
                data_df['exchange'] = exchange
                data_df['is_low'] = False
                data_df['is_high'] = True
                data_df['mirror_level'] = mirror_level

                data_df['timestamp_for_low'] = np.NAN
                data_df['open_time_for_low'] = np.NAN
                data_df['timestamp_for_high'] = np.NAN
                data_df['open_time_for_high'] = np.NAN



                for number_of_row in range (0,len(data_df)):
                    if data_df["low"].iat[number_of_row]==mirror_level:

                        data_df['timestamp_for_low'].iat[number_of_row] =\
                            data_df["Timestamp"].iat[number_of_row]
                        data_df['open_time_for_low'].iat[number_of_row]=\
                            dt.datetime.fromtimestamp ( data_df["Timestamp"].iat[number_of_row] / 1000.0 )
                    # else:
                    #     data_df['timestamp_for_low'].iat[number_of_row] =None
                    #     data_df['open_time_for_low'].iat[number_of_row] = None

                for number_of_row in range (0,len(data_df)):
                    if data_df["high"].iat[number_of_row]==mirror_level:

                        data_df['timestamp_for_high'].iat[number_of_row] =\
                            data_df["Timestamp"].iat[number_of_row]
                        data_df['open_time_for_high'].iat[number_of_row]=\
                            dt.datetime.fromtimestamp ( data_df["Timestamp"].iat[number_of_row] / 1000.0 )




                #data_df['timestamp_for_high'] = timestamp_for_high
                data_df['open_time'] = \
                    [dt.datetime.fromtimestamp ( x / 1000.0 ) for x in data_df.Timestamp]
                #data_df['open_time_for_high'] = dt.datetime.fromtimestamp ( timestamp_for_high / 1000.0 )
                data_df['psar'] = talib.SAR ( data_df.high ,
                                                  data_df.low ,
                                                  acceleration = 0.02 ,
                                                  maximum = 0.2 )
                counter = counter + 1
                data_df['counter'] = int(counter)
                final_df = pd.concat ( [final_df , data_df] , axis = 0 )

                #final_df=final_df.append ( data_df ,ignore_index=False)
                final_df.to_sql ( f"{usdt_pair}_on_{exchange}_at_{mirror_level}" ,
                                 connection_to_usdt_trading_pairs_ohlcv_mirror_levels_with_lower_timeframes ,
                                 if_exists = 'replace' )
                #print(f"{lower_timeframe_for_mirror_level_rebound_trading}_data_df")
                #print ( data_df.to_string() )
            except Exception as e:
                print (e)
                traceback.print_exc()
            finally:
                continue

        print("final_df")
        print(final_df)


    except:
        traceback.print_exc()

    # finally:
    #     exchange_object.close ()



def find_mirror_levels_in_database():
    start_time = time.time ()


    engine_for_usdt_trading_pairs_ohlcv_db , connection_to_usdt_trading_pairs_ohlcv = \
        connect_to_postres_db ( "ohlcv_data_for_usdt_pairs_for_12h_and_lower_tf" )

    # path_to_db_with_USDT_and_btc_pairs = os.path.join ( os.getcwd () , "datasets" ,
    #                                                     "sql_databases" ,
    #                                                     "btc_and_usdt_pairs_from_all_exchanges.db" )

    # connection_to_usdt_pair_levels_formed_by_high = \
    #     sqlite3.connect ( path_to_db_with_USDT_and_btc_pairs )

    engine_for_btc_and_usdt_trading_pairs ,\
    connection_to_btc_and_usdt_trading_pairs = \
        connect_to_postres_db ( "btc_and_usdt_pairs_from_all_exchanges_12h" )




    try:
        mirror_df = \
            pd.read_sql_query ( f'''select * from mirror_levels_without_duplicates''' ,
                                connection_to_btc_and_usdt_trading_pairs )
        print("mirror_df\n",mirror_df.to_string())
        list_or_tables_with_recent_highs_and_lows_which_coincide_with_mirror_level=[]
        usdt_pair_row_counter=0
        usdt_pair=None
        exchange=None
        # tasks=[]
        # loop = asyncio.get_event_loop ()
        engine_for_usdt_trading_pairs_ohlcv_mirror_levels_db_with_lower_timeframes , \
        connection_to_usdt_trading_pairs_ohlcv_mirror_levels_with_lower_timeframes = \
            connect_to_postres_db (
                f"ohlcv_with_lower_timeframes_tables_for_each_mirror_level_12h" )

        try:
            # here we need to drop 1 db
            list_of_all_1d_tables = []

            list_of_tables_in_1d_ohlcv_db = \
                engine_for_usdt_trading_pairs_ohlcv_mirror_levels_db_with_lower_timeframes. \
                    execute ( "SELECT table_name FROM information_schema.tables WHERE table_schema='public'" )
            list_of_tuples_with_tables_in_1d_ohlcv_db = list ( list_of_tables_in_1d_ohlcv_db )
            for tuple_with_table_name in list_of_tuples_with_tables_in_1d_ohlcv_db:
                list_of_all_1d_tables.append ( tuple_with_table_name[0] )

            print ( "list_of_all_1d_tables\n" )
            print ( list_of_all_1d_tables )

            for table_name in list_of_all_1d_tables:
                try:
                    # table_name=table_name.replace("/","_")
                    drop_table ( table_name ,
                                 engine_for_usdt_trading_pairs_ohlcv_mirror_levels_db_with_lower_timeframes )

                except Exception as e:
                    print ( f"can't drop table {table_name}" , e )

        except:
            pass


        # connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_some_timeframe = \
        #     connect_to_postres_db (
        #         f"async_ohlcv_data_for_usdt_trading_pairs" )
        #global fetch_lower_timeframe_ohlcv_data
        procs = []
        how_many_processes_to_spawn=15
        for i in range(0,len(mirror_df),how_many_processes_to_spawn):

            for row in range(i,i+how_many_processes_to_spawn):
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
                    #print ( "mirror_df_row_slice=\n" , mirror_df_row_slice  )
                    usdt_pair=mirror_df.loc[row , "USDT_pair"]
                    exchange=mirror_df.loc[row , "exchange"]
                    usdt_pair_name_and_exchange_df=pd.DataFrame()
                    # pool = Pool ( multiprocessing.cpu_count() - 1 )



                        # pool.starmap(fetch_historical_btc_pairs_asynchronously,[(list_of_all_exchanges[0:len ( list_of_all_exchanges )],)])


                    proc = Process(target=fetch_lower_timeframe_ohlcv_data , args=
                               (usdt_pair ,
                                exchange ,
                                mirror_level ,
                                timestamp_for_low ,
                                timestamp_for_high ,
                                open_time_of_low ,
                                open_time_of_high ,
                                connection_to_usdt_trading_pairs_ohlcv_mirror_levels_with_lower_timeframes ,
                                connection_to_usdt_trading_pairs_ohlcv ,
                                lower_timeframe_for_mirror_level_rebound_trading) )
                    proc.start ()
                    procs.append(proc)
                    # fetch_lower_timeframe_ohlcv_data(usdt_pair,
                    #                                  exchange,
                    #                                  mirror_level,
                    #                                  timestamp_for_low,
                    #                                  timestamp_for_high,
                    #                                  open_time_of_low,
                    #                                  open_time_of_high,
                    #                                  connection_to_usdt_trading_pairs_ohlcv_mirror_levels_some_timeframe,
                    #                                  connection_to_usdt_trading_pairs_ohlcv,
                    #                                  lower_timeframe_for_mirror_level_rebound_trading)






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

            for process in procs:
                process.join()
        connection_to_usdt_trading_pairs_ohlcv_mirror_levels_with_lower_timeframes.close()


        # loop.run_until_complete ( asyncio.wait ( tasks ) )
        # loop.close ()
    except Exception as e:
        print (  e )
        traceback.print_exc ()

    connection_to_btc_and_usdt_trading_pairs.close()
    connection_to_usdt_trading_pairs_ohlcv.close()
    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 3600.0 )
    print ( 'overall time=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time=' , start_time )
    print ( 'end_time=' , end_time )



if __name__=='__main__':

    lower_timeframe_for_mirror_level_rebound_trading = '1h'
    find_mirror_levels_in_database()
