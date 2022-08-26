import os
import traceback

import pandas as pd
import time
import datetime
# import sqlite3
import talib
#import ccxt.async_support as ccxt
import ccxt
import datetime as dt
# from find_keltner_channel import create_empty_database


from sqlalchemy_utils import create_database,database_exists
import db_config
# from sqlalchemy import MetaData
from sqlalchemy import inspect
import logging
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base

def drop_table(table_name,engine):
    engine.execute (
        f'''DROP TABLE IF EXISTS "{table_name}";''' )

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



def check_if_current_bar_closed_within_acceptable_range_to_mirror_level_which_last_was_this_period_ago(lower_timeframe_for_mirror_level_rebound_trading):
    start = time.perf_counter ()
    current_timestamp=time.time()
    #################################
    # path_to_database = os.path.join ( os.getcwd () ,
    #                                   "datasets" ,
    #                                   "sql_databases" ,
    #                                   "multiple_tables_historical_data_for_usdt_trading_pairs_with_mirror_levels_with_highs_or_lows_yesterday.db" )
    # connection_to_usdt_trading_pairs_daily_ohlcv = \
    #     sqlite3.connect ( path_to_database )

    engine_for_usdt_trading_pairs_daily_ohlcv , connection_to_usdt_trading_pairs_daily_ohlcv = \
        connect_to_postres_db ( "many_ohlcv_tables_yesterdays_high_or_low_equal_to_mirror_level" )

    inspector = inspect ( engine_for_usdt_trading_pairs_daily_ohlcv )
    # print(metadata.reflect(engine_for_usdt_trading_pairs_ohlcv_db))
    # print(inspector.get_table_names())


    list_of_table_names_from_ohlcv_with_recent_mirror_highs_and_lows_db = inspector.get_table_names ()


    # list_of_ohlcv_with_recent_mirror_highs_and_lows_with_tuples = cursor.fetchall ()
    list_of_ohlcv_with_recent_mirror_highs_and_lows = []
    # for index , tuple_of_names in enumerate ( list_of_ohlcv_with_recent_mirror_highs_and_lows_with_tuples ):
    #     # print(tuple_of_names[0])
    #     # print(index)
    #
    #     list_of_ohlcv_with_recent_mirror_highs_and_lows.append ( tuple_of_names[0] )
    # print ( "list_of_ohlcv_with_recent_mirror_highs_and_lows\n" ,
    #         list_of_ohlcv_with_recent_mirror_highs_and_lows )


    ##################################
    # path_to_db_with_USDT_and_btc_pairs = os.path.join ( os.getcwd () , "datasets" ,
    #                                                     "sql_databases" ,
    #                                                     "btc_and_usdt_pairs_from_all_exchanges.db" )
    #
    # connection_to_btc_and_usdt_trading_pairs = \
    #     sqlite3.connect ( path_to_db_with_USDT_and_btc_pairs )

    engine_for_btc_and_usdt_trading_pairs_db , connection_to_btc_and_usdt_trading_pairs = \
        connect_to_postres_db ( "btc_and_usdt_pairs_from_all_exchanges" )

    mirror_levels_df = pd.read_sql ( f'''select * from mirror_levels_without_duplicates ;''' ,
                                     connection_to_btc_and_usdt_trading_pairs )
    ###################################3
    # path_to_usdt_trading_pairs_ohlcv_ready_for_rebound = os.path.join ( os.getcwd () ,
    #                                                   "datasets" ,
    #                                                   "sql_databases" ,
    #                                                   "ohlcv_tables_with_pairs_ready_to_rebound_from_mirror_level.db" )
    #
    # if os.path.exists(path_to_usdt_trading_pairs_ohlcv_ready_for_rebound):
    #     os.remove(path_to_usdt_trading_pairs_ohlcv_ready_for_rebound)
    #
    # create_empty_database(path_to_usdt_trading_pairs_ohlcv_ready_for_rebound)
    #
    # connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound = \
    #     sqlite3.connect ( path_to_usdt_trading_pairs_ohlcv_ready_for_rebound )

    engine_for_usdt_trading_pairs_ohlcv_ready_for_rebound_db_some_timeframe ,\
    connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_some_timeframe = \
        connect_to_postres_db ( f"ohlcv_{lower_timeframe_for_mirror_level_rebound_trading}_tables_with_pairs_ready_to_rebound_from_mirror_level" )

    engine_for_usdt_trading_pairs_ohlcv_ready_for_rebound_db_1d_timeframe , \
    connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_1d_timeframe = \
        connect_to_postres_db (
            f"ohlcv_1d_tables_with_pairs_ready_to_rebound_from_mirror_level" )


    #here we need to drop 2 dbs
    list_of_all_1d_tables=[]

    list_of_tables_in_1d_ohlcv_db=\
        engine_for_usdt_trading_pairs_ohlcv_ready_for_rebound_db_1d_timeframe.\
            execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    list_of_tuples_with_tables_in_1d_ohlcv_db=list(list_of_tables_in_1d_ohlcv_db)
    for tuple_with_table_name in list_of_tuples_with_tables_in_1d_ohlcv_db:
        list_of_all_1d_tables.append(tuple_with_table_name[0])

    print ( "list_of_all_1d_tables\n" )
    print(list_of_all_1d_tables)

    for table_name in list_of_all_1d_tables:
        try:
            #table_name=table_name.replace("/","_")
            drop_table(table_name,
                       engine_for_usdt_trading_pairs_ohlcv_ready_for_rebound_db_1d_timeframe)
            drop_table ( table_name ,
                         engine_for_usdt_trading_pairs_ohlcv_ready_for_rebound_db_some_timeframe )

        except Exception as e:
            print(f"can't drop table {table_name}",e)

    # database="ohlcv_1d_tables_with_pairs_ready_to_rebound_from_mirror_level"
    #
    # engine_for_usdt_trading_pairs_ohlcv_ready_for_rebound_db_1d_timeframe.\
    #     execute ( f'''REVOKE CONNECT ON DATABASE {database} FROM public;
    #                             ALTER DATABASE {database} allow_connections = off;
    #                             SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{database}';
    #
    #                         ''' )
    # engine_for_usdt_trading_pairs_ohlcv_ready_for_rebound_db_1d_timeframe.\
    #     execute ( f'''DROP DATABASE {database};''' )
    #
    # database=f"ohlcv_{lower_timeframe_for_mirror_level_rebound_trading}_tables_with_pairs_ready_to_rebound_from_mirror_level"
    # engine_for_usdt_trading_pairs_ohlcv_ready_for_rebound_db_some_timeframe. \
    #     execute ( f'''REVOKE CONNECT ON DATABASE {database} FROM public;
    #                                 ALTER DATABASE {database} allow_connections = off;
    #                                 SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{database}';
    #
    #                             ''' )
    # engine_for_usdt_trading_pairs_ohlcv_ready_for_rebound_db_some_timeframe. \
    #     execute ( f'''DROP DATABASE {database};''' )
    #
    # ###############################
    # engine_for_usdt_trading_pairs_ohlcv_ready_for_rebound_db_some_timeframe , \
    # connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_some_timeframe = \
    #     connect_to_postres_db (
    #         f"ohlcv_{lower_timeframe_for_mirror_level_rebound_trading}_tables_with_pairs_ready_to_rebound_from_mirror_level" )
    #
    # engine_for_usdt_trading_pairs_ohlcv_ready_for_rebound_db_1d_timeframe , \
    # connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_1d_timeframe = \
    #     connect_to_postres_db (
    #         f"ohlcv_1d_tables_with_pairs_ready_to_rebound_from_mirror_level" )

    for row_number in range(0,len(mirror_levels_df)):
        try:
            usdt_trading_pair =mirror_levels_df.loc[row_number,'USDT_pair']
            exchange = mirror_levels_df.loc[row_number,'exchange']
            print ("usdt_trading_pair=",usdt_trading_pair)
            print ("exchange=",exchange)
            mirror_level = mirror_levels_df.loc[row_number , 'mirror_level']
            open_time_of_candle_with_legit_low = mirror_levels_df.loc[row_number ,
                                                                      'open_time_of_candle_with_legit_low']
            open_time_of_candle_with_legit_high = mirror_levels_df.loc[row_number ,
                                                                       'open_time_of_candle_with_legit_high']
            timestamp_for_low = (mirror_levels_df.loc[row_number ,
                                                      'timestamp_for_low'])/1000.0
            timestamp_for_high = (mirror_levels_df.loc[row_number ,
                                                       'timestamp_for_high'])/1000.0
            print("current_timestamp-timestamp_for_low=",
                  current_timestamp-timestamp_for_low)
            if (current_timestamp-timestamp_for_low)<86400*2:
                #last mirror level bar confirming level was was formed by low
                print(f"recently mirror level (low) was hit for {usdt_trading_pair} on {exchange}")
                exchange_object = getattr ( ccxt , exchange ) ()
                exchange_object.enableRateLimit = True
                print ( f"exchange_object.timeframes for {exchange}" )
                print(exchange_object.timeframes)
                exchange_object.load_markets()
                data_daily_timeframe = exchange_object.fetch_ohlcv ( usdt_trading_pair ,
                                                                     timeframe = '1d' )

                data_for_time_period =exchange_object.fetch_ohlcv ( usdt_trading_pair ,
                                                    timeframe=lower_timeframe_for_mirror_level_rebound_trading )
                header = ['Timestamp' , 'open' , 'high' , 'low' , 'close' , 'volume']
                data_daily_df = pd.DataFrame ( data_daily_timeframe , columns = header ).set_index ( 'Timestamp' )
                data_for_time_period_df = pd.DataFrame ( data_for_time_period , columns = header ).set_index ( 'Timestamp' )
                # print("data_df\n",data_df)
                last_price=data_daily_df.iloc[-1]["close"]

                penultimate_high = data_daily_df.iloc[-2]["high"]
                penultimate_low = data_daily_df.iloc[-2]["low"]
                print ( "last_price\n" , last_price )

                print ( "penultimate_high\n" , penultimate_high )
                print ( "penultimate_low\n" , penultimate_low )
                print ( "mirror_level\n" , mirror_level )
                if penultimate_low==mirror_level:
                    print("penultimate_low=mirror_level")
                true_range = penultimate_high - penultimate_low
                last_high = data_daily_df.iloc[-1]["high"]
                last_daily_low = data_daily_df.iloc[-1]["low"]
                backlash=true_range*0.2
                print ( "backlash\n" , backlash )
                print ( "mirror_level+backlash\n" , mirror_level+backlash )
                print ( "true_range\n" , true_range )
                print ( "last_daily_low\n" , last_daily_low )
                if last_daily_low>=mirror_level:
                    if last_daily_low<=(mirror_level+backlash):
                        print(f"last low for {usdt_trading_pair} on {exchange}"
                              f" is within backlash")
                        print ( "data_daily_df\n" , data_daily_df )
                        print("data_for_time_period\n",data_for_time_period)

                        ########################################################
                        data_for_time_period_df['open_time'] = \
                            [dt.datetime.fromtimestamp ( x / 1000.0 ) for x in data_for_time_period_df.index]
                        data_for_time_period_df.set_index ( 'open_time' )
                        data_for_time_period_df['psar'] = talib.SAR ( data_for_time_period_df.high ,
                                                                      data_for_time_period_df.low ,
                                                                      acceleration = 0.02 ,
                                                                      maximum = 0.2 )
                        #add column to df where mirror level and all other are nones
                        data_for_time_period_df['mirror_level_equal_to_lows_or_nans']=None


                        try:
                            for row_number_of_ohlcv_table,\
                                low_for_some_period in enumerate(data_for_time_period_df['low']):

                                # print (" row_number_of_ohlcv_table , low_for_some_period ")
                                # print(type(row_number_of_ohlcv_table), low_for_some_period)
                                if low_for_some_period==mirror_level:
                                    #data_for_time_period_df.loc[row_number_of_ohlcv_table,['mirror_level_or_nans']]=mirror_level
                                    #data_for_time_period_df[row_number_of_ohlcv_table]['mirror_level_or_nans']=mirror_level
                                    data_for_time_period_df.iat[
                                        row_number_of_ohlcv_table ,
                                        data_for_time_period_df.columns.get_loc('mirror_level_equal_to_lows_or_nans')] = mirror_level
                        except Exception as e:
                            print(e)
                            traceback.print_exc()

                        try:
                            for row_number_of_ohlcv_table , high_for_some_period in enumerate (
                                    data_for_time_period_df['high'] ):

                                # print (" row_number_of_ohlcv_table , low_for_some_period ")
                                # print(type(row_number_of_ohlcv_table), low_for_some_period)
                                if high_for_some_period == mirror_level:
                                    # data_for_time_period_df.loc[row_number_of_ohlcv_table,['mirror_level_or_nans']]=mirror_level
                                    # data_for_time_period_df[row_number_of_ohlcv_table]['mirror_level_or_nans']=mirror_level
                                    data_for_time_period_df.iat[
                                        row_number_of_ohlcv_table , data_for_time_period_df.columns.get_loc (
                                            'mirror_level_equal_to_highs_or_nans' )] = mirror_level
                        except Exception as e:
                            print(e)
                            traceback.print_exc()
                        ###########################################
                        data_daily_df['open_time'] = \
                            [dt.datetime.fromtimestamp ( x / 1000.0 ) for x in data_daily_df.index]
                        data_daily_df.set_index ( 'open_time' )
                        data_daily_df['psar'] = talib.SAR ( data_daily_df.high ,
                                                                      data_daily_df.low ,
                                                                      acceleration = 0.02 ,
                                                                      maximum = 0.2 )
                        # add column to df where mirror level and all other are nones
                        data_daily_df['mirror_level_equal_to_lows_or_nans'] = None

                        try:
                            for row_number_of_ohlcv_table , \
                                low_for_some_period in enumerate ( data_daily_df['low'] ):

                                # print (" row_number_of_ohlcv_table , low_for_some_period ")
                                # print(type(row_number_of_ohlcv_table), low_for_some_period)
                                if low_for_some_period == mirror_level:
                                    # data_for_time_period_df.loc[row_number_of_ohlcv_table,['mirror_level_or_nans']]=mirror_level
                                    # data_for_time_period_df[row_number_of_ohlcv_table]['mirror_level_or_nans']=mirror_level
                                    data_daily_df.iat[
                                        row_number_of_ohlcv_table ,
                                        data_daily_df.columns.get_loc (
                                            'mirror_level_equal_to_lows_or_nans' )] = mirror_level
                        except Exception as e:
                            print(e)
                            traceback.print_exc()
                        print ( "data_for_time_period_df\n" , data_for_time_period_df )



                        data_for_time_period_df.to_sql ( f"{usdt_trading_pair}_on_{exchange}_at_{mirror_level}" ,
                                         connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_some_timeframe ,
                                         if_exists = 'replace' )

                        data_daily_df.to_sql ( f"{usdt_trading_pair}_on_{exchange}_at_{mirror_level}" ,
                                                         connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_1d_timeframe ,
                                                         if_exists = 'replace' )




            if (current_timestamp-timestamp_for_high)<86400*2:
                # last mirror level bar confirming level was was formed by high
                print(f"recently mirror level (high) was hit for {usdt_trading_pair} on {exchange}")
                exchange_object = getattr ( ccxt , exchange ) ()
                exchange_object.enableRateLimit = True
                print ( f"exchange_object.timeframes for {exchange}" )
                print ( exchange_object.timeframes )
                exchange_object.load_markets ()
                data_daily_timeframe = exchange_object.fetch_ohlcv ( usdt_trading_pair ,
                                                                     timeframe = '1d' )

                data_for_time_period = exchange_object.fetch_ohlcv ( usdt_trading_pair ,
                                                                     timeframe = lower_timeframe_for_mirror_level_rebound_trading )
                header = ['Timestamp' , 'open' , 'high' , 'low' , 'close' , 'volume']
                data_daily_timeframe_df = pd.DataFrame ( data_daily_timeframe , columns = header ).set_index ( 'Timestamp' )
                data_for_time_period_df = pd.DataFrame ( data_for_time_period , columns = header ).set_index (
                    'Timestamp' )
                # print ( "data_df\n" , data_df )
                last_price = data_daily_timeframe_df.iloc[-1]["close"]
                penultimate_high = data_daily_timeframe_df.iloc[-2]["high"]
                penultimate_low = data_daily_timeframe_df.iloc[-2]["low"]
                print ( "last_price\n" , last_price )
                print ( "penultimate_high\n" , penultimate_high )
                print ( "penultimate_low\n" , penultimate_low )
                print ( "last_price\n" , last_price )
                print ( "mirror_level\n" , mirror_level )
                if penultimate_high==mirror_level:
                    print("penultimate_high=mirror_level")


                true_range=penultimate_high-penultimate_low
                last_high = data_daily_timeframe_df.iloc[-1]["high"]
                last_low = data_daily_timeframe_df.iloc[-1]["low"]

                backlash = true_range*0.2
                print ( "backlash\n" , backlash )
                print ( "mirror_level-backlash\n" , mirror_level - backlash )
                print ( "true_range\n" , true_range )
                print ( "last_high\n" , last_high )
                if last_high <= mirror_level:
                    if last_high >= (mirror_level - backlash):
                        print ( f"last high for {usdt_trading_pair} on {exchange} "
                                f" is within backlash"  )
                        data_for_time_period_df['open_time'] = \
                            [dt.datetime.fromtimestamp ( x / 1000.0 ) for x in data_for_time_period_df.index]
                        data_for_time_period_df.set_index ( 'open_time' )
                        data_for_time_period_df['psar'] = talib.SAR ( data_for_time_period_df.high ,
                                                      data_for_time_period_df.low ,
                                                      acceleration = 0.02 ,
                                                      maximum = 0.2 )

                        data_for_time_period_df['mirror_level_equal_to_highs_or_nans'] = None
                        try:
                            for row_number_of_ohlcv_table , high_for_some_period in enumerate (
                                    data_for_time_period_df['high'] ):

                                # print (" row_number_of_ohlcv_table , low_for_some_period ")
                                # print(type(row_number_of_ohlcv_table), low_for_some_period)
                                if high_for_some_period == mirror_level:
                                    # data_for_time_period_df.loc[row_number_of_ohlcv_table,['mirror_level_or_nans']]=mirror_level
                                    # data_for_time_period_df[row_number_of_ohlcv_table]['mirror_level_or_nans']=mirror_level
                                    data_for_time_period_df.iat[
                                        row_number_of_ohlcv_table , data_for_time_period_df.columns.get_loc (
                                            'mirror_level_equal_to_highs_or_nans' )] = mirror_level
                        except:
                            pass

                        try:
                            for row_number_of_ohlcv_table , \
                                low_for_some_period in enumerate ( data_for_time_period_df['low'] ):

                                # print (" row_number_of_ohlcv_table , low_for_some_period ")
                                # print(type(row_number_of_ohlcv_table), low_for_some_period)
                                if low_for_some_period == mirror_level:
                                    # data_for_time_period_df.loc[row_number_of_ohlcv_table,['mirror_level_or_nans']]=mirror_level
                                    # data_for_time_period_df[row_number_of_ohlcv_table]['mirror_level_or_nans']=mirror_level
                                    data_for_time_period_df.iat[
                                        row_number_of_ohlcv_table ,
                                        data_for_time_period_df.columns.get_loc (
                                            'mirror_level_equal_to_lows_or_nans' )] = mirror_level
                        except:
                            pass

                        data_for_time_period_df['timeframe'] = lower_timeframe_for_mirror_level_rebound_trading



                        print ( "data_for_time_period_df\n" , data_for_time_period_df )
                        data_for_time_period_df.to_sql ( f"{usdt_trading_pair}_on_{exchange}_at_{mirror_level}" ,
                                         connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_some_timeframe ,
                                         if_exists = 'replace' )

                        ###########################################
                        data_daily_timeframe_df['open_time'] = \
                            [dt.datetime.fromtimestamp ( x / 1000.0 ) for x in data_daily_timeframe_df.index]
                        data_daily_timeframe_df.set_index ( 'open_time' )
                        data_daily_timeframe_df['psar'] = talib.SAR ( data_daily_timeframe_df.high ,
                                                            data_daily_timeframe_df.low ,
                                                            acceleration = 0.02 ,
                                                            maximum = 0.2 )
                        # add column to df where mirror level and all other are nones
                        data_daily_timeframe_df['mirror_level_equal_to_highs_or_nans'] = None

                        for row_number_of_ohlcv_table , \
                            high_for_some_period in enumerate ( data_daily_timeframe_df['high'] ):

                            # print (" row_number_of_ohlcv_table , low_for_some_period ")
                            # print(type(row_number_of_ohlcv_table), low_for_some_period)
                            if high_for_some_period == mirror_level:
                                # data_for_time_period_df.loc[row_number_of_ohlcv_table,['mirror_level_or_nans']]=mirror_level
                                # data_for_time_period_df[row_number_of_ohlcv_table]['mirror_level_or_nans']=mirror_level
                                data_daily_timeframe_df.iat[
                                    row_number_of_ohlcv_table ,
                                    data_daily_timeframe_df.columns.get_loc (
                                        'mirror_level_equal_to_highs_or_nans' )] = mirror_level

                        print ( "data_for_time_period_df\n" , data_for_time_period_df.to_string () )

                        data_for_time_period_df.to_sql (
                            f"{usdt_trading_pair}_on_{exchange}_at_{mirror_level}" ,
                            connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_some_timeframe ,
                            if_exists = 'replace' )

                        data_daily_timeframe_df.to_sql ( f"{usdt_trading_pair}_on_{exchange}_at_{mirror_level}" ,
                                               connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_1d_timeframe ,
                                               if_exists = 'replace' )

        except Exception as e:
            traceback.print_exc()
            print(e)
        finally:
            continue
    connection_to_usdt_trading_pairs_daily_ohlcv.close ()
    connection_to_btc_and_usdt_trading_pairs.close()
    connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_some_timeframe.close()
    end = time.perf_counter ()
    print ( "time in seconds is " , end - start )
    print ( "time in minutes is " , (end - start) / 60.0 )
    print ( "time in hours is " , (end - start) / 60.0 / 60.0 )


if __name__=="__main__":
    lower_timeframe_for_mirror_level_rebound_trading='1h'
    check_if_current_bar_closed_within_acceptable_range_to_mirror_level_which_last_was_this_period_ago(lower_timeframe_for_mirror_level_rebound_trading)