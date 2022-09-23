import multiprocessing
import asyncio
import os
import sys
from multiprocessing import Process
import time
import traceback
import db_config
import sqlalchemy
import psycopg2
import pandas as pd
import talib
import datetime as dt
import ccxt.async_support as ccxt  # noqa: E402
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database,database_exists


def drop_table(table_name,engine):
    engine.execute (
        f'DROP TABLE IF EXISTS "{table_name}";' )

def drop_all_tables_from_db(db_name,engine):
    list_of_all_tables=get_list_of_table_names_from_postres_df(db_name)
    for number,table in enumerate(list_of_all_tables):
        print("number of table=",number)
        drop_table(table,engine)

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


def connect_to_postres_db_with_dropping_it_first(database = "async_rounded_ohlcv_data_for_usdt_trading_pairs"):
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
        engine.connect ()
        print ( f'Connection to {engine} established after creating new database' )

    else:
        # Connect the database if exists.

        engine = create_engine ( f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{dummy_database}" ,
                                 isolation_level = 'AUTOCOMMIT' , echo = True )

        engine.execute ( f'''REVOKE CONNECT ON DATABASE {database} FROM public;
                                ALTER DATABASE {database} allow_connections = off;
                                SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{database}';

                            ''' )
        engine.execute ( f'''DROP DATABASE {database};''' )

        engine = create_engine ( f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}" ,
                                 isolation_level = 'AUTOCOMMIT' , echo = True )
        create_database ( engine.url )
        print ( f'new database created for {engine}' )
        # engine.execute ( 'CREATE DATABASE ' + database[0] ).execution_options(autocommit=True)

    connection_to_ohlcv_for_usdt_pairs = engine.connect ()
    print ( f'Connection to {engine} established. Database already existed' )
    return engine , connection_to_ohlcv_for_usdt_pairs

def get_list_of_table_names_from_postres_df(db_name="async_ohlcv_data_for_usdt_trading_pairs"):
    engine_for_usdt_trading_pairs_ohlcv_db , connection_to_usdt_trading_pairs_ohlcv = \
        connect_to_postres_db ( db_name )
    list_of_all_tables_in_db = []

    list_of_tables_in_db = \
        engine_for_usdt_trading_pairs_ohlcv_db. \
            execute ( "SELECT table_name FROM information_schema.tables WHERE table_schema='public'" )
    list_of_tuples_with_tables_db = list ( list_of_tables_in_db )
    for tuple_with_table_name in list_of_tuples_with_tables_db:
        list_of_all_tables_in_db.append ( tuple_with_table_name[0] )

    print ( "list_of_all_tables_in_db\n" )
    print ( list_of_all_tables_in_db )
    return list_of_all_tables_in_db

def round_ohlc_in_one_table(table_in_ohlcv_db,
                            engine_for_usdt_trading_pairs_ohlcv_db ,
                            engine_for_rounded_usdt_trading_pairs_ohlcv_db,
                            round_to_this_number_of_decimals=3):
    ohlcv_dataframe = pd.read_sql_query ( f'''select * from "{table_in_ohlcv_db}"''' ,
                                          engine_for_usdt_trading_pairs_ohlcv_db )
    ohlcv_dataframe['open'] = ohlcv_dataframe['open'].round ( decimals = round_to_this_number_of_decimals )
    ohlcv_dataframe['high'] = ohlcv_dataframe['high'].round ( decimals = round_to_this_number_of_decimals )
    ohlcv_dataframe['low'] = ohlcv_dataframe['low'].round ( decimals = round_to_this_number_of_decimals )
    ohlcv_dataframe['close'] = ohlcv_dataframe['close'].round ( decimals = round_to_this_number_of_decimals )
    print ( ohlcv_dataframe )
    ohlcv_dataframe.to_sql ( f"{table_in_ohlcv_db}" ,
                             engine_for_rounded_usdt_trading_pairs_ohlcv_db ,
                             if_exists = 'replace' )


def round_historical_USDT_pairs_ohlc_in_all_tables():
    engine_for_usdt_trading_pairs_ohlcv_db , connection_to_usdt_trading_pairs_ohlcv = \
        connect_to_postres_db ( "async_ohlcv_data_for_usdt_trading_pairs" )

    engine_for_rounded_usdt_trading_pairs_ohlcv_db ,\
    connection_to_rounded_usdt_trading_pairs_ohlcv = \
        connect_to_postres_db_with_dropping_it_first ( "async_rounded_ohlcv_data_for_usdt_trading_pairs" )

    # drop_all_tables_from_db("async_rounded_ohlcv_data_for_usdt_trading_pairs",
    #                         engine_for_rounded_usdt_trading_pairs_ohlcv_db)

    list_of_all_tables_in_ohlcv_db=\
        get_list_of_table_names_from_postres_df ( "async_ohlcv_data_for_usdt_trading_pairs" )
    ###################################
    ###################################
    round_to_this_number_of_decimals=3
    #################################
    ######################################
    procs = []
    how_many_processes_to_spawn = 30

    for i in range ( 0 , len ( list_of_all_tables_in_ohlcv_db ) , how_many_processes_to_spawn ):
        for table_in_ohlcv_db in list_of_all_tables_in_ohlcv_db[i:i+how_many_processes_to_spawn]:
            try:

                proc = Process ( target = round_ohlc_in_one_table , args =
                                            (table_in_ohlcv_db ,
                                          engine_for_usdt_trading_pairs_ohlcv_db ,
                                          engine_for_rounded_usdt_trading_pairs_ohlcv_db ,
                                          round_to_this_number_of_decimals) )
                # round_ohlc_in_one_table ( table_in_ohlcv_db ,
                #                           engine_for_usdt_trading_pairs_ohlcv_db ,
                #                           engine_for_rounded_usdt_trading_pairs_ohlcv_db ,
                #                           round_to_this_number_of_decimals )
                proc.start ()
                procs.append ( proc )

            except:
                traceback.print_exc()
        for process in procs:
            process.join ()

    # for table_in_ohlcv_db in list_of_all_tables_in_ohlcv_db:
    #     try:
    #         round_ohlc_in_one_table ( table_in_ohlcv_db ,
    #                                   engine_for_usdt_trading_pairs_ohlcv_db ,
    #                                   engine_for_rounded_usdt_trading_pairs_ohlcv_db ,
    #                                   round_to_this_number_of_decimals )
    #     except:
    #         traceback.print_exc()



if __name__=="__main__":
    start = time.perf_counter ()
    round_historical_USDT_pairs_ohlc_in_all_tables()
    end = time.perf_counter ()
    print ( "time in seconds is " , end - start )
    print ( "time in minutes is " , (end - start) / 60.0 )
    print ( "time in hours is " , (end - start) / 60.0 / 60.0 )