from get_list_of_stablecoins import extract_list_of_stablecoins_from_coingecko
import traceback

import pandas as pd
import time
import datetime

import talib

import ccxt
import datetime as dt


from sqlalchemy_utils import create_database,database_exists
import db_config

from sqlalchemy import inspect
import logging
from sqlalchemy import MetaData
from sqlalchemy import create_engine


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

def drop_tables_in_db_if_first_pair_is_stablecoin():
    start = time.perf_counter ()
    current_timestamp = time.time ()

    engine_for_usdt_trading_pairs_daily_ohlcv , connection_to_usdt_trading_pairs_daily_ohlcv = \
        connect_to_postres_db ( "async_ohlcv_data_for_usdt_trading_pairs" )

    engine_for_rounded_usdt_trading_pairs_daily_ohlcv , connection_to_rounded_usdt_trading_pairs_daily_ohlcv = \
        connect_to_postres_db ( "async_rounded_ohlcv_data_for_usdt_trading_pairs" )


    inspector = inspect ( engine_for_usdt_trading_pairs_daily_ohlcv )
    list_of_tablenames_in_initial_ohlcv_db=inspector.get_table_names()

    list_of_usdt_pairs_where_first_asset_is_stablecoin=['USDT', 'USDC', 'BUSD',
                                                        'DAI', 'FRAX', 'TUSD', 'USDP',
                                                        'USDD', 'USDN', 'XAUT', 'GUSD',
                                                        'USTC', 'MIM', 'EURT', 'LUSD',
                                                        'ALUSD', 'FEI', 'EURS', 'USDX',
                                                        'SUSD', 'HUSD', 'FLEXUSD', 'XSGD',
                                                        'DOLA', 'MIMATIC', 'CUSD', 'OUSD',
                                                        'YUSD', 'AGEUR', 'USDK', 'TRYB',
                                                        'MUSD', 'CEUR', 'BEAN', 'RSV',
                                                        'VAI', 'TOR', 'SEUR', 'USDS', 'XIDR',
                                                        'BITCNY', 'VST', 'EOSDT', 'USDS',
                                                        'CNHT', 'USDB', 'PAR', 'ZUSD', 'FLOAT',
                                                        'IUSDS', 'ARTH', 'USDAP', 'ESD', 'DSD',
                                                        'ZUSD', 'BAC', 'EUROS', 'ONC', 'STATIK',
                                                        'IEUROS', 'BSD', 'SAC', 'USD', 'XDAI',
                                                        'DUSD', 'USN', 'THKD', 'QC', 'USDS',
                                                        'IRON', 'KDAI', 'USDO', 'USTC', 'JEUR',
                                                        'JGBP', 'JCHF', 'IRON', 'CADC', 'OUSD',
                                                        'JPYC', 'USX', 'BDO', 'COUSD', 'NZDS',
                                                        'AUSD']

    try:
        list_of_usdt_pairs_where_first_asset_is_stablecoin=\
            extract_list_of_stablecoins_from_coingecko()
    except:
        traceback.print_exc()

    finally:
        print("list_of_tablenames_in_initial_ohlcv_db")
        print ( list_of_tablenames_in_initial_ohlcv_db )

        print ( 'len ( list_of_tablenames_in_initial_ohlcv_db ) ')
        print ( len(list_of_tablenames_in_initial_ohlcv_db) )

        print ( "list_of_usdt_pairs_where_first_asset_is_stablecoin" )
        print ( list_of_usdt_pairs_where_first_asset_is_stablecoin )
        count=0

        for ohlcv_table_name in list_of_tablenames_in_initial_ohlcv_db:

            split_ohlcv_table_name=ohlcv_table_name.split("/")
            print ( "split_ohlcv_table_name" )
            print(split_ohlcv_table_name)
            if split_ohlcv_table_name[0] in list_of_usdt_pairs_where_first_asset_is_stablecoin:
                count = count + 1
                print("count=",count)
                print("table to be dropped is ",ohlcv_table_name)
                drop_table(ohlcv_table_name,engine_for_usdt_trading_pairs_daily_ohlcv)
                drop_table ( ohlcv_table_name , engine_for_rounded_usdt_trading_pairs_daily_ohlcv )

    connection_to_usdt_trading_pairs_daily_ohlcv.close()
    connection_to_rounded_usdt_trading_pairs_daily_ohlcv.close ()


    end = time.perf_counter ()
    print ( "time in seconds is " , end - start )
    print ( "time in minutes is " , (end - start) / 60.0 )
    print ( "time in hours is " , (end - start) / 60.0 / 60.0 )


if __name__=="__main__":
    drop_tables_in_db_if_first_pair_is_stablecoin()