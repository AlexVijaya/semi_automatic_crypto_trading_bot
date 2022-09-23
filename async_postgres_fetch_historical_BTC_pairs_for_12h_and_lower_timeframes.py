# -*- coding: utf-8 -*-
import multiprocessing
import asyncio
import os
import sys
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

def connect_to_postres_db(database):
    dialect = db_config.dialect
    driver = db_config.driver
    password = db_config.password
    user = db_config.user
    host = db_config.host
    port = db_config.port

    dummy_database = db_config.dummy_database

    engine = create_engine ( f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}" ,
                             isolation_level = 'AUTOCOMMIT' ,
                             echo = False,
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

    if database_exists ( engine.url ):
        print("database exists ok")

        engine = create_engine ( f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{dummy_database}" ,
                                 isolation_level = 'AUTOCOMMIT' , echo = True )
        engine.execute(f'''REVOKE CONNECT ON DATABASE {database} FROM public;''')
        engine.execute ( f'''
                            ALTER DATABASE {database} allow_connections = off;
                            SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{database}';

                        ''' )
        engine.execute ( f'''DROP DATABASE {database};''' )

        engine = create_engine ( f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}" ,
                                 isolation_level = 'AUTOCOMMIT' , echo = True )
        create_database ( engine.url )
        print ( f'new database created for {engine}' )

    connection = engine.connect ()

    print ( f'Connection to {engine} established. Database already existed.'
            f' So no new db was created' )
    return engine , connection

def convert_string_timeframe_into_seconds(timeframe):
    timeframe_in_seconds=0
    if timeframe=='1d':
        timeframe_in_seconds=86400
    if timeframe == '12h':
        timeframe_in_seconds = 86400/2
    if timeframe == '6h':
        timeframe_in_seconds = 86400/4
    if timeframe == '4h':
        timeframe_in_seconds = 86400/6
    if timeframe == '8h':
        timeframe_in_seconds = 86400/3
    return timeframe_in_seconds



new_counter=0
not_active_pair_counter = 0
list_of_inactive_pairs=[]

async def get_hisorical_data_from_exchange_for_many_symbols(exchange,
                                                            counter,
                                                            engine,timeframe):
    print("exchange=",exchange)
    global new_counter
    global list_of_inactive_pairs
    global not_active_pair_counter

    exchange_object = getattr ( ccxt , exchange ) ()
    exchange_object.enableRateLimit = True


    try:
        # connection_to_usdt_trading_pairs_ohlcv = \
        #     sqlite3.connect ( os.path.join ( os.getcwd () ,
        #                                      "datasets" ,
        #                                      "sql_databases" ,
        #                                      "all_exchanges_multiple_tables_historical_data_for_usdt_trading_pairs.db" ) )

        await exchange_object.load_markets ()
        list_of_all_symbols_from_exchange=exchange_object.symbols

        list_of_trading_pairs_with_USDT = []
        list_of_trading_pairs_with_USD = []
        list_of_trading_pairs_with_BTC = []

        timeframe_duration_in_seconds=convert_string_timeframe_into_seconds(timeframe)

        for trading_pair in list_of_all_symbols_from_exchange:
            # for item in counter_gen():
            #     print ("item=",item)

            counter =counter+1

            try:
                print ( "exchange=" , exchange )
                print ( "usdt_pair=" , trading_pair )
                if "UP/" in trading_pair or "DOWN/" in trading_pair or "BEAR/" in trading_pair or "BULL/" in trading_pair:
                    continue
                if "/BTC" in trading_pair:
                    new_counter = new_counter + 1
                    print("new_counter=",new_counter)
                    list_of_trading_pairs_with_USDT.append(trading_pair)
                    # print ( f"list_of_trading_pairs_with_USDT_on_{exchange}\n" ,
                    #         list_of_trading_pairs_with_USDT )

                    if ('active' in exchange_object.markets[trading_pair]) or (exchange_object.markets[trading_pair]['active']):
                        data = await exchange_object.fetch_ohlcv ( trading_pair , timeframe )

                        print ( f"counter_for_{exchange}=" , counter )
                        header = ['Timestamp' , 'open' , 'high' , 'low' , 'close' , 'volume']
                        data_df = pd.DataFrame ( data , columns = header ).set_index ( 'Timestamp' )
                        # try:
                        #     data_4h = await exchange_object.fetch_ohlcv ( trading_pair , '1d' )
                        #
                        #     data_df_4h = pd.DataFrame ( data_4h , columns = header ).set_index ( 'Timestamp' )
                        #     print(f"data_df_4h_for_{trading_pair} on exchange {exchange}\n",
                        #           data_df_4h)
                        #     data_df_4h['open_time'] = \
                        #         [dt.datetime.fromtimestamp ( x / 1000.0 ) for x in data_df_4h.index]
                        #     data_df_4h.set_index ( 'open_time' )
                        #     # print ( "list_of_dates=\n" , list_of_dates )
                        #     # time.sleep(5)
                        #     data_df_4h['psar'] = talib.SAR ( data_df_4h.high ,
                        #                                   data_df_4h.low ,
                        #                                   acceleration = 0.02 ,
                        #                                   maximum = 0.2 )
                        #     print ( "data_df_4h\n" , data_df_4h )
                        #
                        #     data_df_4h.to_sql ( f"{trading_pair}_on_{exchange}" ,
                        #                      connection_to_usdt_trading_pairs_4h_ohlcv ,
                        #                      if_exists = 'replace' )
                        #
                        #
                        # except Exception as e:
                        #     print("something is wrong with 4h timeframe"
                        #           f"for {trading_pair} on exchange {exchange}\n",e)
                        print ( "=" * 80 )
                        print ( f'ohlcv for {trading_pair} on exchange {exchange}\n' )
                        print ( data_df )
                        data_df['trading_pair'] = trading_pair
                        data_df['exchange'] = exchange
                        current_timestamp=time.time()
                        last_timestamp_in_df=data_df.tail(1).index.item()/1000.0
                        print("current_timestamp=",current_timestamp)
                        print("data_df.tail(1).index.item()=",data_df.tail(1).index.item()/1000.0)

                        #check if the pair is active
                        if not abs(current_timestamp-last_timestamp_in_df)<(timeframe_duration_in_seconds*1):
                            print(f"not quite active trading pair {trading_pair} on {exchange}")
                            not_active_pair_counter=not_active_pair_counter+1
                            print("not_active_pair_counter=",not_active_pair_counter)
                            list_of_inactive_pairs.append(f"{trading_pair}_on_{exchange}")
                            continue

                        data_df['open_time'] = \
                            [dt.datetime.fromtimestamp ( x / 1000.0 ) for x in data_df.index]
                        data_df.set_index ( 'open_time' )
                        # print ( "list_of_dates=\n" , list_of_dates )
                        # time.sleep(5)
                        data_df['psar'] = talib.SAR ( data_df.high ,
                                                      data_df.low ,
                                                      acceleration = 0.02 ,
                                                      maximum = 0.2 )
                        print ( "data_df\n" , data_df )

                        data_df.to_sql ( f"{trading_pair}_on_{exchange}" ,
                                         engine ,
                                         if_exists = 'replace' )



                elif "/USD" in trading_pair and "/USDT" not in trading_pair:
                    #print(trading_pair)
                    list_of_trading_pairs_with_USD.append(trading_pair)

                elif "/USDT" in trading_pair:
                    #print(trading_pair)
                    list_of_trading_pairs_with_BTC.append(trading_pair)
                else:
                    continue




                    #print ( "data=" , data )
            except ccxt.base.errors.RequestTimeout:
                print("found ccxt.base.errors.RequestTimeout error inner")
                continue


            except ccxt.RequestTimeout:
                print("found ccxt.RequestTimeout error inner")
                continue


            except Exception as e:
                print(f"problem with {trading_pair} on {exchange}\n", e)
                traceback.print_exc ()
                continue
            finally:
                await exchange_object.close ()
                continue
        await exchange_object.close ()
        # connection_to_usdt_trading_pairs_ohlcv.close()

    except Exception as e:
        print ( f"found {e} error outer" )
        traceback.print_exc ()


    finally:
        await exchange_object.close ()
        print("exchange object is closed")


def fetch_historical_usdt_pairs_asynchronously(engine,exchanges_list,timeframe):
    start=time.perf_counter()
    # exchanges_list=['aax', 'ascendex', 'bequant', 'bibox', 'bigone',
    #                 'binance', 'binancecoinm', 'binanceus', 'binanceusdm',
    #                 'bit2c', 'bitbank', 'bitbay', 'bitbns', 'bitcoincom',
    #                 'bitfinex', 'bitfinex2', 'bitflyer', 'bitforex', 'bitget',
    #                 'bithumb', 'bitmart', 'bitmex', 'bitopro', 'bitpanda', 'bitrue',
    #                 'bitso', 'bitstamp', 'bitstamp1', 'bittrex', 'bitvavo', 'bkex',
    #                 'bl3p', 'blockchaincom', 'btcalpha', 'btcbox', 'btcmarkets',
    #                 'btctradeua', 'btcturk', 'buda', 'bw', 'bybit', 'bytetrade',
    #                 'cdax', 'cex', 'coinbase', 'coinbaseprime', 'coinbasepro',
    #                 'coincheck', 'coinex', 'coinfalcon', 'coinflex', 'coinmate',
    #                 'coinone', 'coinspot', 'crex24', 'cryptocom', 'currencycom',
    #                 'delta', 'deribit', 'digifinex', 'eqonex', 'exmo', 'flowbtc',
    #                 'fmfwio', 'ftx', 'ftxus', 'gateio', 'gemini', 'hitbtc', 'hitbtc3',
    #                 'hollaex', 'huobi', 'huobijp', 'huobipro', 'idex',
    #                 'independentreserve', 'indodax', 'itbit', 'kraken', 'kucoin',
    #                 'kucoinfutures', 'kuna', 'latoken', 'lbank', 'lbank2', 'liquid',
    #                 'luno', 'lykke', 'mercado', 'mexc', 'mexc3', 'ndax', 'novadax',
    #                 'oceanex', 'okcoin', 'okex', 'okex5', 'okx', 'paymium', 'phemex',
    #                 'poloniex', 'probit', 'qtrade', 'ripio', 'stex', 'therock',
    #                 'tidebit', 'tidex', 'timex', 'upbit', 'vcc', 'wavesexchange',
    #                 'wazirx', 'whitebit', 'woo', 'xena', 'yobit', 'zaif', 'zb',
    #                 'zipmex', 'zonda']


    # connection_to_usdt_trading_pairs_daily_ohlcv = \
    #     sqlite3.connect ( os.path.join ( os.getcwd () ,
    #                                      "datasets" ,
    #                                      "sql_databases" ,
    #                                      "async_all_exchanges_multiple_tables_historical_data_for_usdt_trading_pairs.db" ) )

    # connection_to_usdt_trading_pairs_4h_ohlcv = \
    #     sqlite3.connect ( os.path.join ( os.getcwd () ,
    #                                      "datasets" ,
    #                                      "sql_databases" ,
    #                                      "async_all_exchanges_multiple_tables_historical_data_for_usdt_trading_pairs_4h.db" ) )

    # coroutines = [await get_hisorical_data_from_exchange_for_many_symbols(exchange ) for exchange in  exchanges_list]
    # await asyncio.gather(*coroutines, return_exceptions = True)
    #
    usdt_trading_pair_number = 0
    counter = 0
    global new_counter
    global list_of_inactive_pairs
    loop=asyncio.get_event_loop()
    tasks=[loop.create_task(get_hisorical_data_from_exchange_for_many_symbols(exchange,
                                                                              counter,
                                                                              engine,timeframe)) for exchange in  exchanges_list]

    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
    #connection_to_usdt_trading_pairs_daily_ohlcv.close()
    # connection_to_usdt_trading_pairs_4h_ohlcv.close ()
    print("list_of_inactive_pairs\n",list_of_inactive_pairs)
    print("len(list_of_inactive_pairs=",len(list_of_inactive_pairs))
    end = time.perf_counter ()
    print("time in seconds is ", end-start)
    print ( "time in minutes is " , (end - start)/60.0 )
    print ( "time in hours is " , (end - start) / 60.0/60.0 )

def fetch_all_ohlcv_tables(timeframe):

    engine , connection_to_ohlcv_for_usdt_pairs =\
        connect_to_postres_db ("ohlcv_data_for_btc_pairs_for_12h_and_lower_tf")
    exchanges_list = ccxt.exchanges
    how_many_exchanges = len ( exchanges_list )
    step_for_exchanges = 50

    # fetch_historical_usdt_pairs_asynchronously(engine,exchanges_list)

    process_list = []
    for exchange_counter in \
            range ( 0 , len ( exchanges_list ) ,
                    step_for_exchanges ):
        print ( "exchange_counter=" , exchange_counter )
        print (
            f"exchanges[{exchange_counter}:{exchange_counter} + {step_for_exchanges}]" )
        print ( exchanges_list[
                exchange_counter:exchange_counter + step_for_exchanges] )

        # for number_of_exchange , exchange \
        #         in enumerate ( exchanges_list[exchange_counter:exchange_counter +
        #                                                               how_many_separate_processes_to_spawn_each_corresponding_to_one_exchange] ):
        print ( exchanges_list[
                exchange_counter:exchange_counter + step_for_exchanges] )

        p = multiprocessing.Process ( target =
                                      fetch_historical_usdt_pairs_asynchronously ,
                                      args = (engine , exchanges_list[
                                                       exchange_counter:exchange_counter + step_for_exchanges],timeframe) )
        p.start ()
        process_list.append ( p )
    for process in process_list:
        process.join ()

    connection_to_ohlcv_for_usdt_pairs.close ()
if __name__=="__main__":
    timeframe='12h'
    fetch_all_ohlcv_tables(timeframe)
#asyncio.run(get_hisorical_data_from_exchange_for_many_symbols_and_exchanges())

