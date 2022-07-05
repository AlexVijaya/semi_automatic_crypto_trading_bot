# -*- coding: utf-8 -*-
import sqlite3
import asyncio
import os
import sys
import time
import traceback
#from find_keltner_channel import create_empty_database
import pandas as pd
import talib
import datetime as dt
import ccxt.async_support as ccxt  # noqa: E402
from pathlib import Path
# async def get_hisorical_data_from_exchange_for_one_symbol(exchange_object, usdt_pair):
#
#     #await exchange_object.load_markets ()
#     #exchange_object.enableRateLimit = True
#     data = await exchange_object.fetch_ohlcv ( usdt_pair , '1d' )
#     print("usdt_pair=",usdt_pair)
#     #print ("data\n",data)
#     #print("\nexchange_object.symbols\n",exchange_object.symbols)
#
#     await exchange_object.close ()
#     return data
#
#
# def counter_gen(n=0):
#
#     while True:
#         n += 1
#         yield n
new_counter=0
not_active_pair_counter = 0

async def get_hisorical_data_from_exchange_for_many_symbols(exchange,
                                                            counter,
                                                            connection_to_btc_trading_pairs_ohlcv):
    print("exchange=",exchange)
    global new_counter
    exchange_object = getattr ( ccxt , exchange ) ()
    exchange_object.enableRateLimit = True
    global not_active_pair_counter
    try:

        # connection_to_btc_trading_pairs_ohlcv = \
        #     sqlite3.connect ( os.path.join ( os.getcwd () ,
        #                                      "datasets" ,
        #                                      "sql_databases" ,
        #                                      "all_exchanges_multiple_tables_historical_data_for_btc_trading_pairs.db" ) )

        await exchange_object.load_markets ()
        list_of_all_symbols_from_exchange=exchange_object.symbols

        list_of_trading_pairs_with_USDT = []
        list_of_trading_pairs_with_USD = []
        list_of_trading_pairs_with_BTC = []

        for trading_pair in list_of_all_symbols_from_exchange:
            # for item in counter_gen():
            #     print ("item=",item)

            counter =counter+1

            try:
                print ( "exchange=" , exchange )
                print ( "btc_pair=" , trading_pair )
                if "UP/" in trading_pair or "DOWN/" in trading_pair or "BEAR/" in trading_pair or "BULL/" in trading_pair:
                    continue
                if "/USDT" in trading_pair:

                    list_of_trading_pairs_with_USDT.append(trading_pair)
                    # print ( f"list_of_trading_pairs_with_USDT_on_{exchange}\n" ,
                    #         list_of_trading_pairs_with_USDT )



                elif "/USD" in trading_pair and "/USDT" not in trading_pair:
                    #print(trading_pair)
                    list_of_trading_pairs_with_USD.append(trading_pair)

                elif "/BTC" in trading_pair:
                    #print(trading_pair)
                    new_counter = new_counter + 1
                    print ( "new_counter=" , new_counter )
                    list_of_trading_pairs_with_BTC.append(trading_pair)

                    data = await exchange_object.fetch_ohlcv ( trading_pair , '1d' )

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
                    #                      connection_to_btc_trading_pairs_4h_ohlcv ,
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
                    current_timestamp = time.time ()
                    last_timestamp_in_df = data_df.tail ( 1 ).index.item () / 1000.0
                    print ( "current_timestamp=" , current_timestamp )
                    print ( "data_df.tail(1).index.item()=" , data_df.tail ( 1 ).index.item () / 1000.0 )

                    # check if the pair is active
                    # if not (current_timestamp - last_timestamp_in_df) < (86400):
                    #     print ( f"not quite active trading pair {trading_pair} on {exchange}" )
                    #     not_active_pair_counter = not_active_pair_counter + 1
                    #     print ( "not_active_pair_counter=" , not_active_pair_counter )
                    #     continue

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
                                     connection_to_btc_trading_pairs_ohlcv ,
                                     if_exists = 'replace' )



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
        # connection_to_btc_trading_pairs_ohlcv.close()

    except ccxt.base.errors.RequestTimeout:
        print ( "found ccxt.base.errors.RequestTimeout error outer" )
        traceback.print_exc ()

        pass



    except Exception as e:
        print(f"problem with {exchange}\n", e)
        traceback.print_exc()

        #await exchange_object.close ()

    finally:
        await exchange_object.close ()
        print ( "exchange object is closed" )


def fetch_historical_btc_pairs_asynchronously(exchanges_list):
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
    #exchanges_list=ccxt.exchanges

    path_to_async_ohlcv_db=os.path.join ( os.getcwd () ,
                                         "datasets" ,
                                         "sql_databases" ,"async_databases",
                                         "2async_all_exchanges_multiple_tables_historical_data_for_btc_trading_pairs.db" )

    path_to_async_ohlcv_db_dir = os.path.join ( os.getcwd () ,
                                            "datasets" ,
                                            "sql_databases","async_databases"  )

    Path ( path_to_async_ohlcv_db_dir ).mkdir ( parents = True , exist_ok = True )
    if os.path.exists ( path_to_async_ohlcv_db ):
        os.remove ( path_to_async_ohlcv_db )
    print ( "before removal of db" )
    # time.sleep(20)
    print ( "after removal of db" )
    #create_empty_database ( path_to_async_ohlcv_db )

    connection_to_btc_trading_pairs_daily_ohlcv = \
        sqlite3.connect ( path_to_async_ohlcv_db)

    # connection_to_btc_trading_pairs_4h_ohlcv = \
    #     sqlite3.connect ( os.path.join ( os.getcwd () ,
    #                                      "datasets" ,
    #                                      "sql_databases" ,
    #                                      "async_all_exchanges_multiple_tables_historical_data_for_btc_trading_pairs_4h.db" ) )

    # coroutines = [await get_hisorical_data_from_exchange_for_many_symbols(exchange ) for exchange in  exchanges_list]
    # await asyncio.gather(*coroutines, return_exceptions = True)
    #
    btc_trading_pair_number = 0
    counter = 0
    global new_counter

    loop=asyncio.get_event_loop()
    tasks=[loop.create_task(get_hisorical_data_from_exchange_for_many_symbols(exchange,
                                                                              counter,
                                                                              connection_to_btc_trading_pairs_daily_ohlcv,
                                                                              )) for exchange in  exchanges_list]

    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
    connection_to_btc_trading_pairs_daily_ohlcv.close()
    # connection_to_btc_trading_pairs_4h_ohlcv.close ()
    end = time.perf_counter ()
    print("time in seconds is ", end-start)
    print ( "time in minutes is " , (end - start)/60.0 )
    print ( "time in hours is " , (end - start) / 60.0/60.0 )
if __name__=="__main__":
    exchange_list=ccxt.exchanges
    how_many_exchanges=len(exchange_list)
    try:
        fetch_historical_btc_pairs_asynchronously(exchange_list[11:20])
    except Exception as e:
        print ( e )
        traceback.print_exc ()


#asyncio.run(get_hisorical_data_from_exchange_for_many_symbols_and_exchanges())

