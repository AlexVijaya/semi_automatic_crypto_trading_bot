# -*- coding: utf-8 -*-
import sqlite3
import asyncio
import os
import sys
import time
import traceback

import pandas as pd
import talib
import datetime as dt
import ccxt.async_support as ccxt  # noqa: E402
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
def counter_gen(n=0):

    while True:
        n += 1
        yield n
new_counter=0

async def get_hisorical_data_from_exchange_for_many_symbols(exchange,counter,connection_to_usdt_trading_pairs_ohlcv ):
    print("exchange=",exchange)
    global new_counter
    try:
        exchange_object = getattr ( ccxt , exchange ) ()
        exchange_object.enableRateLimit = True
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

        for trading_pair in list_of_all_symbols_from_exchange:
            # for item in counter_gen():
            #     print ("item=",item)

            counter =counter+1

            try:
                print ( "exchange=" , exchange )
                print ( "usdt_pair=" , trading_pair )
                if "UP/" in trading_pair or "DOWN/" in trading_pair or "BEAR/" in trading_pair or "BULL/" in trading_pair:
                    continue
                if "/USDT" in trading_pair:
                    new_counter = new_counter + 1
                    print("new_counter=",new_counter)
                    list_of_trading_pairs_with_USDT.append(trading_pair)
                    # print ( f"list_of_trading_pairs_with_USDT_on_{exchange}\n" ,
                    #         list_of_trading_pairs_with_USDT )
                    data = await exchange_object.fetch_ohlcv ( trading_pair , '1d' )
                    print ( f"counter_for_{exchange}=" , counter )
                    header = ['Timestamp' , 'open' , 'high' , 'low' , 'close' , 'volume']
                    data_df = pd.DataFrame ( data , columns = header ).set_index ( 'Timestamp' )
                    print ( "=" * 80 )
                    print ( f'ohlcv for {trading_pair} on exchange {exchange}\n' )
                    print ( data_df )
                    data_df['trading_pair'] = trading_pair
                    data_df['exchange'] = exchange

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
                                     connection_to_usdt_trading_pairs_ohlcv ,
                                     if_exists = 'replace' )


                elif "/USD" in trading_pair and "/USDT" not in trading_pair:
                    #print(trading_pair)
                    list_of_trading_pairs_with_USD.append(trading_pair)

                elif "/BTC" in trading_pair:
                    #print(trading_pair)
                    list_of_trading_pairs_with_BTC.append(trading_pair)
                else:
                    continue




                    #print ( "data=" , data )
            except ccxt.base.errors.RequestTimeout:
                print("found ccxt.base.errors.RequestTimeout error inner")
                pass

            except Exception as e:
                print(f"problem with {trading_pair} on {exchange}\n", e)
                traceback.print_exc ()
                continue
        await exchange_object.close ()
        # connection_to_usdt_trading_pairs_ohlcv.close()

    except ccxt.base.errors.RequestTimeout:
        print ( "found ccxt.base.errors.RequestTimeout error outer" )
        traceback.print_exc ()
        pass

    except Exception as e:
        print(f"problem with {exchange}\n", e)
        traceback.print_exc()
        #await exchange_object.close ()


def fetch_historical_usdt_pairs_asynchronously():
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
    exchanges_list=ccxt.exchanges

    connection_to_usdt_trading_pairs_ohlcv = \
        sqlite3.connect ( os.path.join ( os.getcwd () ,
                                         "datasets" ,
                                         "sql_databases" ,
                                         "async_all_exchanges_multiple_tables_historical_data_for_usdt_trading_pairs.db" ) )

    # coroutines = [await get_hisorical_data_from_exchange_for_many_symbols(exchange ) for exchange in  exchanges_list]
    # await asyncio.gather(*coroutines, return_exceptions = True)
    #
    usdt_trading_pair_number = 0
    counter = 0
    global new_counter
    loop=asyncio.get_event_loop()
    tasks=[loop.create_task(get_hisorical_data_from_exchange_for_many_symbols(exchange,
                                                                              counter,
                                                                              connection_to_usdt_trading_pairs_ohlcv)) for exchange in  exchanges_list]

    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
    connection_to_usdt_trading_pairs_ohlcv.close()
    end = time.perf_counter ()
    print("time in seconds is ", end-start)
    print ( "time in minutes is " , (end - start)/60.0 )
    print ( "time in hours is " , (end - start) / 60.0/60.0 )
if __name__=="__main__":
    fetch_historical_usdt_pairs_asynchronously()
#asyncio.run(get_hisorical_data_from_exchange_for_many_symbols_and_exchanges())

