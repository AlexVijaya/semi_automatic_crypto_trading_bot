import pprint
from typing import List , Any
import talib
#from drop_table_from_database import drop_table_from_database
import traceback
from collections import Counter
import ccxt
import pandas as pd
import sqlite3
import time
import os
import datetime as dt
from pathlib import Path
import datetime
import numpy as np
start_time=time.time()
def get_list_of_all_exchanges():
    '''It get the list of all cryptocurrency exchanges'''
    exchanges=ccxt.exchanges
    #print(exchanges)
    #print(len(exchanges))
    return exchanges
#get_list_of_all_exchanges()

def drop_table_from_database(table_name,path_to_database):
    conn=sqlite3.connect(path_to_database)
    cur=conn.cursor()
    cur.execute('drop table if exists {}'.format(table_name))
    conn.commit()
    conn.close()



path_to_databases = os.path.join ( os.getcwd () , "datasets" , "sql_databases" )
Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )
path_to_db=os.path.join(os.getcwd(),"datasets",
                                    "sql_databases",
                                    "all_exchanges_multiple_tables_historical_data_for_btc_trading_pairs.db")

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
create_empty_database(path_to_db)


def get_USDT_and_BTC_trading_pairs_from_one_exchange(exchange_name):

    exchange_object=getattr(ccxt,exchange_name)()
    #print(type(ccxt.binance()))
    #print(exchange_object)
    exchange_object.load_markets()
    #print(type(exchange_object))
    #print(exchange_object.has)
    list_of_all_symbols_from_exchange=exchange_object.symbols
    list_of_trading_pairs_with_USDT=[]
    list_of_trading_pairs_with_USD = []
    list_of_trading_pairs_with_BTC = []
    for symbol in list_of_all_symbols_from_exchange:
        if "UP/" in symbol or "DOWN/" in symbol or "BEAR/" in symbol or "BULL/" in symbol:
            continue
        if "USDT" in symbol:
            #print(symbol)
            list_of_trading_pairs_with_USDT.append(symbol)
        elif "USD" in symbol and "USDT" not in symbol:
            #print(symbol)
            list_of_trading_pairs_with_USD.append(symbol)

        elif "/BTC" in symbol:
            #print(symbol)
            list_of_trading_pairs_with_BTC.append(symbol)
        else:
            continue


    # list_of_trading_pairs_with_BTC = []
    # for symbol in list_of_all_symbols_from_exchange:
    #     if "UP" in symbol or "DOWN" in symbol or "BEAR" in symbol or "BULL" in symbol:
    #         continue
    #     if "/BTC" in symbol:
    #         print(symbol)
    #         list_of_trading_pairs_with_BTC.append(symbol)
    #     else:
    #         continue

    # print( list_of_trading_pairs_with_USDT)
    #
    # print ( len(list_of_trading_pairs_with_USDT) )
    #
    # print ( list_of_trading_pairs_with_BTC )
    #
    # print ( len ( list_of_trading_pairs_with_BTC ) )

    return list_of_trading_pairs_with_USD,\
           list_of_trading_pairs_with_USDT,\
           list_of_trading_pairs_with_BTC
    #print(exchange_object.symbols)
    #data_for_symbol_and_tf=exchange_object.fetch_ohlcv('BTCUSDT','1d')
    #print(data_for_symbol_and_tf)
get_USDT_and_BTC_trading_pairs_from_one_exchange()

def flatten(l):
    out = []
    for item in l:
        if isinstance(item, (list, tuple)):
            out.extend(flatten(item))
        else:
            out.append(item)
    return out

list_of_all_btc_pairs_from_all_exchanges_without_duplicates=[]
list_of_all_usdt_pairs_from_all_exchanges_without_duplicates=[]
dict_of_all_btc_pairs_from_all_exchanges_without_duplicates={}
dict_of_all_usdt_pairs_from_all_exchanges_without_duplicates={}
flattened_list_of_all_btc_pairs_from_all_exchanges=None
flattened_list_of_all_usdt_pairs_from_all_exchanges=None
def get_USDT_and_BTC_trading_pairs_from_all_exchanges():
    global list_of_all_btc_pairs_from_all_exchanges_without_duplicates
    global list_of_all_usdt_pairs_from_all_exchanges_without_duplicates
    global flattened_list_of_all_btc_pairs_from_all_exchanges
    global flattened_list_of_all_usdt_pairs_from_all_exchanges
    list_of_all_exchanges=get_list_of_all_exchanges ()
    list_of_all_btc_pairs_from_all_exchanges=[]
    list_of_all_usdt_pairs_from_all_exchanges=[]

    for exchange in list_of_all_exchanges:

        try:
            usd_pairs_list, usdt_pairs_list, btc_pairs_list=\
                get_USDT_and_BTC_trading_pairs_from_one_exchange  ( exchange )
            print("+"*80)
            print(exchange)
            #print ( "usd_pairs_list=\n" , usd_pairs_list )
            #print("usdt_pairs_list=\n",usdt_pairs_list)
            #print ( "btc_pairs_list=\n" , btc_pairs_list )
            list_of_all_btc_pairs_from_all_exchanges.append(btc_pairs_list)
            list_of_all_usdt_pairs_from_all_exchanges.append ( usdt_pairs_list )
            # print ( "len ( list_of_all_btc_pairs_from_all_exchanges )= ")
            # print(len(list_of_all_btc_pairs_from_all_exchanges))
            # print ( "len ( list_of_all_usdt_pairs_from_all_exchanges )= " )
            # print ( len ( list_of_all_usdt_pairs_from_all_exchanges ) )
            #time.sleep ( 3 )
            print("-"*80)
        except Exception as e:
            print(f'problem with exchange {exchange}\n', e)
        finally:
            continue

    print("+++++++list_of_all_btc_pairs_from_all_exchanges++++++\n",
          list_of_all_btc_pairs_from_all_exchanges,
          '\nnumber_of_all_btc_pairs_from_all_exchanges=',
          len(list_of_all_btc_pairs_from_all_exchanges))
    #time.sleep ( 30000 )

    flattened_list_of_all_btc_pairs_from_all_exchanges=\
        flatten ( list_of_all_btc_pairs_from_all_exchanges )
    flattened_list_of_all_usdt_pairs_from_all_exchanges = \
        flatten ( list_of_all_usdt_pairs_from_all_exchanges )

    list_of_all_btc_pairs_from_all_exchanges_without_duplicates=\
        list(dict.fromkeys(flatten(list_of_all_btc_pairs_from_all_exchanges)))
    list_of_all_usdt_pairs_from_all_exchanges_without_duplicates = \
        list ( dict.fromkeys ( flatten ( list_of_all_usdt_pairs_from_all_exchanges ) ) )

    print ( "+++++++list_of_all_btc_pairs_from_all_exchanges_without_duplicates++++++\n" ,
            list_of_all_btc_pairs_from_all_exchanges_without_duplicates ,
            '\nnumber_of_usdt_btc_pairs_from_all_exchanges_without_duplicates=' ,
            len ( list_of_all_usdt_pairs_from_all_exchanges_without_duplicates ) )

    #time.sleep(5)
    #dict_with_counted_btc_pairs_without_duplicates=\
    #    dict(Counter(list_of_all_btc_pairs_from_all_exchanges_without_duplicates))
    #print('dict_with_counted_btc_pairs_without_duplicates=\n')
    #pprint.pprint(dict_with_counted_btc_pairs_without_duplicates)

    #time.sleep(30000)
    return list_of_all_btc_pairs_from_all_exchanges_without_duplicates

if __name__=="__main__":
    get_USDT_and_BTC_trading_pairs_from_all_exchanges()

def insert_USDT_and_BTC_trading_pairs_from_all_exchanges_into_db(
        list_of_all_btc_pairs_from_all_exchanges_without_duplicates,
        list_of_all_usdt_pairs_from_all_exchanges_without_duplicates):


    path_to_db_with_USDT_and_btc_pairs = os.path.join ( os.getcwd () , "datasets" ,
                                "sql_databases" ,
                                "btc_and_usdt_pairs_from_all_exchanges.db" )
    create_empty_database ( path_to_db_with_USDT_and_btc_pairs )
    connection_to_btc_and_usdt_trading_pairs=\
        sqlite3.connect ( path_to_db_with_USDT_and_btc_pairs )
    df_of_all_btc_pairs_from_all_exchanges_without_duplicates=\
        pd.DataFrame(list_of_all_btc_pairs_from_all_exchanges_without_duplicates,
                     columns = ["btc_trading_pairs"])
    df_of_all_usdt_pairs_from_all_exchanges_without_duplicates=\
        pd.DataFrame (list_of_all_usdt_pairs_from_all_exchanges_without_duplicates,
                     columns = ["usdt_trading_pairs"] )

    df_of_all_btc_pairs_from_all_exchanges_without_duplicates.to_sql ( "btc_pair" ,
                     connection_to_btc_and_usdt_trading_pairs ,
                     if_exists = 'replace' )

    df_of_all_usdt_pairs_from_all_exchanges_without_duplicates.to_sql ( "usdt_pair" ,
                                                                       connection_to_btc_and_usdt_trading_pairs ,
                                                                       if_exists = 'replace' )
    connection_to_btc_and_usdt_trading_pairs.close()
    pass

if __name__=="__main__":
    insert_USDT_and_BTC_trading_pairs_from_all_exchanges_into_db(
            list_of_all_btc_pairs_from_all_exchanges_without_duplicates,
            list_of_all_usdt_pairs_from_all_exchanges_without_duplicates)


path_to_db=os.path.join(os.getcwd(),"datasets",
                                    "sql_databases",
                                    "all_exchanges_multiple_tables_historical_data_for_btc_trading_pairs.db")
#Path(path_to_db).mkdir(parents=True, exist_ok=True)
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


def get_BTC_and_USDT_pair_ohlcv_from_exchanges(list_of_all_btc_pairs_from_all_exchanges_without_duplicates,
                                      dict_of_all_btc_pairs_from_all_exchanges_without_duplicates,
                                      flattened_list_of_all_btc_pairs_from_all_exchanges,
                                      flattened_list_of_all_usdt_pairs_from_all_exchanges):
    #global flattened_list_of_all_btc_pairs_from_all_exchanges
    # global flattened_list_of_all_usdt_pairs_from_all_exchanges
    header = ['Timestamp' , 'open' , 'high' , 'low' , 'close' , 'volume']
    list_of_all_exchanges = get_list_of_all_exchanges ()
    print("list_of_all_exchanges\n",list_of_all_exchanges)
    #time.sleep(10000)
    list_of_all_btc_pairs_from_all_exchanges = []
    list_of_all_usdt_pairs_from_all_exchanges = []
    dict_of_all_usdt_pairs_with_mirror_levels_from_all_exchanges = {}
    this_many_last_days=90
    # connection_to_btc_trading_pairs_ohlcv = sqlite3.connect ( os.path.join ( os.getcwd () ,
    #                                                         "datasets" ,
    #                                                         "sql_databases" ,
    #                                                         "all_exchanges_multiple_tables_historical_data_for_btc_trading_pairs.db" ))
    connection_to_usdt_trading_pairs_ohlcv =\
        sqlite3.connect ( os.path.join ( os.getcwd () ,
                                         "datasets" ,
                                         "sql_databases" ,
                                         "all_exchanges_multiple_tables_historical_data_for_usdt_trading_pairs.db" ) )
    path_to_db_with_USDT_and_btc_pairs = os.path.join ( os.getcwd () , "datasets" ,
                                                        "sql_databases" ,
                                                        "btc_and_usdt_pairs_from_all_exchanges.db" )

    connection_to_btc_and_usdt_trading_pairs = \
        sqlite3.connect ( path_to_db_with_USDT_and_btc_pairs )

    # mirror_level_df=pd.DataFrame(columns = ['USDT_pair', 'exchange', 'mirror_level',
    #                                          'timestamp_for_low','timestamp_for_high',
    #                                          'low','high','open_time_of_candle_with_legit_low',
    #                                         'open_time_of_candle_with_legit_high'])
    try:
        drop_table_from_database("mirror_levels",path_to_db_with_USDT_and_btc_pairs)
    except:
        print("cant drop table from db")
    for exchange in list_of_all_exchanges:

        try:
            usd_pairs_list , usdt_pairs_list , btc_pairs_list = \
                get_USDT_and_BTC_trading_pairs_from_one_exchange ( exchange )
            print ( "+" * 80 )
            print ( exchange )
            print("len(list_of_all_exchanges)=",len(list_of_all_exchanges))

            print ( "\nusd_pairs_list=\n" , usd_pairs_list )
            exchange_object = getattr ( ccxt , exchange ) ()
            exchange_object.load_markets ()
            exchange_object.enableRateLimit = True

            for usdt_pair in usdt_pairs_list:
                try:
                    list_of_all_usdt_pairs_from_all_exchanges.append ( usdt_pair )
                    print ( f'{len ( list_of_all_usdt_pairs_from_all_exchanges )} out of '
                            f'{len ( flattened_list_of_all_usdt_pairs_from_all_exchanges )}'
                            f' have been added' )
                    #      /len(list_of_all_btc_pairs_from_all_exchanges_without_duplicates))
                    data = exchange_object.fetch_ohlcv ( usdt_pair , '1d' )
                    data_df = pd.DataFrame ( data , columns = header ).set_index ( 'Timestamp' )
                    print ( "=" * 80 )
                    print ( f'ohlcv for {usdt_pair} on exchange {exchange}\n' )
                    print ( data_df )
                    data_df['trading_pair'] = usdt_pair
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
                    print("data_df\n",data_df)


                    data_df.to_sql ( f"{usdt_pair}_on_{exchange}" ,
                                     connection_to_usdt_trading_pairs_ohlcv ,
                                     if_exists = 'replace' )


                except:
                    continue

            print ( "-" * 80 )
        except Exception as e:
            print ( f'problem with exchange {exchange}\n',e )
            traceback.print_exc ()
        finally:
            continue
    print("len(list_of_all_btc_pairs_from_all_exchanges)\n",
          len(list_of_all_btc_pairs_from_all_exchanges))



    print('difference between two lists=\n',
          list((Counter(list_of_all_btc_pairs_from_all_exchanges_without_duplicates)-
               Counter(list_of_all_btc_pairs_from_all_exchanges)).elements()))
    connection_to_usdt_trading_pairs_ohlcv.close()
    connection_to_btc_and_usdt_trading_pairs.close ()

if __name__=="__main__":
    get_BTC_and_USDT_pair_ohlcv_from_exchanges(
        list_of_all_btc_pairs_from_all_exchanges_without_duplicates,
        dict_of_all_btc_pairs_from_all_exchanges_without_duplicates,
        flattened_list_of_all_btc_pairs_from_all_exchanges,
        flattened_list_of_all_usdt_pairs_from_all_exchanges)

end_time=time.time()
overall_time=end_time-start_time
print('overall time in minutes=', overall_time/60.0)
print('overall time in hours=', overall_time/3600.0)
print('overall time=', str(datetime.timedelta(seconds = overall_time)))
print('start_time=', start_time)
print('end_time=', end_time)