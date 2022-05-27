import pprint
import time
import traceback

import talib
import os
from drop_table_from_database import  drop_table_from_database
from pathlib import Path
import sqlite3
import pandas as pd
import asyncio
from Heiken_Ashi_calculation import HA
from binance import AsyncClient, BinanceSocketManager
from insert_only_one_binance_trading_pair_ohlc_into_db import insert_only_one_binance_trading_pair_ohlc_into_db
import pprint
# async def main():
#     start_time=time.perf_counter()
#     client_for_binance=await AsyncClient.create()
#     # exchange_info=await client_for_binance.get_exchange_info()
#     # tickers=await client_for_binance.get_all_tickers()
#     # end_time=time.perf_counter()
#     # print('overall time=',end_time-start_time)
#     # print ( '\nexchange_info=' , exchange_info )
#     # print ( '\ntickers=' , tickers )
#     res=await asyncio.gather(
#         client_for_binance.get_exchange_info(),
#         client_for_binance.get_all_tickers())
#     print(res)
#     await client_for_binance.close_connection()
# if __name__=="__main__":
#     loop=asyncio.get_event_loop()
#     loop.run_until_complete(main())
def create_frame(msg):
    #print(type(msg))
    df=pd.DataFrame.from_records([msg["k"]])
    df['t']=pd.to_datetime(df['t'],unit = 'ms')
    df['T'] = pd.to_datetime ( df['T'] , unit = 'ms' )
    # df['o']=df['o'].astype(float)
    # df['h'] = df['h'].astype ( float )
    # df['l'] = df['l'].astype ( float )
    # df['c'] = df['c'].astype ( float )
    # df=df.rename(columns = {'o':'open','h':'high','l':'low','c':'close','t':'time','s':'trading_pair'})
    # heiken_ashi_df=HA(df)
    # heiken_ashi_df=heiken_ashi_df.rename(columns={'open':'HA_open','high':'HA_high','low':'HA_low','close':'HA_close'})
    # df=df.join(heiken_ashi_df)
    #interval_df=pd.DataFrame()
    # if df.at[0,"x"]==True:
    #     print("x is True")
    #     interval_df=df
    #     # if i==0:
        #     next_interval_df = pd.DataFrame ()
        #     current_interval_df = pd.DataFrame ()
        #
        #     current_interval_df['sar'] = talib.SAR ( df.high , df.low ,
        #                                        acceleration = 0.02 ,
        #                                        maximum = 0.2 )
        # if i>0:
        #     next_interval_df["sar"]=talib.SAR ( current_interval_df.high , current_interval_df.low ,
        #                                        acceleration = 0.02 ,
        #                                        maximum = 0.2 )
        #
        # i=i+1


    #print(df.head(1000).to_string())
    #print ( heiken_ashi_df.head ( 1000 ).to_string () )
    #df=df.loc[:,["s","E","k"]]
    return df

async def main():
    client_for_binance = await AsyncClient.create ()
    await kline_listener ( client_for_binance )

    await client_for_binance.close_connection()

async def order_book(client_for_binance,symbol):
    order_book = await client_for_binance.get_order_book ( symbol = symbol )
    print ( order_book )

# async with bsm.trade_socket(symbol = symbol) as ts:
#         while True:
#             result=await ts.recv()
#             #print(result)



async def kline_listener(client_for_binance):
    # symbol = 'BTCUSDT'
    # path_to_databases = os.path.join ( os.getcwd () , "datasets" , "sql_databases" )
    # Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )
    # connection_binance_interval_ohlc_db = sqlite3.connect (
    #     os.path.join ( os.getcwd () , "datasets" ,
    #                    "sql_databases" ,
    #                    "binance_interval_ohlc.db" ) )
    # drop_table_from_database ( f"binance_interval_ohlc_for_{symbol}" ,
    #                            os.path.join ( os.getcwd () , "datasets" ,
    #                                           "sql_databases" ,
    #                                           "binance_interval_ohlc.db" ) )

    interval = '1m'
    start_moment = '1 day ago, UTC'
    symbol = 'BTCUSDT'
    path_to_databases = os.path.join ( os.getcwd () , "datasets" , "sql_databases" )
    Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )
    connection_binance_interval_ohlc_db = sqlite3.connect ( os.path.join ( os.getcwd () ,
                                                            "datasets" ,
                                                            "sql_databases" ,
                                                            "binance_1_minute_historical_data.db" ) )
    path_to_database_table_in_which_to_be_dropped = os.path.join ( os.getcwd () , "datasets" ,
                                                                   "sql_databases" ,
                                                                   "binance_1_minute_historical_data.db" )
    drop_table_from_database ( f"binance_interval_ohlc_for_{symbol}" ,
                                                        os.path.join ( os.getcwd () , "datasets" ,
                                                        "sql_databases" ,
                                                        "binance_interval_ohlc.db" ) )
    # drop_table_from_database ( f"binance_interval_ohlc_for_{symbol}" ,
    #                            os.path.join ( os.getcwd () , "datasets" ,
    #                                           "sql_databases" ,
    #                                           "binance_interval_ohlc.db" ) )

    insert_only_one_binance_trading_pair_ohlc_into_db ( symbol , interval ,
                                                        connection_binance_interval_ohlc_db ,
                                                        path_to_database_table_in_which_to_be_dropped ,
                                                        start_moment )







    bsm=BinanceSocketManager(client_for_binance)


    counter =1
    #output_df3=pd.DataFrame()
    try:
        async with bsm.kline_socket(symbol=symbol) as stream:
            while True:
                res=await stream.recv()
                df=create_frame ( res )
                print(df.to_string())
                if df.at[0,'x']==True:
                    last_rows_in_db =pd.read_sql_query(f'''select * from (SELECT * 
                                                        from ohlc_for_BTCUSDT ORDER BY id 
                                                        DESC LIMIT 60) order by id asc ;
                                                        ''',connection_binance_interval_ohlc_db)




                    df['id']=1439+counter
                    df['t'] = pd.to_datetime ( df['t'] , unit = 'ms' )
                    df['t']=df['t'].dt.tz_localize('UTC').dt.tz_convert('Europe/Moscow')
                    df['t'] = df['t'].dt.tz_localize ( None )
                    #df['t']=df['t'].dt.tz_localize('UTC').dt.tz_convert('Europe/Moscow')df['t'] = df['t'].replace ( tzinfo=None )
                    df['T'] = pd.to_datetime ( df['T'] , unit = 'ms' )
                    df['o'] = df['o'].astype ( float )
                    df['h'] = df['h'].astype ( float )
                    df['l'] = df['l'].astype ( float )
                    df['c'] = df['c'].astype ( float )
                    df = df.rename ( columns = {'o': 'open' , 'h': 'high' , 'l': 'low' , 'c': 'close' , 't': 'open_time' ,
                                                's': 'trading_pair', 'v':'volume','q':'quote_volume'} )
                    print("df.tail(1)=\n",df.tail(1).to_string() )
                    df.drop(["T","i","f","L","n","x","V","Q","quote_volume","B"],axis=1,inplace=True)
                    print ( df.columns.values.tolist () )
                    heiken_ashi_df = HA ( df )
                    heiken_ashi_df = heiken_ashi_df.rename (
                        columns = {'open': 'HA_open' , 'high': 'HA_high' , 'low': 'HA_low' , 'close': 'HA_close'} )
                    df = df.join ( heiken_ashi_df )
                    #df['id']=counter
                    # if counter>1:
                    #     prev_row_df=pd.read_sql_query('''select * from
                    #     binance_interval_ohlc_for_BTCUSDT where
                    #     id=(select max(id) from binance_interval_ohlc_for_BTCUSDT)
                    #     ''',connection_binance_interval_ohlc_db)


                    if str(last_rows_in_db.at[last_rows_in_db.index[-1],'open_time'])== str(df.at[df.index[-1],'open_time']):
                        print ( "+++++++++++++++++++++++++++++++++++++++++++" )
                        print ( last_rows_in_db.at[last_rows_in_db.index[-1] , 'open_time'] )
                        print ( df.at[df.index[-1] , 'open_time'] )
                        continue

                    joined_df_of_multiple_rows_to_find_sar=pd.concat([last_rows_in_db,df])
                    print("joined_df_of_multiple_rows_to_find_sar\n ",joined_df_of_multiple_rows_to_find_sar.to_string() )
                    joined_df_of_multiple_rows_to_find_sar["sar"] = talib.SAR ( joined_df_of_multiple_rows_to_find_sar.high ,
                                                   joined_df_of_multiple_rows_to_find_sar.low ,
                                                   acceleration = 0.02 ,
                                                   maximum = 0.2 )
                    print(joined_df_of_multiple_rows_to_find_sar['sar'].tail(1))
                    df['sar']=joined_df_of_multiple_rows_to_find_sar['sar'].tail(1)
                    df['index'] = joined_df_of_multiple_rows_to_find_sar['id'].tail ( 1 )
                    joined_df_of_multiple_rows_to_find_sar.at[joined_df_of_multiple_rows_to_find_sar.index[-1],"index"]=\
                        joined_df_of_multiple_rows_to_find_sar.at[joined_df_of_multiple_rows_to_find_sar.index[-1],"id"]
                    print ( "joined_df_of_multiple_rows_to_find_sar.tail(1)\n" , joined_df_of_multiple_rows_to_find_sar.tail ( 1 ).to_string() )
                    print("\n\n\ndf=\n",df.to_string() )
                    #df=joined_df_of_two_row_to_find_sar.tail(1)
                    print ( "joined_df_of_two_row_to_find_sar\n " , joined_df_of_multiple_rows_to_find_sar.to_string() )
                    df.to_sql ( f"ohlc_for_{symbol}" ,
                                connection_binance_interval_ohlc_db ,
                                if_exists = 'append' , index = False )

                    counter = counter + 1




                #output_df3=pd.concat([output_df3,output_df2],axis=0)

                #
                # prev_df=output_df
                #
                # print ( "prev_df\n" , prev_df )

                #pprint.pprint (res['k']['o'] )
                #print (create_frame(res))

                # if counter==5:
                #     counter=0
                #     # order_book=await client_for_binance.get_order_book(symbol=symbol)
                #     # pprint.pprint (order_book)
                #     loop.call_soon ( asyncio.create_task , order_book ( client_for_binance , symbol ) )
    except Exception as e:
        print (e)
        traceback.print_exc()


if __name__=="__main__":
    loop=asyncio.get_event_loop()
    loop.run_until_complete(main())