import shutil
import time
import os
import pandas as pd
import datetime
import sqlite3
from pathlib import Path
import traceback
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pdfkit
import imgkit
import plotly.express as px
from datetime import datetime
#from if_asset_is_close_to_hh_or_ll import find_asset_close_to_hh_and_ll


from sqlalchemy_utils import create_database,database_exists
import db_config
# from sqlalchemy import MetaData
from sqlalchemy import inspect
import logging
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base



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





def import_1d_ohlcv_and_mirror_levels_for_plotting(usdt_trading_pair,
                                                exchange,mirror_level):
    engine_for_usdt_trading_pairs_ohlcv_ready_for_rebound_db_1d_timeframe , \
    connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_1d_timeframe = \
        connect_to_postres_db (
            f"ohlcv_1d_tables_with_pairs_ready_to_rebound_from_mirror_level" )

    historical_1d_data_for_usdt_trading_pair_df=\
        pd.read_sql ( f'''select * from "{usdt_trading_pair}_on_{exchange}_at_{mirror_level}" ;'''  ,
                             connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_1d_timeframe )

    connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_1d_timeframe.close()
    return historical_1d_data_for_usdt_trading_pair_df


def import_some_period_ohlcv_and_mirror_levels_for_plotting(usdt_trading_pair,
                                                exchange,mirror_level,lower_timeframe_for_mirror_level_rebound_trading='1h'):
    engine_for_usdt_trading_pairs_ohlcv_ready_for_rebound_db_some_period_timeframe , \
    connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_some_period_timeframe = \
        connect_to_postres_db (
            f"ohlcv_{lower_timeframe_for_mirror_level_rebound_trading}_tables_with_pairs_ready_to_rebound_from_mirror_level" )

    historical_some_period_data_for_usdt_trading_pair_df=\
        pd.read_sql ( f'''select * from "{usdt_trading_pair}_on_{exchange}_at_{mirror_level}" ;'''  ,
                             connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_some_period_timeframe )

    connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_some_period_timeframe.close()
    return historical_some_period_data_for_usdt_trading_pair_df



def plot_ohlcv_chart_with_mirror_levels_from_given_exchange (lower_timeframe_for_mirror_level_rebound_trading='1h'):
    start_time=time.time()

    engine_for_usdt_trading_pairs_ohlcv_ready_for_rebound_db_1d_timeframe , \
    connection_to_usdt_trading_pairs_ohlcv_ready_for_rebound_1d_timeframe = \
        connect_to_postres_db (
            f"ohlcv_1d_tables_with_pairs_ready_to_rebound_from_mirror_level" )
    inspector=inspect(engine_for_usdt_trading_pairs_ohlcv_ready_for_rebound_db_1d_timeframe)
    list_of_ohlcv_table_names_with_recent_mirror_highs_and_lows=inspector.get_table_names()

    print("list_of_ohlcv_table_names_with_recent_mirror_highs_and_lows=\n",
          list_of_ohlcv_table_names_with_recent_mirror_highs_and_lows)
    print ( "len ( list_of_ohlcv_table_names_with_recent_mirror_highs_and_lows )" )
    print(len(list_of_ohlcv_table_names_with_recent_mirror_highs_and_lows))

    engine_for_btc_and_usdt_trading_pairs_db , connection_to_btc_and_usdt_trading_pairs = \
        connect_to_postres_db ( "btc_and_usdt_pairs_from_all_exchanges" )

    mirror_levels_df = pd.read_sql ( f'''select * from mirror_levels_without_duplicates ;''' ,
                                     connection_to_btc_and_usdt_trading_pairs )
    #print ( "mirror_levels_df\n" , mirror_levels_df )

    # delete previously plotted charts


    # try:
    #     folder_to_be_deleted = os.path.join ( os.getcwd () ,
    #                                           'datasets' ,
    #                                           'plots' ,
    #                                           'crypto_exchange_plots_ready_for_rebound' )
    #     shutil.rmtree ( folder_to_be_deleted )
    #     pass
    # except Exception as e:
    #     print ( "error deleting folder \n" , e )
    #     pass

    for row_number in range(0,len(mirror_levels_df)):
        usdt_trading_pair =mirror_levels_df.loc[row_number,'USDT_pair']
        exchange = mirror_levels_df.loc[row_number,'exchange']
        mirror_level = mirror_levels_df.loc[row_number , 'mirror_level']

        joint_string_for_table_name = mirror_levels_df.loc[row_number , "USDT_pair"] + \
                                          '_on_' + mirror_levels_df.loc[row_number , "exchange"] + \
                                          '_at_' + f'{mirror_levels_df.loc[row_number , "mirror_level"]}'
        # print("joint_string_for_table_name")
        # print ( joint_string_for_table_name )
        timestamp_for_low_original = mirror_levels_df.loc[row_number , 'timestamp_for_low']
        timestamp_for_high_original = mirror_levels_df.loc[row_number , 'timestamp_for_high']

        timestamp_for_low = (mirror_levels_df.loc[row_number , 'timestamp_for_low']) / 1000.0
        timestamp_for_high = (mirror_levels_df.loc[row_number , 'timestamp_for_high']) / 1000.0

        if joint_string_for_table_name in list_of_ohlcv_table_names_with_recent_mirror_highs_and_lows:
            #print ( "joint_string_for_table_name" )
            print ( joint_string_for_table_name )
            counter=list_of_ohlcv_table_names_with_recent_mirror_highs_and_lows.index(joint_string_for_table_name)
            print("counter")
            print(counter)



            try:

                open_time_of_candle_with_legit_low = mirror_levels_df.loc[row_number ,
                                                    'open_time_of_candle_with_legit_low']
                open_time_of_candle_with_legit_high = mirror_levels_df.loc[row_number ,
                                                    'open_time_of_candle_with_legit_high']

                # open_time_of_candle_with_legit_high = datetime.strptime ( open_time_of_candle_with_legit_high_str ,
                #                                                           '%Y-%m-%d %H:%M:%S' )
                # open_time_of_candle_with_legit_low = datetime.strptime ( open_time_of_candle_with_legit_low_str ,
                #                                                           '%Y-%m-%d %H:%M:%S' )

                # open_time_of_candle_with_legit_high=open_time_of_candle_with_legit_high.strftime("%d-%m-%Y %H:%M:%S")
                # open_time_of_candle_with_legit_low = open_time_of_candle_with_legit_low.strftime ( "%d-%m-%Y %H:%M:%S" )

                # open_time_of_candle_with_legit_high=datetime.strptime ( open_time_of_candle_with_legit_high ,
                #                                                           '%d-%m-%Y %H:%M:%S' )
                # open_time_of_candle_with_legit_low=datetime.strptime ( open_time_of_candle_with_legit_low ,
                #                                                           '%d-%m-%Y %H:%M:%S' )
                #print (type(open_time_of_candle_with_legit_high))

                try:
                    historical_1d_data_for_usdt_trading_pair_df=\
                        import_1d_ohlcv_and_mirror_levels_for_plotting ( usdt_trading_pair,exchange,mirror_level)
                    traceback.print_exc ()
                    #time.sleep ( 100 )
                except Exception as e:
                    print(f"problem with plotting {usdt_trading_pair} on {exchange} at {mirror_level}")
                    #traceback.print_exc()

                    #time.sleep(100)
                    continue

                try:
                    historical_some_period_data_for_usdt_trading_pair_df=\
                        import_some_period_ohlcv_and_mirror_levels_for_plotting ( usdt_trading_pair,exchange,mirror_level)

                    #time.sleep ( 100 )
                except Exception as e:
                    print(f"problem with plotting {usdt_trading_pair} on {exchange} at {mirror_level}")
                    traceback.print_exc()

                    #time.sleep(100)
                    continue


                plot_number=row_number+1
                print(f'{usdt_trading_pair} on {exchange} is number {row_number+1} '
                      f'out of {len(mirror_levels_df)}')
                print ( "historical_1d_data_for_usdt_trading_pair_df\n" ,
                       historical_1d_data_for_usdt_trading_pair_df )

                usdt_trading_pair_without_slash=usdt_trading_pair.replace("/","")

                if ":" in usdt_trading_pair_without_slash:
                    print ( 'found pair with :' , usdt_trading_pair_without_slash )
                    usdt_trading_pair_without_slash=usdt_trading_pair_without_slash.replace(":",'__')
                    print( 'found pair with :',usdt_trading_pair_without_slash)

                historical_1d_data_for_usdt_trading_pair_df_list_of_highs=\
                    historical_1d_data_for_usdt_trading_pair_df['high'].to_list()

                historical_1d_data_for_usdt_trading_pair_df_list_of_lows = \
                    historical_1d_data_for_usdt_trading_pair_df['low'].to_list ()

                open_time_of_another_high_list=[]
                open_time_of_another_low_list=[]
                open_time_of_another_high_list_df=\
                    pd.DataFrame(columns = ['open_time_of_high','mirror_level'])
                open_time_of_another_low_list_df =\
                    pd.DataFrame (columns = ['open_time_of_low','mirror_level'])
                for index, high in enumerate(historical_1d_data_for_usdt_trading_pair_df_list_of_highs):
                    if high==mirror_level:
                        #print('high of mirror level is found more than one time\n ')
                        #print (f'index={index}')
                        open_time_of_another_high=\
                            historical_1d_data_for_usdt_trading_pair_df.loc[index,'open_time']
                        #print ( f'open_time_of_another_high={open_time_of_another_high}' )
                        open_time_of_another_high_list.append(open_time_of_another_high)
                        if open_time_of_another_high!=open_time_of_candle_with_legit_high:
                            open_time_of_another_high_list.append ( open_time_of_another_high )
                            historical_1d_data_for_usdt_trading_pair_df.loc[historical_1d_data_for_usdt_trading_pair_df["open_time"] ==
                                                                         open_time_of_another_high, ['open_time_of_another_high']]= open_time_of_another_high

                            historical_1d_data_for_usdt_trading_pair_df.loc[
                                historical_1d_data_for_usdt_trading_pair_df["open_time"] ==
                                open_time_of_another_high , ['mirror_level_of_another_high']] =mirror_level
                        else:
                            historical_1d_data_for_usdt_trading_pair_df.loc[historical_1d_data_for_usdt_trading_pair_df["open_time"] ==
                                                                         open_time_of_another_high, ['open_time_of_another_high']]= None

                            historical_1d_data_for_usdt_trading_pair_df.loc[
                                historical_1d_data_for_usdt_trading_pair_df["open_time"] ==
                                open_time_of_another_high , ['mirror_level_of_another_high']] = None

                for index, open_time_of_another_high in enumerate(open_time_of_another_high_list):
                    open_time_of_another_high_list_df.at[ index, 'open_time_of_high']=open_time_of_another_high
                    open_time_of_another_high_list_df.at[index, 'mirror_level'] = mirror_level

                for index, low in enumerate(historical_1d_data_for_usdt_trading_pair_df_list_of_lows):
                    if low==mirror_level:
                        #print('low of mirror level is found more than one time\n')
                        #print ( f'index={index}' )
                        open_time_of_another_low = \
                            historical_1d_data_for_usdt_trading_pair_df.loc[index , 'open_time']
                        #print ( f'open_time_of_another_low={open_time_of_another_low}' )
                        if open_time_of_another_low!=open_time_of_candle_with_legit_low:
                            open_time_of_another_low_list.append ( open_time_of_another_low )

                            historical_1d_data_for_usdt_trading_pair_df.loc[historical_1d_data_for_usdt_trading_pair_df["open_time"] ==
                                                                         open_time_of_another_low, ['open_time_of_another_low']]=\
                                open_time_of_another_low

                            historical_1d_data_for_usdt_trading_pair_df.loc[
                                historical_1d_data_for_usdt_trading_pair_df["open_time"] ==
                                open_time_of_another_low , ['mirror_level_of_another_low']] =mirror_level

                        else:
                            historical_1d_data_for_usdt_trading_pair_df.loc[historical_1d_data_for_usdt_trading_pair_df["open_time"] ==
                                                                         open_time_of_another_low, ['open_time_of_another_low']]= None

                            historical_1d_data_for_usdt_trading_pair_df.loc[
                                historical_1d_data_for_usdt_trading_pair_df["open_time"] ==
                                open_time_of_another_low , ['mirror_level_of_another_low']] =None

                for index, open_time_of_another_low in enumerate(open_time_of_another_low_list):
                    open_time_of_another_low_list_df.at[ index, 'open_time_of_low']=open_time_of_another_low
                    open_time_of_another_low_list_df.at[index, 'mirror_level'] = mirror_level

                #print("+++++++++++++++++++++++++")
                #print ( "open_time_of_another_high_list_df\n" , open_time_of_another_high_list_df )

                #print ( "open_time_of_another_low_list_df\n" , open_time_of_another_low_list_df )
                print ( "+++++++++++++++++++++++++" )
                #print ( f'open_time_of_another_low_list={open_time_of_another_low_list}' )
                #print ( f'open_time_of_another_high_list={open_time_of_another_high_list}' )
                #print("-"*80)
                print ( "historical_1d_data_for_usdt_trading_pair_df\n" ,
                        historical_1d_data_for_usdt_trading_pair_df )

                number_of_charts = 3
                how_many_last_rows_to_plot = 0
                #plotting charts with mirror levels
                try:
                    where_to_plot_html = os.path.join ( os.getcwd () ,
                                                   'datasets' ,
                                                   'plots' ,
                                                   'crypto_exchange_plots_ready_for_rebound' ,
                                                   'crypto_exchange_plots_html',
                                                   f'{plot_number}_{usdt_trading_pair_without_slash}_on_{exchange}.html')

                    where_to_plot_pdf = os.path.join ( os.getcwd () ,
                                                       'datasets' ,
                                                       'plots' ,
                                                       'crypto_exchange_plots_ready_for_rebound' ,
                                                       'crypto_exchange_plots_pdf',
                                                       f'{plot_number}_{usdt_trading_pair_without_slash}.pdf' )
                    where_to_plot_svg = os.path.join ( os.getcwd () ,
                                                       'datasets' ,
                                                       'plots' ,
                                                       'crypto_exchange_plots_ready_for_rebound' ,
                                                       'crypto_exchange_plots_svg' ,
                                                       f'{plot_number}_{usdt_trading_pair_without_slash}_on_{exchange}.svg' )
                    where_to_plot_jpg = os.path.join ( os.getcwd () ,
                                                       'datasets' ,
                                                       'plots' ,
                                                       'crypto_exchange_plots_ready_for_rebound' ,
                                                       'crypto_exchange_plots_jpg' ,
                                                       f'{plot_number}_{usdt_trading_pair_without_slash}_on_{exchange}.jpg' )

                    where_to_plot_png = os.path.join ( os.getcwd () ,
                                                       'datasets' ,
                                                       'plots' ,
                                                       'crypto_exchange_plots_ready_for_rebound' ,
                                                       'crypto_exchange_plots_png' ,
                                                       f'{plot_number}_{usdt_trading_pair_without_slash}_on_{exchange}.png' )
                    #create directory for crypto_exchange_plots parent folder
                    # if it does not exists
                    path_to_databases = os.path.join ( os.getcwd () ,
                                                       'datasets' ,
                                                       'plots' ,
                                                       'crypto_exchange_plots_ready_for_rebound' )
                    Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )
                    #create directories for all hh images
                    formats=['png','svg','pdf','html','jpg']
                    for img_format in formats:
                        path_to_special_format_images_of_mirror_charts =\
                            os.path.join ( os.getcwd () ,
                                                           'datasets' ,
                                                           'plots' ,
                                                           'crypto_exchange_plots_ready_for_rebound',
                                                           f'crypto_exchange_plots_{img_format}')
                        Path ( path_to_special_format_images_of_mirror_charts ).mkdir ( parents = True , exist_ok = True )

                    fig = make_subplots ( rows = number_of_charts , cols = 1 ,
                                          shared_xaxes = False ,
                                          subplot_titles =
                                          ('1d','1d',lower_timeframe_for_mirror_level_rebound_trading),

                                           vertical_spacing = 0.05 )
                    fig.update_layout ( height = 1500 * number_of_charts,
                                        width = 4000  ,
                                        title_text = f'{usdt_trading_pair} '
                                                     f'on {exchange} with mirror level={mirror_level}' ,
                                        font = dict (
                                            family = "Courier New, monospace" ,
                                            size = 40 ,
                                            color = "RebeccaPurple"
                                        ) )
                    fig.update_xaxes ( rangeslider = {'visible': False} , row = 1 , col = 1 )
                    fig.update_xaxes ( rangeslider = {'visible': False} , row = 2 , col = 1 )
                    fig.update_xaxes ( rangeslider = {'visible': False} , row = 3 , col = 1 )
                    config = dict ( {'scrollZoom': True} )
                    # print(type("historical_1d_data_for_usdt_trading_pair_df['open_time']\n",
                    #            historical_1d_data_for_usdt_trading_pair_df.loc[3,'open_time']))


                    try:
                        fig.add_trace ( go.Candlestick ( name = f'{usdt_trading_pair} on {exchange}' ,
                                                         x = historical_1d_data_for_usdt_trading_pair_df['open_time'] ,
                                                         open = historical_1d_data_for_usdt_trading_pair_df['open'] ,
                                                         high = historical_1d_data_for_usdt_trading_pair_df['high'] ,
                                                         low = historical_1d_data_for_usdt_trading_pair_df['low'] ,
                                                         close = historical_1d_data_for_usdt_trading_pair_df['close'] ,
                                                         increasing_line_color = 'green' , decreasing_line_color = 'red'
                                                         ) , row = 1 , col = 1 , secondary_y = False )
                    except Exception as e:
                        print ( "error=" , e )
                        traceback.print_exc ()


                    row_number_of_low = historical_1d_data_for_usdt_trading_pair_df.index[
                        historical_1d_data_for_usdt_trading_pair_df["Timestamp"] == timestamp_for_low_original].values
                    row_number_of_high = historical_1d_data_for_usdt_trading_pair_df.index[
                        historical_1d_data_for_usdt_trading_pair_df["Timestamp"] == timestamp_for_high_original].values

                    row_number_of_low = int ( row_number_of_low[0] )
                    row_number_of_high = int ( row_number_of_high[0] )

                    print ( "row_number_of_low=" , row_number_of_low )
                    print ( "row_number_of_high=" , row_number_of_high )

                    print ( "timestamp_for_low=" , timestamp_for_low )
                    print ( "timestamp_for_high=" , timestamp_for_high )

                    try:
                        how_many_last_rows_to_plot = 0
                        if row_number_of_low > row_number_of_high:
                            row_for_plotting_start = row_number_of_high - 20
                            how_many_last_rows_to_plot = \
                                len ( historical_1d_data_for_usdt_trading_pair_df ) - row_for_plotting_start
                            print ( "how_many_last_rows_to_plot=" , how_many_last_rows_to_plot )
                        else:
                            row_for_plotting_start = row_number_of_low - 20
                            how_many_last_rows_to_plot = \
                                len ( historical_1d_data_for_usdt_trading_pair_df ) - row_for_plotting_start
                            print ( "how_many_last_rows_to_plot=" , how_many_last_rows_to_plot )

                        fig.add_trace ( go.Candlestick ( name = f'{usdt_trading_pair} on {exchange}' ,
                                                         x = historical_1d_data_for_usdt_trading_pair_df[
                                                             'open_time'].tail ( how_many_last_rows_to_plot ) ,
                                                         open = historical_1d_data_for_usdt_trading_pair_df[
                                                             'open'].tail (
                                                             how_many_last_rows_to_plot ) ,
                                                         high = historical_1d_data_for_usdt_trading_pair_df[
                                                             'high'].tail (
                                                             how_many_last_rows_to_plot ) ,
                                                         low = historical_1d_data_for_usdt_trading_pair_df[
                                                             'low'].tail (
                                                             how_many_last_rows_to_plot ) ,
                                                         close = historical_1d_data_for_usdt_trading_pair_df[
                                                             'close'].tail ( how_many_last_rows_to_plot ) ,
                                                         increasing_line_color = 'green' ,
                                                         decreasing_line_color = 'red'
                                                         ) , row = 2 , col = 1 , secondary_y = False )
                    except:
                        traceback.print_exc ()



                    try:
                        if len(open_time_of_another_high_list)>0:
                            fig.add_scatter ( x = historical_1d_data_for_usdt_trading_pair_df["open_time_of_another_high"],
                                              y = historical_1d_data_for_usdt_trading_pair_df["mirror_level_of_another_high"] ,
                                              mode = "markers" ,
                                              marker = dict ( color = 'cyan' , size = 15 ) ,
                                              name = "highs of mirror level" , row = 1 , col = 1 )
                    except Exception as e:
                        print ( "error=" , e )
                        traceback.print_exc ()
                    try:
                        if len(open_time_of_another_low_list)>0:
                            fig.add_scatter ( x = historical_1d_data_for_usdt_trading_pair_df["open_time_of_another_low"] ,
                                              y = historical_1d_data_for_usdt_trading_pair_df["mirror_level_of_another_low"] ,
                                              mode = "markers" ,
                                              marker = dict ( color = 'magenta' , size = 15 ) ,
                                              name = "lows of mirror level" , row = 1 , col = 1 )
                    except Exception as e:
                        print ( "error=" , e )
                        traceback.print_exc ()
                    try:
                        fig.add_scatter ( x = [open_time_of_candle_with_legit_low] ,
                                          y = [mirror_level] , mode = "markers" ,
                                          marker = dict ( color = 'red' , size = 15 ) ,
                                          name = "low of mirror level" , row = 1 , col = 1 )
                    except:
                        traceback.print_exc ()
                    try:
                        fig.add_scatter ( x = [open_time_of_candle_with_legit_high] ,
                                          y = [mirror_level] , mode = "markers" ,
                                          marker = dict ( color = 'green' , size = 15 ) ,
                                          name = "high of mirror level" , row = 1 , col = 1 )
                    except:
                        traceback.print_exc ()

                    try:
                        if len ( open_time_of_another_high_list ) > 0:
                            fig.add_scatter (
                                x = historical_1d_data_for_usdt_trading_pair_df["open_time_of_another_high"] ,
                                y = historical_1d_data_for_usdt_trading_pair_df["mirror_level_of_another_high"] ,
                                mode = "markers" ,
                                marker = dict ( color = 'cyan' , size = 15 ) ,
                                name = "highs of mirror level" , row = 2 , col = 1 )
                            print ( "printing dots" )
                    except Exception as e:
                        print ( "error=" , e )
                        traceback.print_exc ()
                    try:
                        if len ( open_time_of_another_low_list ) > 0:
                            fig.add_scatter (
                                x = historical_1d_data_for_usdt_trading_pair_df["open_time_of_another_low"] ,
                                y = historical_1d_data_for_usdt_trading_pair_df[
                                    "mirror_level_of_another_low"] ,
                                mode = "markers" ,
                                marker = dict ( color = 'magenta' , size = 15 ) ,
                                name = "lows of mirror level" , row = 2 , col = 1 )
                    except Exception as e:
                        print ( "error=" , e )
                        traceback.print_exc ()

                    try:
                        fig.add_scatter ( x = [open_time_of_candle_with_legit_low] ,
                                          y = [mirror_level] , mode = "markers" ,
                                          marker = dict ( color = 'red' , size = 15 ) ,
                                          name = "low of mirror level" , row = 2 , col = 1 )
                    except:
                        traceback.print_exc ()
                        pass
                    try:
                        fig.add_scatter ( x = [open_time_of_candle_with_legit_high] ,
                                          y = [mirror_level] , mode = "markers" ,
                                          marker = dict ( color = 'green' , size = 15 ) ,
                                          name = "high of mirror level" , row = 2 , col = 1 )
                    except:
                        traceback.print_exc ()
                        pass

                    try:
                        fig.add_trace ( go.Candlestick ( name = f'{usdt_trading_pair} on {exchange} ' ,
                                                         x = historical_some_period_data_for_usdt_trading_pair_df['open_time'] ,
                                                         open = historical_some_period_data_for_usdt_trading_pair_df['open'] ,
                                                         high = historical_some_period_data_for_usdt_trading_pair_df['high'] ,
                                                         low = historical_some_period_data_for_usdt_trading_pair_df['low'] ,
                                                         close = historical_some_period_data_for_usdt_trading_pair_df['close'] ,
                                                         increasing_line_color = 'green' , decreasing_line_color = 'red'
                                                         ) , row = 3 , col = 1 , secondary_y = False )
                        print("hourly chart added")

                    except Exception as e:
                        print ( "error=" , e )
                        traceback.print_exc ()

                    try:
                        if "mirror_level_equal_to_highs_or_nans" in historical_some_period_data_for_usdt_trading_pair_df.columns:
                            fig.add_scatter ( x =historical_some_period_data_for_usdt_trading_pair_df['open_time'],
                                              y = historical_some_period_data_for_usdt_trading_pair_df[
                                                    "mirror_level_equal_to_highs_or_nans"] , mode = "markers" ,
                                              marker = dict ( color = 'green' , size = 15 ) ,
                                              name = "high of mirror level" , row = 3 , col = 1 )
                    except:
                        traceback.print_exc ()
                        pass


                    try:
                        if "mirror_level_equal_to_lows_or_nans" in historical_some_period_data_for_usdt_trading_pair_df.columns:
                            fig.add_scatter ( x =historical_some_period_data_for_usdt_trading_pair_df['open_time'],
                                              y = historical_some_period_data_for_usdt_trading_pair_df[
                                                    "mirror_level_equal_to_lows_or_nans"] , mode = "markers" ,
                                              marker = dict ( color = 'red' , size = 15 ) ,
                                              name = "high of mirror level" , row = 3 , col = 1 )
                    except:
                        traceback.print_exc ()
                        pass






                    # fig.add_shape ( type = 'line' , x0 = historical_1d_data_for_usdt_trading_pair_df.loc[0,'open_time'] ,
                    #                 x1 = historical_1d_data_for_usdt_trading_pair_df.loc[-1,'open_time'] ,
                    #                 y0 = mirror_level ,
                    #                 y1 = mirror_level ,
                    #                 line = dict ( color = "purple" , width = 1 ) , row = 1 , col = 1 )

                    # try:
                    #     fig.add_trace(go.Scatter(x = historical_1d_data_for_usdt_trading_pair_df['open_time'] ,
                    #                   y = historical_1d_data_for_usdt_trading_pair_df['keltner_mband'] ,
                    #                     mode = "lines") ,row = 1 , col = 1)
                    # except Exception as e:
                    #     print('cannot plot keltner channel', e)
                    #     traceback.print_exc ()
                    #
                    # try:
                    #     fig.add_trace(go.Scatter(x = historical_1d_data_for_usdt_trading_pair_df['open_time'] ,
                    #                   y = historical_1d_data_for_usdt_trading_pair_df['keltner_hband'] ,
                    #                     mode = "lines") ,row = 1 , col = 1)
                    # except Exception as e:
                    #     print('cannot plot keltner channel', e)
                    #     traceback.print_exc ()
                    #
                    # try:
                    #     fig.add_trace(go.Scatter(x = historical_1d_data_for_usdt_trading_pair_df['open_time'] ,
                    #                   y = historical_1d_data_for_usdt_trading_pair_df['keltner_lband'] ,
                    #                     mode = "lines") ,row = 1 , col = 1)
                    # except Exception as e:
                    #     print('cannot plot keltner channel', e)
                    #     traceback.print_exc ()

                    try:
                        fig.add_hline ( y = mirror_level )
                    except:
                        traceback.print_exc ()

                    #adding parabolic sar

                    try:
                        fig.add_scatter ( x = historical_1d_data_for_usdt_trading_pair_df["open_time"] ,
                                          y = historical_1d_data_for_usdt_trading_pair_df["psar"] , mode = "markers" ,
                                          marker = dict ( color = 'blue' , size = 4 ) ,
                                          name = "psar" , row = 1 , col = 1 )
                    except:
                        traceback.print_exc ()

                    try:
                        fig.add_scatter ( x = historical_1d_data_for_usdt_trading_pair_df["open_time"].tail (
                            how_many_last_rows_to_plot ) ,
                            y = historical_1d_data_for_usdt_trading_pair_df["psar"].tail (
                                how_many_last_rows_to_plot ) ,
                            mode = "markers" ,
                            marker = dict ( color = 'blue' , size = 4 ) ,
                            name = "psar" , row = 2 , col = 1 )
                    except Exception as e:
                        print ( 'cannot plot psar' , e )
                        traceback.print_exc ()

                    # try:
                    #     fig.add_scatter ( x = historical_some_period_data_for_usdt_trading_pair_df["open_time"] ,
                    #                       y = historical_some_period_data_for_usdt_trading_pair_df["psar"] ,
                    #                       mode = "markers" ,
                    #                       marker = dict ( color = 'blue' , size = 4 ) ,
                    #                       name = "psar" , row = 3 , col = 1 )
                    # except:
                    #     traceback.print_exc ()
                    #


                    #fig.update_xaxes ( patch = dict ( type = 'category' ) , row = 1 , col = 1 )

                    # fig.update_layout ( height = 700  , width = 20000 * i, title_text = 'Charts of some crypto assets' )
                    fig.update_layout ( margin_autoexpand = True )
                    # fig['layout'][f'xaxis{0}']['title'] = 'dates for ' + symbol
                    fig.layout.annotations[0].update ( text = f"{usdt_trading_pair} "
                                                              f"on {exchange} with mirror level={mirror_level}" )
                    fig.print_grid ()

                    fig.write_html ( where_to_plot_html )

                    # convert html to svg
                    imgkit.from_file ( where_to_plot_html , where_to_plot_svg )
                    # convert html to png

                    # imgkit.from_file ( where_to_plot_html ,
                    #                    where_to_plot_png ,
                    #                    options = {'format': 'png'} )

                    # convert html to jpg

                    imgkit.from_file ( where_to_plot_html ,
                                       where_to_plot_jpg ,
                                       options = {'format': 'jpeg'} )






                except Exception as e:
                    print ( f'Exception in {usdt_trading_pair}' )
                    print(e)
                    traceback.print_exc ()

                finally:
                    continue
            except Exception as e:
                print(f"problem plotting {usdt_trading_pair} on {exchange}")
                traceback.print_exc ()
                continue

    connection_to_btc_and_usdt_trading_pairs.close()


    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 60.0 / 60.0)
if __name__=="__main__":
    lower_timeframe_for_mirror_level_rebound_trading='1h'
    plot_ohlcv_chart_with_mirror_levels_from_given_exchange (lower_timeframe_for_mirror_level_rebound_trading)