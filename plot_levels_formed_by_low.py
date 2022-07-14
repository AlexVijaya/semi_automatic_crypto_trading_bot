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
import numpy as np
import plotly.express as px
from datetime import datetime
#from if_asset_is_close_to_hh_or_ll import find_asset_close_to_hh_and_ll


def import_ohlcv_and_mirror_levels_for_plotting(usdt_trading_pair,
                                                exchange,
                                                connection_to_usdt_trading_pairs_ohlcv):

    # path_to_usdt_trading_pairs_ohlcv=os.path.join ( os.getcwd () ,
    #                                      "datasets" ,
    #                                      "sql_databases" ,
    #                                      "async_all_exchanges_multiple_tables_historical_data_for_usdt_trading_pairs.db" )
    # connection_to_usdt_trading_pairs_ohlcv = \
    #     sqlite3.connect (  path_to_usdt_trading_pairs_ohlcv)
    print("usdt_trading_pair=",usdt_trading_pair)

    historical_data_for_usdt_trading_pair_df=\
        pd.read_sql ( f'''select * from "{usdt_trading_pair}_on_{exchange}" ;'''  ,
                             connection_to_usdt_trading_pairs_ohlcv )

    #connection_to_usdt_trading_pairs_ohlcv.close()

    return historical_data_for_usdt_trading_pair_df




def plot_ohlcv_chart_with_levels_formed_by_lows (async_var):
    start_time=time.time()
    current_timestamp = time.time ()
    counter=0

    path_to_usdt_trading_pairs_ohlcv = os.path.join ( os.getcwd () ,
                                                      "datasets" ,
                                                      "sql_databases" ,
                                                      "async_all_exchanges_multiple_tables_historical_data_for_usdt_trading_pairs.db" )
    connection_to_usdt_trading_pairs_ohlcv = \
        sqlite3.connect ( path_to_usdt_trading_pairs_ohlcv )

    path_to_db_with_USDT_and_btc_pairs = os.path.join ( os.getcwd () , "datasets" ,
                                                        "sql_databases" ,
                                                        "btc_and_usdt_pairs_from_all_exchanges.db" )

    connection_to_usdt_pair_levels_formed_by_low = \
        sqlite3.connect ( path_to_db_with_USDT_and_btc_pairs )
    table_with_all_low_levels_df=pd.read_sql ( f'''select * from levels_formed_by_lows ;''' ,
                                     connection_to_usdt_pair_levels_formed_by_low )

    # delete previously plotted charts
    folder_to_be_deleted = os.path.join ( os.getcwd () ,
                                          'datasets' ,
                                          'plots' ,
                                          'crypto_exchange_plots_levels_formed_by_lows' )

    try:
        shutil.rmtree ( folder_to_be_deleted )
        pass
    except Exception as e:
        print ( "error deleting folder \n" , e )
        pass


    for row_of_lows in range(0,len(table_with_all_low_levels_df)):
        # print("table_with_all_low_levels_df[[row_of_lows]]")
        counter=counter+1

        try:
            print ( table_with_all_low_levels_df.loc[[row_of_lows]].to_string() )
            one_row_df=table_with_all_low_levels_df.loc[[row_of_lows]]
            usdt_trading_pair = table_with_all_low_levels_df.loc[row_of_lows , 'USDT_pair']
            exchange = table_with_all_low_levels_df.loc[row_of_lows , 'exchange']
            print ( "usdt_trading_pair=" , usdt_trading_pair )
            print ( "exchange=" , exchange )
            level_formed_by_low=table_with_all_low_levels_df.loc[row_of_lows , 'level_formed_by_low']
            list_of_timestamps=[]
            for key in one_row_df.keys():
                print("key=",key)
                if "timestamp" in key:
                    if one_row_df[key].iat[0]==one_row_df[key].iat[0]:
                        timestamp_of_low=one_row_df[key].iat[0]
                        date_object=datetime.fromtimestamp(timestamp_of_low/1000.0)
                        string_of_date_and_time=date_object.strftime ( '%Y-%m-%d %H:%M:%S' )

                        list_of_timestamps.append(string_of_date_and_time)

            print("list_of_timestamps=",list_of_timestamps)


            data_df=import_ohlcv_and_mirror_levels_for_plotting ( usdt_trading_pair ,
                                                          exchange,
                                                        connection_to_usdt_trading_pairs_ohlcv )

            usdt_trading_pair_without_slash = usdt_trading_pair.replace ( "/" , "" )

            # deleting : symbol because somehow it does not get to plot
            if ":" in usdt_trading_pair_without_slash:
                print ( 'found pair with :' , usdt_trading_pair_without_slash )
                usdt_trading_pair_without_slash = usdt_trading_pair_without_slash.replace ( ":" , '__' )
                print ( 'found pair with :' , usdt_trading_pair_without_slash )

            print ( f'{usdt_trading_pair} on {exchange} is number {row_of_lows + 1} '
                    f'out of {len ( table_with_all_low_levels_df )}' )


            print("data_df\n",data_df)
            last_date_with_time = data_df["open_time"].iloc[-1]
            print ( "type(last_date_with_time)\n" , type ( last_date_with_time ) )
            print ( "last_date_with_time\n" , last_date_with_time )
            last_date_without_time = last_date_with_time.split ( " " )
            print ( "last_date_with_time\n" , last_date_without_time[0] )
            last_date_without_time = last_date_without_time[0]



            # plotting charts with mirror levels
            try:
                number_of_charts=1
                where_to_plot_html = os.path.join ( os.getcwd () ,
                                                    'datasets' ,
                                                    'plots' ,
                                                    'crypto_exchange_plots_levels_formed_by_lows' ,
                                                    'crypto_exchange_plots_html' ,
                                                    f'{counter}_{usdt_trading_pair_without_slash}_on_{exchange}.html' )

                where_to_plot_pdf = os.path.join ( os.getcwd () ,
                                                   'datasets' ,
                                                   'plots' ,
                                                   'crypto_exchange_plots_levels_formed_by_lows'  ,
                                                   'crypto_exchange_plots_pdf' ,
                                                   f'{counter}_{usdt_trading_pair_without_slash}_on_{exchange}.pdf' )
                where_to_plot_svg = os.path.join ( os.getcwd () ,
                                                   'datasets' ,
                                                   'plots' ,
                                                   'crypto_exchange_plots_levels_formed_by_lows' ,
                                                   'crypto_exchange_plots_svg' ,
                                                   f'{counter}_{usdt_trading_pair_without_slash}_on_{exchange}.svg' )
                where_to_plot_jpg = os.path.join ( os.getcwd () ,
                                                   'datasets' ,
                                                   'plots' ,
                                                   'crypto_exchange_plots_levels_formed_by_lows' ,
                                                   'crypto_exchange_plots_jpg' ,
                                                   f'{counter}_{usdt_trading_pair_without_slash}_on_{exchange}.jpg' )

                where_to_plot_png = os.path.join ( os.getcwd () ,
                                                   'datasets' ,
                                                   'plots' ,
                                                   'crypto_exchange_plots_levels_formed_by_lows' ,
                                                   'crypto_exchange_plots_png' ,
                                                   f'{counter}_{usdt_trading_pair_without_slash}_on_{exchange}.png' )
                # create directory for crypto_exchange_plots parent folder
                # if it does not exists
                path_to_databases = os.path.join ( os.getcwd () ,
                                                   'datasets' ,
                                                   'plots' ,
                                                   'crypto_exchange_plots_levels_formed_by_lows' )
                Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )
                # create directories for all hh images
                formats = ['png' , 'svg' , 'pdf' , 'html' , 'jpg']
                for img_format in formats:
                    path_to_special_format_images_of_mirror_charts = \
                        os.path.join ( os.getcwd () ,
                                       'datasets' ,
                                       'plots' ,
                                       'crypto_exchange_plots_levels_formed_by_lows' ,
                                       f'crypto_exchange_plots_{img_format}' )
                    Path ( path_to_special_format_images_of_mirror_charts ).mkdir ( parents = True , exist_ok = True )

                fig = make_subplots ( rows = 1 , cols = number_of_charts ,
                                      shared_xaxes = False ,
                                      subplot_titles = tuple ( usdt_trading_pair ) ,
                                      specs = [[{"secondary_y": True}]] )
                fig.update_layout ( height = 1500 ,
                                    width = 4000 * number_of_charts ,
                                    title_text = f'{usdt_trading_pair} '
                                                 f'on {exchange} with level formed by low={level_formed_by_low} on {last_date_without_time}' ,
                                    font = dict (
                                        family = "Courier New, monospace" ,
                                        size = 40 ,
                                        color = "RebeccaPurple"
                                    ) )
                fig.update_xaxes ( rangeslider = {'visible': False} , row = 1 , col = number_of_charts )
                config = dict ( {'scrollZoom': True} )
                # print(type("historical_data_for_usdt_trading_pair_df['open_time']\n",
                #            historical_data_for_usdt_trading_pair_df.loc[3,'open_time']))

                try:
                    fig.add_trace ( go.Candlestick ( name = f'{usdt_trading_pair} on {exchange}' ,
                                                     x = data_df['open_time'] ,
                                                     open = data_df['open'] ,
                                                     high = data_df['high'] ,
                                                     low = data_df['low'] ,
                                                     close = data_df['close'] ,
                                                     increasing_line_color = 'green' , decreasing_line_color = 'red'
                                                     ) , row = 1 , col = 1 , secondary_y = False )
                except Exception as e:
                    print ( "error" , e )
                    traceback.print_exc ()

                try:
                    for timestamp in list_of_timestamps:
                        fig.add_scatter ( x = [timestamp],
                                          y = [level_formed_by_low] , mode = "markers" ,
                                          marker = dict ( color = 'red' , size = 15 ) ,
                                          name = "level formed by low" , row = 1 , col = 1 )
                    pass
                except Exception as e:
                    print ( "error" , e )
                    traceback.print_exc ()




                fig.add_hline ( y = level_formed_by_low )

                fig.update_xaxes ( patch = dict ( type = 'category' ) , row = 1 , col = 1 )

                # fig.update_layout ( height = 700  , width = 20000 * i, title_text = 'Charts of some crypto assets' )
                fig.update_layout ( margin_autoexpand = True )
                # fig['layout'][f'xaxis{0}']['title'] = 'dates for ' + symbol
                fig.layout.annotations[0].update ( text = f"{usdt_trading_pair} "
                                                          f"on {exchange} with level formed by_low={level_formed_by_low}" )
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
                print ( "error" , e )
                traceback.print_exc ()



        except Exception as e:
            print("error",e)
            traceback.print_exc()
            pass

    connection_to_usdt_pair_levels_formed_by_low.close()
    connection_to_usdt_trading_pairs_ohlcv.close()
    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 60.0 / 60.0)


if __name__=="__main__":
    async_var=True
    plot_ohlcv_chart_with_levels_formed_by_lows (async_var)
