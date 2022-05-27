import pathlib
import time
import os
import datetime


import pandas as pd
import sqlite3
import pdfkit
import imgkit
import pathlib

from binance_config import api_secret
from binance_config import api_key
from binance.client import Client

def plot_all_trading_pairs_from_binance ():
    start_time=time.time()

    client = Client ( api_key = api_key , api_secret = api_secret )

    path_to_databases = os.path.join ( os.getcwd () , "datasets" , "sql_databases" )

    pathlib.Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )

    path_to_database_table_in_which_to_be_dropped = os.path.join ( os.getcwd () , "datasets" ,
                                                                   "sql_databases" , "binance_historical_data.db" )

    connection_to_tickers = sqlite3.connect ( os.path.join ( os.getcwd () , "datasets" ,
                                                             "sql_databases" , "binance_trading_pairs.db" ) )
    # uncomment if  you want to select all trading pairs from binance
    # sql_query = pd.read_sql_query ( '''select trading_pair from trading_pairs_tickers''' ,
    #                                 connection_to_tickers )

    sql_query = pd.read_sql_query ( '''select trading_pair from trading_pairs_tickers 
                                            where trading_pair like "%usdt"''' ,
                                    connection_to_tickers )

    tickers_df = pd.DataFrame ( sql_query )
    print ( tickers_df )
    from plot_levels_with_plotly_binance_symbols import plot_levels_with_plotly
    from pathlib import Path
    #options for pdfkit
    options={'orientation':'Portrait',
             'page-width':'5000',
             'no-print-media-type':None,
             'margin-top': '0.75in' ,
             'margin-right': '0.75in' ,
             'margin-bottom': '0.75in' ,
             'margin-left': '0.75in'
             }
    tickers_list=tickers_df['trading_pair'].to_list()
    for trading_pair in tickers_list:
        try:
            where_to_plot_html = os.path.join ( os.getcwd () ,
                                           'datasets' ,
                                           'plots' ,
                                           'binance_plots' ,
                                            'binance_plots_html',
                                           f'{trading_pair}.html')

            where_to_plot_pdf = os.path.join ( os.getcwd () ,
                                               'datasets' ,
                                               'plots' ,
                                               'binance_plots' ,
                                               'binance_plots_pdf',
                                               f'{trading_pair}.pdf' )
            where_to_plot_svg = os.path.join ( os.getcwd () ,
                                               'datasets' ,
                                               'plots' ,
                                               'binance_plots' ,
                                               'binance_plots_svg' ,
                                               f'{trading_pair}.svg' )

            where_to_plot_png = os.path.join ( os.getcwd () ,
                                               'datasets' ,
                                               'plots' ,
                                               'binance_plots' ,
                                               'binance_plots_png' ,
                                               f'{trading_pair}.png' )
            #create directory for hh parent folder if it does not extist
            path_to_databases = os.path.join ( os.getcwd () ,
                                               'datasets' ,
                                               'plots' ,
                                               'binance_plots' )
            Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )
            #create directories for all hh images
            formats=['png','svg','pdf','html']
            for img_format in formats:
                path_to_special_format_images_of_hh = os.path.join ( os.getcwd () ,
                                                   'datasets' ,
                                                   'plots' ,
                                                   'binance_plots',
                                                   f'binance_plots_{img_format}')
                Path ( path_to_special_format_images_of_hh ).mkdir ( parents = True ,
                                                                     exist_ok = True )

            print (f"plotting {trading_pair}", "/", len(tickers_list))
            plot_levels_with_plotly(trading_pair,where_to_plot_html,path_to_databases)

            #convert html to pdf
            #pdfkit.from_file(where_to_plot_html , where_to_plot_pdf, options=options)

            #convert html to svg
            imgkit.from_file ( where_to_plot_html , where_to_plot_svg )
            imgkit.from_file ( where_to_plot_html , where_to_plot_png, options={'format':'png'} )
        except Exception as e:
            print (f'Exception in {trading_pair}')
            print (e)
        finally:
            continue

    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    from yield_binance_trading_pair import yield_binance_trading_pair
    from pathlib import Path
    # yield_binance_trading_pair ()
    # for symbol in yield_binance_trading_pair ():
    #     print ( 'part after yield_binance_trading_pair is at work' )
    #     print ( "symbol" , symbol )
    #     where_to_plot_var = os.path.join ( os.getcwd () ,
    #                                        'datasets' ,
    #                                        'plots' ,
    #                                        'binance_plots' ,
    #                                        f'{symbol}.png' )
    #     path_to_databases = os.path.join ( os.getcwd () , 'datasets' , 'plots' , 'binance_plots' )
    #     Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )

    #     plot_levels_with_plotly ( symbol , where_to_plot,path_to_databases )
    finish_time = datetime.datetime.utcfromtimestamp ( time.time () ).strftime ( '%Y-%m-%dT%H:%M:%SZ' )
    print ( 'finished at' , finish_time )
plot_all_trading_pairs_from_binance()