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

def import_ohlcv_and_mirror_levels_for_plotting(usdt_trading_pair,
                                                exchange):

    path_to_usdt_trading_pairs_ohlcv=os.path.join ( os.getcwd () ,
                                         "datasets" ,
                                         "sql_databases" ,
                                         "multiple_tables_historical_data_for_usdt_trading_pairs_with_mirror_levels_with_highs_or_lows_yesterday.db" )
    connection_to_usdt_trading_pairs_ohlcv = \
        sqlite3.connect (  path_to_usdt_trading_pairs_ohlcv)
    print("usdt_trading_pair_for_debug=",usdt_trading_pair)
    #time.sleep(4)
    historical_data_for_usdt_trading_pair_df=\
        pd.read_sql ( f'''select * from "{usdt_trading_pair}_on_{exchange}" ;'''  ,
                             connection_to_usdt_trading_pairs_ohlcv )

    return historical_data_for_usdt_trading_pair_df



def plot_ohlcv_chart_with_mirror_levels_from_given_exchange (async_var):
    start_time=time.time()
    current_timestamp = time.time ()
    counter=0

    path_to_usdt_trading_pairs_ohlcv_recent = os.path.join ( os.getcwd () ,
                                                      "datasets" ,
                                                      "sql_databases" ,
                                                      "multiple_tables_historical_data_for_usdt_trading_pairs_with_mirror_levels_with_highs_or_lows_yesterday.db" )
    connection_to_usdt_trading_pairs_ohlcv_recent = \
        sqlite3.connect ( path_to_usdt_trading_pairs_ohlcv_recent )

    #async_var = True
    # if async_var == True:
    #     connection_to_usdt_trading_pairs_ohlcv_recent = \
    #         sqlite3.connect ( os.path.join ( os.getcwd () ,
    #                                          "datasets" ,
    #                                          "sql_databases" ,
    #                                          "async_all_exchanges_multiple_tables_historical_data_for_usdt_trading_pairs.db" ) )

    cursor_recent=connection_to_usdt_trading_pairs_ohlcv_recent.cursor()
    cursor_recent.execute("SELECT name FROM sqlite_master WHERE type='table';")
    list_of_ohlcv_with_recent_mirror_highs_and_lows=cursor_recent.fetchall()
    list_of_exchanges_with_recent_mirror_lows_and_highs=[]
    list_of_usdt_pair_with_recent_mirror_lows_and_highs = []
    usdt_pair_plus_exchange_dict={}
    for recent_table in list_of_ohlcv_with_recent_mirror_highs_and_lows:
        usdt_pair_plus_exchange_list=recent_table[0].split('_on_')
        usdt_pair_plus_exchange_dict[usdt_pair_plus_exchange_list[0]]=[usdt_pair_plus_exchange_list[1]]
        if (usdt_pair_plus_exchange_list[0], [usdt_pair_plus_exchange_list[1]]) not in usdt_pair_plus_exchange_dict.items():
            usdt_pair_plus_exchange_dict[usdt_pair_plus_exchange_list[0]].append(usdt_pair_plus_exchange_list[1])
        list_of_exchanges_with_recent_mirror_lows_and_highs.append(usdt_pair_plus_exchange_list[1])
        list_of_usdt_pair_with_recent_mirror_lows_and_highs.append ( usdt_pair_plus_exchange_list[0] )
        print("usdt_pair_plus_exchange_list=\n",usdt_pair_plus_exchange_list)
    print("usdt_pair_plus_exchange_dict=\n",usdt_pair_plus_exchange_dict)

    print("list_of_ohlcv_with_recent_mirror_highs_and_lows=\n",
          list_of_ohlcv_with_recent_mirror_highs_and_lows)

    path_to_db_with_USDT_and_btc_pairs = os.path.join ( os.getcwd () , "datasets" ,
                                                        "sql_databases" ,
                                                        "btc_and_usdt_pairs_from_all_exchanges.db" )

    connection_to_btc_and_usdt_trading_pairs = \
        sqlite3.connect ( path_to_db_with_USDT_and_btc_pairs )

    mirror_levels_df = pd.read_sql ( f'''select * from mirror_levels_without_duplicates ;''' ,
                                     connection_to_btc_and_usdt_trading_pairs )
    #print ( "mirror_levels_df\n" , mirror_levels_df )

    # delete preveiously plotted charts
    # folder_to_be_deleted = os.path.join ( os.getcwd () ,
    #                                       'datasets' ,
    #                                       'plots' ,
    #                                       'crypto_exchange_plots_recent_highs_and_lows_equal_mirror_level' )
    #
    # try:
    #     shutil.rmtree ( folder_to_be_deleted )
    #     pass
    # except Exception as e:
    #     print ( "error deleting folder \n" , e )
    #     pass
    counter=0
    for row_number in range(0,len(mirror_levels_df)):
        usdt_trading_pair =mirror_levels_df.loc[row_number,'USDT_pair']
        exchange = mirror_levels_df.loc[row_number,'exchange']
        print ("usdt_trading_pair=",usdt_trading_pair)
        print ("exchange=",exchange)

        timestamp_for_low = (mirror_levels_df.loc[row_number , 'timestamp_for_low']) / 1000.0
        timestamp_for_high = (mirror_levels_df.loc[row_number , 'timestamp_for_high']) / 1000.0
        if  ((current_timestamp - timestamp_for_low) < (86400 * 2)) or\
                ((current_timestamp - timestamp_for_high) < (86400 * 2)):
            print ( f"recently mirror level was hit for {usdt_trading_pair} on {exchange}" )

            # if  ((current_timestamp - timestamp_for_high) < (86400 * 2)):
            #     print ( f"recently mirror level (high) was hit for {usdt_trading_pair} on {exchange}" )
            #     continue
            #check if the usdt pair and exchange have mirror levels with recent highs and
            #lows touching mirror levels
            if  usdt_pair_plus_exchange_dict.get(usdt_trading_pair)==[exchange]:
                counter=counter+1
                print ("counter=",counter)


                mirror_level=mirror_levels_df.loc[row_number,'mirror_level']
                open_time_of_candle_with_legit_low = mirror_levels_df.loc[row_number ,
                                                    'open_time_of_candle_with_legit_low']
                open_time_of_candle_with_legit_high = mirror_levels_df.loc[row_number ,
                                                    'open_time_of_candle_with_legit_high']


                historical_data_for_usdt_trading_pair_df=\
                    import_ohlcv_and_mirror_levels_for_plotting ( usdt_trading_pair,exchange)
                #counter+=1
                #print("counter=", counter)
                print(f'{usdt_trading_pair} on {exchange} is number {row_number+1} '
                      f'out of {len(mirror_levels_df)}')
                print ( "historical_data_for_usdt_trading_pair_df\n" ,
                       historical_data_for_usdt_trading_pair_df )

                usdt_trading_pair_without_slash=usdt_trading_pair.replace("/","")

                #deleting : symbol because somehow it does not get to plot
                if ":" in usdt_trading_pair_without_slash:
                    print ( 'found pair with :' , usdt_trading_pair_without_slash )
                    usdt_trading_pair_without_slash=usdt_trading_pair_without_slash.replace(":",'__')
                    print( 'found pair with :',usdt_trading_pair_without_slash)


                historical_data_for_usdt_trading_pair_df_list_of_highs=\
                    historical_data_for_usdt_trading_pair_df['high'].to_list()

                historical_data_for_usdt_trading_pair_df_list_of_lows = \
                    historical_data_for_usdt_trading_pair_df['low'].to_list ()

                open_time_of_another_high_list=[]
                open_time_of_another_low_list=[]
                open_time_of_another_high_list_df=\
                    pd.DataFrame(columns = ['open_time_of_high','mirror_level'])
                open_time_of_another_low_list_df =\
                    pd.DataFrame (columns = ['open_time_of_low','mirror_level'])
                for index, high in enumerate(historical_data_for_usdt_trading_pair_df_list_of_highs):
                    if high==mirror_level:
                        #print('high of mirror level is found more than one time\n ')
                        #print (f'index={index}')
                        open_time_of_another_high=\
                            historical_data_for_usdt_trading_pair_df.loc[index,'open_time']
                        #print ( f'open_time_of_another_high={open_time_of_another_high}' )
                        open_time_of_another_high_list.append(open_time_of_another_high)
                        if open_time_of_another_high!=open_time_of_candle_with_legit_high:
                            open_time_of_another_high_list.append ( open_time_of_another_high )
                            historical_data_for_usdt_trading_pair_df.loc[historical_data_for_usdt_trading_pair_df["open_time"] ==
                                                                         open_time_of_another_high, ['open_time_of_another_high']]= open_time_of_another_high

                            historical_data_for_usdt_trading_pair_df.loc[
                                historical_data_for_usdt_trading_pair_df["open_time"] ==
                                open_time_of_another_high , ['mirror_level_of_another_high']] =mirror_level
                        else:
                            historical_data_for_usdt_trading_pair_df.loc[historical_data_for_usdt_trading_pair_df["open_time"] ==
                                                                         open_time_of_another_high, ['open_time_of_another_high']]= None

                            historical_data_for_usdt_trading_pair_df.loc[
                                historical_data_for_usdt_trading_pair_df["open_time"] ==
                                open_time_of_another_high , ['mirror_level_of_another_high']] = None


                for index, open_time_of_another_high in enumerate(open_time_of_another_high_list):
                    open_time_of_another_high_list_df.at[ index, 'open_time_of_high']=open_time_of_another_high
                    open_time_of_another_high_list_df.at[index, 'mirror_level'] = mirror_level



                for index, low in enumerate(historical_data_for_usdt_trading_pair_df_list_of_lows):
                    if low==mirror_level:
                        #print('low of mirror level is found more than one time\n')
                        #print ( f'index={index}' )
                        open_time_of_another_low = \
                            historical_data_for_usdt_trading_pair_df.loc[index , 'open_time']
                        #print ( f'open_time_of_another_low={open_time_of_another_low}' )
                        if open_time_of_another_low!=open_time_of_candle_with_legit_low:
                            open_time_of_another_low_list.append ( open_time_of_another_low )

                            historical_data_for_usdt_trading_pair_df.loc[historical_data_for_usdt_trading_pair_df["open_time"] ==
                                                                         open_time_of_another_low, ['open_time_of_another_low']]=\
                                open_time_of_another_low

                            historical_data_for_usdt_trading_pair_df.loc[
                                historical_data_for_usdt_trading_pair_df["open_time"] ==
                                open_time_of_another_low , ['mirror_level_of_another_low']] =mirror_level

                        else:
                            historical_data_for_usdt_trading_pair_df.loc[historical_data_for_usdt_trading_pair_df["open_time"] ==
                                                                         open_time_of_another_low, ['open_time_of_another_low']]= None

                            historical_data_for_usdt_trading_pair_df.loc[
                                historical_data_for_usdt_trading_pair_df["open_time"] ==
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
                print ( "historical_data_for_usdt_trading_pair_df\n" ,
                        historical_data_for_usdt_trading_pair_df )

                number_of_charts = 1

                last_date_with_time=historical_data_for_usdt_trading_pair_df["open_time"].iloc[-1]
                print("type(last_date_with_time)\n",type(last_date_with_time))
                print ( "last_date_with_time\n" , last_date_with_time)
                last_date_without_time=last_date_with_time.split(" ")
                print ( "last_date_with_time\n" , last_date_without_time[0] )
                last_date_without_time=last_date_without_time[0]

                #time.sleep(9)

                #plotting charts with mirror levels
                try:
                    where_to_plot_html = os.path.join ( os.getcwd () ,
                                                   'datasets' ,
                                                   'plots' ,
                                                   'crypto_exchange_plots_recent_highs_and_lows_equal_mirror_level' ,
                                                    f'recent_mirror_plots_on_{last_date_without_time}',
                                                   'crypto_exchange_plots_html',
                                                   f'{counter}_{usdt_trading_pair_without_slash}_on_{exchange}.html')

                    where_to_plot_pdf = os.path.join ( os.getcwd () ,
                                                       'datasets' ,
                                                       'plots' ,
                                                       'crypto_exchange_plots_recent_highs_and_lows_equal_mirror_level' ,
                                                        f'recent_mirror_plots_on_{last_date_without_time}',
                                                       'crypto_exchange_plots_pdf',
                                                       f'{counter}_{usdt_trading_pair_without_slash}_on_{exchange}.pdf' )
                    where_to_plot_svg = os.path.join ( os.getcwd () ,
                                                       'datasets' ,
                                                       'plots' ,
                                                       'crypto_exchange_plots_recent_highs_and_lows_equal_mirror_level' ,
                                                        f'recent_mirror_plots_on_{last_date_without_time}',
                                                       'crypto_exchange_plots_svg' ,
                                                       f'{counter}_{usdt_trading_pair_without_slash}_on_{exchange}.svg' )
                    where_to_plot_jpg = os.path.join ( os.getcwd () ,
                                                       'datasets' ,
                                                       'plots' ,
                                                       'crypto_exchange_plots_recent_highs_and_lows_equal_mirror_level' ,
                                                        f'recent_mirror_plots_on_{last_date_without_time}',
                                                       'crypto_exchange_plots_jpg' ,
                                                       f'{counter}_{usdt_trading_pair_without_slash}_on_{exchange}.jpg' )

                    where_to_plot_png = os.path.join ( os.getcwd () ,
                                                       'datasets' ,
                                                       'plots' ,
                                                       'crypto_exchange_plots_recent_highs_and_lows_equal_mirror_level' ,
                                                        f'recent_mirror_plots_on_{last_date_without_time}',
                                                       'crypto_exchange_plots_png' ,
                                                       f'{counter}_{usdt_trading_pair_without_slash}_on_{exchange}.png' )
                    #create directory for crypto_exchange_plots parent folder
                    # if it does not exists
                    path_to_databases = os.path.join ( os.getcwd () ,
                                                       'datasets' ,
                                                       'plots' ,
                                                       'crypto_exchange_plots_recent_highs_and_lows_equal_mirror_level',
                                                        f'recent_mirror_plots_on_{last_date_without_time}' )
                    Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )
                    #create directories for all hh images
                    formats=['png','svg','pdf','html','jpg']
                    for img_format in formats:
                        path_to_special_format_images_of_mirror_charts =\
                            os.path.join ( os.getcwd () ,
                                                           'datasets' ,
                                                           'plots' ,
                                                           'crypto_exchange_plots_recent_highs_and_lows_equal_mirror_level',
                                                            f'recent_mirror_plots_on_{last_date_without_time}',
                                                           f'crypto_exchange_plots_{img_format}')
                        Path ( path_to_special_format_images_of_mirror_charts ).mkdir ( parents = True , exist_ok = True )

                    fig = make_subplots ( rows = 1 , cols = number_of_charts ,
                                          shared_xaxes = False ,
                                          subplot_titles = tuple ( usdt_trading_pair ) ,
                                          specs = [[{"secondary_y": True}]] )
                    fig.update_layout ( height = 1500 ,
                                        width = 4000 * number_of_charts ,
                                        title_text = f'{usdt_trading_pair} '
                                                     f'on {exchange} with mirror level={mirror_level} on {last_date_without_time}' ,
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
                                                         x = historical_data_for_usdt_trading_pair_df['open_time'] ,
                                                         open = historical_data_for_usdt_trading_pair_df['open'] ,
                                                         high = historical_data_for_usdt_trading_pair_df['high'] ,
                                                         low = historical_data_for_usdt_trading_pair_df['low'] ,
                                                         close = historical_data_for_usdt_trading_pair_df['close'] ,
                                                         increasing_line_color = 'green' , decreasing_line_color = 'red'
                                                         ) , row = 1 , col = 1 , secondary_y = False )

                        try:
                            if len(open_time_of_another_high_list)>0:
                                fig.add_scatter ( x = historical_data_for_usdt_trading_pair_df["open_time_of_another_high"],
                                                  y = historical_data_for_usdt_trading_pair_df["mirror_level_of_another_high"] ,
                                                  mode = "markers" ,
                                                  marker = dict ( color = 'cyan' , size = 15 ) ,
                                                  name = "highs of mirror level" , row = 1 , col = 1 )
                        except:
                            pass
                        try:
                            if len(open_time_of_another_low_list)>0:
                                fig.add_scatter ( x = historical_data_for_usdt_trading_pair_df["open_time_of_another_low"] ,
                                                  y = historical_data_for_usdt_trading_pair_df["mirror_level_of_another_low"] ,
                                                  mode = "markers" ,
                                                  marker = dict ( color = 'magenta' , size = 15 ) ,
                                                  name = "lows of mirror level" , row = 1 , col = 1 )
                        except:
                            pass
                        fig.add_scatter ( x = [open_time_of_candle_with_legit_low] ,
                                          y = [mirror_level] , mode = "markers" ,
                                          marker = dict ( color = 'red' , size = 15 ) ,
                                          name = "low of mirror level" , row = 1 , col = 1 )
                        fig.add_scatter ( x = [open_time_of_candle_with_legit_high] ,
                                          y = [mirror_level] , mode = "markers" ,
                                          marker = dict ( color = 'green' , size = 15 ) ,
                                          name = "high of mirror level" , row = 1 , col = 1 )

                        # fig.add_shape ( type = 'line' , x0 = historical_data_for_usdt_trading_pair_df.loc[0,'open_time'] ,
                        #                 x1 = historical_data_for_usdt_trading_pair_df.loc[-1,'open_time'] ,
                        #                 y0 = mirror_level ,
                        #                 y1 = mirror_level ,
                        #                 line = dict ( color = "purple" , width = 1 ) , row = 1 , col = 1 )
                        #

                        #plot keltner channel
                        # try:
                        #     fig.add_trace(go.Scatter(x = historical_data_for_usdt_trading_pair_df['open_time'] ,
                        #                   y = historical_data_for_usdt_trading_pair_df['keltner_mband'] ,
                        #                     mode = "lines") ,row = 1 , col = 1)
                        # except Exception as e:
                        #     print('cannot plot keltner channel', e)
                        #
                        # try:
                        #     fig.add_trace(go.Scatter(x = historical_data_for_usdt_trading_pair_df['open_time'] ,
                        #                   y = historical_data_for_usdt_trading_pair_df['keltner_hband'] ,
                        #                     mode = "lines") ,row = 1 , col = 1)
                        # except Exception as e:
                        #     print('cannot plot keltner channel', e)
                        #
                        # try:
                        #     fig.add_trace(go.Scatter(x = historical_data_for_usdt_trading_pair_df['open_time'] ,
                        #                   y = historical_data_for_usdt_trading_pair_df['keltner_lband'] ,
                        #                     mode = "lines") ,row = 1 , col = 1)
                        # except Exception as e:
                        #     print('cannot plot keltner channel', e)


                        fig.add_hline ( y = mirror_level )

                        # fig.add_scatter ( x = open_time_of_candle_with_legit_high ,
                        #                   y = mirror_level , mode = "markers" ,
                        #                   marker = dict ( color = 'blue' , size = 5 ) ,
                        #                   name = "mirror levels" , row = 1 , col = 1 )

                        fig.add_scatter ( x = historical_data_for_usdt_trading_pair_df["open_time"] ,
                                          y = historical_data_for_usdt_trading_pair_df["psar"] , mode = "markers" ,
                                          marker = dict ( color = 'blue' , size = 2 ) ,
                                          name = "psar" , row = 1 , col = 1 )



                        fig.update_xaxes ( patch = dict ( type = 'category' ) , row = 1 , col = 1 )

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
                        print ( e )


                except Exception as e:
                    print ( f'Exception in {usdt_trading_pair}' )
                    print(e)
                    traceback.print_exc ()

                finally:
                    continue
            else:
                continue


    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 60.0 / 60.0)


if __name__=="__main__":
    async_var=False
    plot_ohlcv_chart_with_mirror_levels_from_given_exchange (async_var)
