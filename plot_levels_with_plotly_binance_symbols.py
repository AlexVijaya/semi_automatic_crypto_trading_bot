import time
import os
import traceback
import sys
import plotly.io
from pathlib import Path
from calculate_horizontal_levels_using_lows_from_source2 import calculate_horizontal_levels_using_only_lows_binance
from calculate_horizontal_levels_using_highs_from_source import calculate_horizontal_levels_using_only_highs_binance
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from yield_binance_trading_pair import yield_binance_trading_pair

def plot_levels_with_plotly(symbol,where_to_plot,path_to_databases,
                            level_for_this_period=365,
                            round_to_this_number_of_decimal_places = 3,plot_to_files=True
                            ):
    original_stdout=sys.stdout
    with open("standard_output_to_this_file.txt","w") as std_out_file:
        print(" ",file=std_out_file)


    number_2level_touches_with_low = 2
    number_of_more_than_3level_touches_with_low = 3

    number_2level_touches_with_high = 2
    number_of_more_than_3level_touches_with_high = 3
    # i=0
    # denominator = 20
    number_of_charts = 1
    # symbols=[ticker for ticker in yield_binance_trading_pair()]
    # print(symbols)
    # for symbol in symbols:
    #     print(symbols.index ( symbol ))
    #     if (symbols.index ( symbol ) + 1) % denominator == 0:
    #         number_of_charts = number_of_charts + 1
    # print ( number_of_charts )
    #time.sleep(3)




    fig = make_subplots ( rows = 1 , cols = number_of_charts ,
                          shared_xaxes = False,subplot_titles=tuple(symbol),
                          specs=[[{"secondary_y": True}]] )
    fig.update_layout ( height = 1600 , width = 4000 * number_of_charts , title_text = f'{symbol}',
                        font = dict (
                            family = "Courier New, monospace" ,
                            size = 20 ,
                            color = "RebeccaPurple"
                        ))
    fig.update_xaxes ( rangeslider = {'visible': False} , row = 1 , col = number_of_charts )
    config = dict ( {'scrollZoom': True} )


    count_low_values_merged_full_df , low_levels_with_2touches , low_levels_with_2touches_all_dates = \
        calculate_horizontal_levels_using_only_lows_binance ( symbol ,
                                                      round_to_this_number_of_decimal_places ,
                                                      number_2level_touches_with_low,
                                                      level_for_this_period )
    count_low_values_merged_full_df , low_levels_with_3touches , low_levels_with_3touches_all_dates = \
        calculate_horizontal_levels_using_only_lows_binance ( symbol ,
                                                      round_to_this_number_of_decimal_places ,
                                                      number_of_more_than_3level_touches_with_low,
                                                      level_for_this_period )

    count_high_values_merged_full_df , high_levels_with_2touches , high_levels_with_2touches_all_dates = \
        calculate_horizontal_levels_using_only_highs_binance ( symbol ,
                                                       round_to_this_number_of_decimal_places ,
                                                       number_2level_touches_with_high,
                                                       level_for_this_period )
    count_high_values_merged_full_df , high_levels_with_3touches , high_levels_with_3touches_all_dates = \
        calculate_horizontal_levels_using_only_highs_binance ( symbol ,
                                                       round_to_this_number_of_decimal_places ,
                                                       number_of_more_than_3level_touches_with_high,
                                                       level_for_this_period )


    try:
        fig.add_trace ( go.Candlestick (name=f'{symbol}',
            x = count_high_values_merged_full_df.index ,
            open = count_high_values_merged_full_df['open'] ,
            high = count_high_values_merged_full_df['high'] ,
            low = count_high_values_merged_full_df['low'] ,
            close = count_high_values_merged_full_df['close'] ,
            increasing_line_color = 'green' , decreasing_line_color = 'red'
        ) , row=1 , col=1, secondary_y = False )
    except Exception as e:
        print (e)

    try:
        count_low_values_merged_full_df_for_BTCUSDT , low_levels_with_2touches_for_BTCUSDT , low_levels_with_2touches_all_dates_for_BTCUSDT = \
            calculate_horizontal_levels_using_only_lows_binance ( 'BTCUSDT' ,
                                                                  round_to_this_number_of_decimal_places ,
                                                                  number_2level_touches_with_low ,
                                                                  level_for_this_period )

        fig.add_trace ( go.Ohlc (name='BTCUSDT',
            x = count_low_values_merged_full_df.index ,
            open = count_low_values_merged_full_df_for_BTCUSDT['open'] ,
            high = count_low_values_merged_full_df_for_BTCUSDT['high'] ,
            low = count_low_values_merged_full_df_for_BTCUSDT['low'] ,
            close = count_low_values_merged_full_df_for_BTCUSDT['close'] ,
            increasing_line_color = 'blue' , decreasing_line_color = 'black'
        ) , row=1 , col=1, secondary_y = True )
    except Exception as e:
        print (e)

    # print("high_levels_with_2touches\n", high_levels_with_2touches)
    # time.sleep ( 2 )
    #fig.layout.xaxis.type='category'
    # fig.add_shape(type='line',x0=count_high_values_merged_full_df.iloc[-10].name,
    #               x1=count_high_values_merged_full_df.iloc[-1].name,y0=40,y1=40)
    # print ( "low_levels_with_3touches\n\n+++++++++++++" , low_levels_with_3touches )
    # print ( "low_levels_with_2touches\n\n+++++++++++++" , low_levels_with_2touches )
    # print ( "high_levels_with_3touches\n\n+++++++++++++" , high_levels_with_3touches )
    print ( "high_levels_with_2touches\n\n+++++++++++++\n" , high_levels_with_2touches.head(10000).to_string() )


    try:
        for row_number,element in enumerate(high_levels_with_2touches):
            fig.add_shape ( type = 'line' , x0 = high_levels_with_2touches["index"].iloc[row_number] ,
                                       x1=count_high_values_merged_full_df.iloc[-1].name,
                            y0=high_levels_with_2touches["high"].iloc[row_number],
                            y1=high_levels_with_2touches["high"].iloc[row_number],
                            line=dict(color="aqua",width=1), row=1 , col=1 )
    except Exception as e:
        print (e)


    # print ( ";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;" )
    try:
        for row_number,element in enumerate(low_levels_with_2touches):
            fig.add_shape ( type = 'line' , x0 = low_levels_with_2touches["index"].iloc[row_number] ,
                                       x1=count_low_values_merged_full_df.iloc[-1].name,
                            y0=low_levels_with_2touches["low"].iloc[row_number],
                            y1=low_levels_with_2touches["low"].iloc[row_number],
                            line=dict(color="purple",width=1), row=1 , col=1)
    except Exception as e:
        print (e)


    try:
        for row_number,element in enumerate(high_levels_with_3touches):
            fig.add_shape ( type = 'line' , x0 = high_levels_with_3touches["index"].iloc[row_number] ,
                                       x1=count_high_values_merged_full_df.iloc[-1].name,
                            y0=high_levels_with_3touches["high"].iloc[row_number],
                            y1=high_levels_with_3touches["high"].iloc[row_number],
                            line=dict(color="aqua",width=1), row=1 , col=1 )
    except Exception as e:
        print (e)



    try:
        for row_number,element in enumerate(low_levels_with_3touches):
            # print("row number", row_number)
            fig.add_shape ( type = 'line' , x0 = low_levels_with_3touches["index"].iloc[row_number] ,
                                       x1=count_low_values_merged_full_df.iloc[-1].name,
                            y0=low_levels_with_3touches["low"].iloc[row_number],
                            y1=low_levels_with_3touches["low"].iloc[row_number],
                            line=dict(color="purple",width=1), row=1 , col=1)
    except Exception as e:
        print (e)
    #print ( "low_levels_with_3touches\n\n" , low_levels_with_3touches )


    try:
        fig.add_scatter(x=high_levels_with_2touches["index"],
                        y=high_levels_with_2touches["high"],mode="markers",
                        marker=dict(color='blue',size=5),
                        name="high_levels_with_2touches", row=1 , col=1)

    except Exception as e:
        print (e)
    # print ("high_levels_with_2touches.index", high_levels_with_2touches.index,
    #        "\n", "high_levels_with_2touches\n", high_levels_with_2touches)
    #print(";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;")


    try:
        fig.add_scatter ( x = high_levels_with_3touches["index"] ,
                          y = high_levels_with_3touches["high"] , mode = "markers" ,
                          marker = dict ( color = 'black' , size = 5 ) ,
                          name = "high_levels_with_3_or_more_touches" , row=1 , col=1)
        # print ( "high_levels_with_3touches.index" , high_levels_with_3touches.index ,
        #         "\n" , "high_levels_with_3touches" , high_levels_with_3touches )
    except Exception as e:
        print (e)


    try:
        fig.add_scatter ( x = low_levels_with_2touches["index"] ,
                          y = low_levels_with_2touches["low"] , mode = "markers" ,
                          marker = dict ( color = 'cyan' , size = 5 ) ,
                          name = "low_levels_with_2touches" , row=1 , col=1)
    except Exception as e:
        print (e)


    try:
        fig.add_scatter ( x = low_levels_with_3touches["index"] ,
                          y = low_levels_with_3touches["low"] , mode = "markers" ,
                          marker = dict ( color = 'magenta' , size = 5 ) ,
                          name = "low_levels_with_3_or_more_touches" , row=1 , col=1)
    except Exception as e:
        print (e)

    fig.update_xaxes ( patch = dict ( type = 'category' ) , row=1 , col=1 )

    #fig.update_layout ( height = 700  , width = 20000 * i, title_text = 'Charts of some crypto assets' )
    fig.update_layout ( margin_autoexpand = True)
    #fig['layout'][f'xaxis{0}']['title'] = 'dates for ' + symbol
    fig.layout.annotations[0].update ( text = symbol )
    fig.print_grid ()
    # print(count_high_values_merged_full_df)
    # print(count_high_values_merged_full_df)
    # except Exception as e:
    #     print(f"problem with {symbol}")
    #     print (e)

        # traceback.print_exc ()
        #continue
    #fig.show (config=config)

    if plot_to_files==True:

        #path_to_databases = os.path.join(os.getcwd() ,'datasets','plots', 'binance_plots_hh')
        Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )
        #fig.write_image( file=where_to_plot,format = 'html')
        fig.write_html(where_to_plot)
    else:
        fig.show ( config = config )


