import time
import mplfinance as mpf
import tkinter as tk
import numpy as np
from tkinter import *
#from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
#import sqlite3
#import pandas as pd
import os
from calculate_horizontal_levels_using_lows_in_db_from_yf import calculate_horizontal_levels_using_only_lows
from calculate_horizontal_levels_using_highs_in_db_from_yf_new import calculate_horizontal_levels_using_only_highs
def plot_levels(symbol):
    win = tk.Tk ()
    win.geometry ( '1600x850' )

    mc = mpf.make_marketcolors ( up = 'green' , down = 'red' , edge = 'k' , wick = 'b' , volume = 'gray' ,
                                 inherit = True )
    s = mpf.make_mpf_style ( gridaxis = 'both' , gridstyle = '-.' , y_on_right = False , marketcolors = mc )
    save_config_var = dict ( fname = os.path.join ( os.getcwd () , "datasets" , "plots" , f'{symbol}.png' ) ,
                             dpi = 800 )
    #adjust your plot with these settings

    level_for_this_period=60

    round_to_this_number_of_decimal_places=3

    number_2level_touches_with_low=2
    number_of_more_than_3level_touches_with_low=3

    number_2level_touches_with_high = 2
    number_of_more_than_3level_touches_with_high = 3

    count_low_values_merged_full_df, low_levels_with_2touches,low_levels_with_2touches_all_dates=\
        calculate_horizontal_levels_using_only_lows (symbol ,round_to_this_number_of_decimal_places ,number_2level_touches_with_low,level_for_this_period )
    count_low_values_merged_full_df , low_levels_with_3touches , low_levels_with_3touches_all_dates =\
        calculate_horizontal_levels_using_only_lows (symbol , round_to_this_number_of_decimal_places , number_of_more_than_3level_touches_with_low,level_for_this_period )

    count_high_values_merged_full_df , high_levels_with_2touches , high_levels_with_2touches_all_dates = \
        calculate_horizontal_levels_using_only_highs ( symbol , round_to_this_number_of_decimal_places , number_2level_touches_with_high,level_for_this_period )
    count_high_values_merged_full_df , high_levels_with_3touches , high_levels_with_3touches_all_dates = \
        calculate_horizontal_levels_using_only_highs ( symbol , round_to_this_number_of_decimal_places , number_of_more_than_3level_touches_with_high,level_for_this_period )
    #
    print ( "count_high_values_merged_full_df\n" ,
            count_high_values_merged_full_df.head ( 10000 ).to_string () )
    print ( "high_levels_with_3touches\n" ,
            high_levels_with_3touches.head ( 10000 ).to_string () )
    print ( "high_levels_with_3touches_all_dates\n" ,
            high_levels_with_3touches_all_dates.head ( 10000 ).to_string () )
    #time.sleep ( 0 )

    print(["r"] * len ( low_levels_with_2touches ))
    red_list=["r"] * len ( low_levels_with_2touches )
    magenta_list = ["m"] * len ( low_levels_with_3touches )
    red_list.extend ( magenta_list )
    #print("high_levels_with_2touches",high_levels_with_2touches)
    #time.sleep(5)
    green_list = ["g"] * len ( high_levels_with_2touches )
    cyan_list = ["c"] * len ( high_levels_with_3touches )
    green_list.extend ( cyan_list )
    cyan_plus_red_list=red_list.extend(cyan_list)
    print("magenta_list",magenta_list)
    print("cyan_list",cyan_list)
    #print([["r"] * len ( levels_with_2touches )].append(["r"] * len ( levels_with_3touches )))
    # hlines_dict = dict ( hlines = tuple ( low_levels_with_2touches['Low'].tolist ()+low_levels_with_3touches['Low'].tolist () ) ,
    #                      colors =  tuple(magenta_list) ,
    #                      linewidths = tuple ( [0.1] * len ( low_levels_with_2touches )+[0.1] * len ( low_levels_with_3touches ) ) )

    #change 3 to 2 in low_levels_with_3touches on both lines than change red_list
    # to magenta list to get more levels plotted or uncomment the above three lines
    # hlines_dict = dict (
    #     hlines = tuple ( low_levels_with_3touches['Low'].tolist () ) ,
    #     colors = tuple ( red_list ) ,
    #     linewidths = tuple ( [0.1] * len ( low_levels_with_3touches ) ) )
    #
    # hlines_dict = dict (
    #     hlines = tuple ( high_levels_with_3touches['High'].tolist () ) ,
    #     colors = tuple ( cyan_list ) ,
    #     linewidths = tuple ( [0.1] * len ( low_levels_with_3touches )+[0.1] * len ( high_levels_with_3touches ) ) )
    #print("low_levels_with_3touches['Low']\n",low_levels_with_3touches['Low'])
    #time.sleep(10)


    hlines_dict = dict (
        hlines = tuple ( low_levels_with_3touches['Low'].tolist ()+high_levels_with_3touches['High'].tolist () ) ,
        colors = tuple ( [*magenta_list,*cyan_list] ) ,
        linewidths = tuple ( [0.1] * len ( high_levels_with_3touches ) ) )

    levels_lows_with_2touches_dict = mpf.make_addplot ( low_levels_with_2touches_all_dates["Low"] , type = 'scatter' ,markersize = 0.1 )

    levels_lows_with_3touches_dict = mpf.make_addplot ( low_levels_with_3touches_all_dates["Low"] , type = 'scatter' ,
                                                        markersize = 0.3 , color='magenta')

    levels_highs_with_2touches_dict = mpf.make_addplot ( high_levels_with_2touches_all_dates["High"] , type = 'scatter' ,
                                                        markersize = 0.1 )

    levels_highs_with_3touches_dict = mpf.make_addplot ( high_levels_with_3touches_all_dates["High"] , type = 'scatter' ,
                                                        markersize = 0.3,color='cyan' )

    print("len(high_levels_with_2touches_all_dates)",len(high_levels_with_2touches_all_dates))
    print ( "len(high_levels_with_3touches_all_dates)" , len ( high_levels_with_3touches_all_dates ) )
    print ( "len(low_levels_with_2touches_all_dates)" , len ( low_levels_with_2touches_all_dates ) )
    print ( "len(low_levels_with_3touches_all_dates)" , len ( low_levels_with_3touches_all_dates ) )
    fig,ax=mpf.plot ( count_low_values_merged_full_df , title = f'{symbol}' ,hlines=hlines_dict,
                      type = 'candle' , style = 'nightclouds' ,
                          volume = False ,
                          addplot = [levels_lows_with_2touches_dict,levels_lows_with_3touches_dict,
                                                        levels_highs_with_2touches_dict,levels_highs_with_3touches_dict] ,
                          savefig = save_config_var , tight_layout = True, closefig=True, returnfig=True )
    print ( "plt.get_fignums()=" , plt.get_fignums () )
    fig.clf()
    print("plt.get_fignums()=",plt.get_fignums())
    plt.close(plt.gcf())
    print ( "plt.get_fignums()=" , plt.get_fignums () )
    #time.sleep(3)

    #chart_canvas = FigureCanvasTkAgg ( fig , master = win )
    #chart_canvas.draw ()
    #chart_canvas.get_tk_widget ().place ( relx = 0 , rely = 0 , width = 1350 , height = 680 )
    # print("stock_df_rounded_ohlc\n",stock_df_rounded_ohlc.duplicated(subset='Low'))
    # print(stock_df_rounded_ohlc['Low'].value_counts().to_string ())
    # win.mainloop ()


