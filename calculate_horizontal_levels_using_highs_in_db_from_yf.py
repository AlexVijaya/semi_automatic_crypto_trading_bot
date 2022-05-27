import os
import time
import mplfinance as mpf
import tkinter as tk
import numpy as np
from tkinter import *
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import sqlite3
import pandas as pd
def calculate_horizontal_levels_using_only_highs():
    start_time=time.time()
    conn=sqlite3.connect( os.path.join( os.getcwd() , 'datasets','sql_databases','yf_db_historical_data_new.db' ) )
    cur=conn.cursor()
    conn.row_factory=sqlite3.Row
    symbol="UBER"
    #cur.execute('''select * from stock_prices where stock_id=10 order by id''')
    stock_df=pd.read_sql(f'''select * from yf_gerchik_tickers_and_prices where symbol="{symbol}" ;''',
                         conn,parse_dates = 'Date')
    round_to_this_number_of_decimal_places=3
    stock_df_rounded=stock_df.round({'Open':round_to_this_number_of_decimal_places,
                                     'High':round_to_this_number_of_decimal_places,
                                     'Low':round_to_this_number_of_decimal_places,
                                     'Close':round_to_this_number_of_decimal_places})
    stock_df_rounded_ohlc=stock_df_rounded.loc[:,['Date','Open','High','Low','Close']]

    # stock_df_rounded_ohlc_with_equal_highs_keep_false = \
    #     stock_df_rounded_ohlc[stock_df_rounded_ohlc.duplicated ( subset = ["High"] ,keep = False)]

    stock_df_rounded_ohlc_with_equal_highs_keep_false = stock_df_rounded_ohlc

    stock_df_rounded_ohlc_with_equal_highs_keep_false_sorted=\
        stock_df_rounded_ohlc_with_equal_highs_keep_false.sort_values ( by = "Date" )

    stock_series_rounded_ohlc_with_equal_highs_keep_false=\
        stock_df_rounded_ohlc_with_equal_highs_keep_false['High'].value_counts()
    df_count_high_values=stock_series_rounded_ohlc_with_equal_highs_keep_false.to_frame ()

    df_count_high_values.reset_index(level=0, inplace=True)


    df_count_high_values.rename({'High':'number_of_same_highs', 'index':'High'}, axis='columns', inplace=True)
    df_count_high_values_merged=pd.merge(stock_df_rounded_ohlc_with_equal_highs_keep_false_sorted,
                                        df_count_high_values[['High','number_of_same_highs']],
                                        on='High',how='left' )
    count_high_values_merged_full_df=pd.merge(stock_df_rounded,
                                             df_count_high_values_merged[['High', 'number_of_same_highs']],
                                             on='High' ,how='left')

    count_high_values_merged_full_df.drop_duplicates(subset='Date',inplace=True)
    count_high_values_merged_full_df.reset_index(inplace = True,drop=True)
    count_high_values_merged_full_df.update(stock_df)
    print ( "count_high_values_merged_full_df\n",count_high_values_merged_full_df.head ( 10000).to_string () )
    print("stock_df_rounded_ohlc\n",stock_df_rounded_ohlc.head ( 10000).to_string () )
    print ( "df_count_high_values_merged\n" ,
            df_count_high_values_merged.head ( 10000 ).to_string () )
    count_high_values_merged_full_df["NaN_Highs"] = np.nan
    print ( "count_high_values_merged_full_df\n" ,
            count_high_values_merged_full_df.head ( 10000 ).to_string () )



    levels_with_2_touches=\
        count_high_values_merged_full_df.loc[count_high_values_merged_full_df["number_of_same_highs"]==3]
    print("levels_with_2_touches\n",levels_with_2_touches)

    levels_with_2_touches_all_dates=pd.merge(count_high_values_merged_full_df.loc[:,["Date","NaN_Highs"]],
                                             levels_with_2_touches.loc[:,["Date","High"]], on="Date",how='left')
    print ( "levels_with_2_touches_all_dates\n" , levels_with_2_touches_all_dates )
    count_high_values_merged_full_df['Date'] = pd.to_datetime ( count_high_values_merged_full_df['Date'] )
    count_high_values_merged_full_df.set_index ( 'Date' , inplace = True )

    levels_with_2_touches_all_dates['Date'] = pd.to_datetime ( levels_with_2_touches_all_dates['Date'] )
    levels_with_2_touches_all_dates.set_index ( 'Date' , inplace = True )
    print ( "levels_with_2_touches_all_dates\n" , levels_with_2_touches_all_dates )
    print(levels_with_2_touches['High'].tolist())
    print(tuple([0.5]*len(levels_with_2_touches)))
    win=tk.Tk()
    win.geometry('1600x850')
    #fig=mpf.figure(figsize=(1,1), dpi=100)
    #fig,ax=mpf.plot(count_high_values_merged_full_df,type='candle', returnfig=True )
    #chart=fig.add_subplot(111)
    #chart_canvas=tk.Canvas(win,background='white',width=900,height=1000)
    #kwargs=dict(title='Candlestick plot', figratio=(15,10),figscale=5)
    #plt.get_backend ()
    mc=mpf.make_marketcolors(up='green',down='red',edge='black', wick='i',volume='in', inherit=True)
    s=mpf.make_mpf_style(gridaxis='both',gridstyle='-.',y_on_right=False,marketcolors=mc)
    save_config_var=dict(fname=os.path.join(os.getcwd(),"datasets","plots",f'{symbol}.png'), dpi=800)
    hlines_dict=dict(hlines=tuple(levels_with_2_touches['High'].tolist()),
                     colors=tuple(["g"]*len(levels_with_2_touches)),
                     linewidths=tuple([0.1]*len(levels_with_2_touches)))
    levels_highs_with_2_touches_dict=mpf.make_addplot(levels_with_2_touches_all_dates["High"],type='scatter',
                                                      markersize=0.1)

    fig,ax=mpf.plot(count_high_values_merged_full_df,title=f'{symbol}',type='candle',style=s,volume=False,
                    returnfig=True,addplot=levels_highs_with_2_touches_dict,hlines=hlines_dict,
                    savefig=save_config_var,tight_layout=True)
    chart_canvas=FigureCanvasTkAgg(fig, master=win)
    chart_canvas.draw()
    chart_canvas.get_tk_widget().place(relx=0,rely=0,width=1350,height=680)
    #print("stock_df_rounded_ohlc\n",stock_df_rounded_ohlc.duplicated(subset='High'))
    #print(stock_df_rounded_ohlc['High'].value_counts().to_string ())
    #win.mainloop ()
    end_time=time.time()
    execution_time=end_time-start_time
    print(execution_time)

calculate_horizontal_levels_using_only_highs()