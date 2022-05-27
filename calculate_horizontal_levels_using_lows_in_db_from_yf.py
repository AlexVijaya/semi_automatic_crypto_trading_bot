import os
import time

import numpy as np

import sqlite3
import pandas as pd


def calculate_horizontal_levels_using_only_lows(symbol,round_to_this_number_of_decimal_places,
                                                number_of_same_lows,level_for_this_period):
    start_time=time.time()
    conn=sqlite3.connect( os.path.join( os.getcwd() , 'datasets','sql_databases','yf_db_historical_data_new.db' ) )
    cur=conn.cursor()
    conn.row_factory=sqlite3.Row

    #cur.execute('''select * from stock_prices where stock_id=10 order by id''')

    stock_df = pd.read_sql ( f'''select * from (select * from yf_gerchik_tickers_and_prices
                                        where symbol="{symbol}"
                                        order by Date Desc limit {level_for_this_period})
                                        order by Date ASC;''' ,
                             conn , parse_dates = 'Date' )

    # stock_df=pd.read_sql(f'''select * from yf_gerchik_tickers_and_prices where symbol="{symbol}" ;''',
    #                      conn,parse_dates = 'Date')

    stock_df_rounded=stock_df.round({'Open':round_to_this_number_of_decimal_places,
                                     'High':round_to_this_number_of_decimal_places,
                                     'Low':round_to_this_number_of_decimal_places,
                                     'Close':round_to_this_number_of_decimal_places})
    stock_df_rounded_ohlc=stock_df_rounded.loc[:,['Date','Open','High','Low','Close']]

    # stock_df_rounded_ohlc_with_equal_lows_keep_false = \
    #     stock_df_rounded_ohlc[stock_df_rounded_ohlc.duplicated ( subset = ["Low"] ,keep = False)]

    stock_df_rounded_ohlc_with_equal_lows_keep_false = stock_df_rounded_ohlc

    stock_df_rounded_ohlc_with_equal_lows_keep_false_sorted=\
        stock_df_rounded_ohlc_with_equal_lows_keep_false.sort_values ( by = "Date" )

    stock_series_rounded_ohlc_with_equal_lows_keep_false=\
        stock_df_rounded_ohlc_with_equal_lows_keep_false['Low'].value_counts()
    df_count_low_values=stock_series_rounded_ohlc_with_equal_lows_keep_false.to_frame ()

    df_count_low_values.reset_index(level=0, inplace=True)


    df_count_low_values.rename({'Low':'number_of_same_lows', 'index':'Low'}, axis='columns', inplace=True)
    df_count_low_values_merged=pd.merge(stock_df_rounded_ohlc_with_equal_lows_keep_false_sorted,
                                        df_count_low_values[['Low','number_of_same_lows']],
                                        on='Low',how='left' )
    count_low_values_merged_full_df=pd.merge(stock_df_rounded,
                                             df_count_low_values_merged[['Low', 'number_of_same_lows']],
                                             on='Low' ,how='left')

    count_low_values_merged_full_df.drop_duplicates(subset='Date',inplace=True)
    count_low_values_merged_full_df.reset_index(inplace = True,drop=True)
    count_low_values_merged_full_df.update(stock_df)

    count_low_values_merged_full_df["NaN_Lows"] = np.nan




    levels_with_touches=\
        count_low_values_merged_full_df.loc[count_low_values_merged_full_df["number_of_same_lows"]>=number_of_same_lows]
    #print("levels_with_touches\n",levels_with_touches)

    levels_with_touches_all_dates=pd.merge(count_low_values_merged_full_df.loc[:,["Date","NaN_Lows"]],
                                            levels_with_touches.loc[:,["Date","Low"]],
                                           on="Date",how='left')
    #print ( "levels_with_touches_all_dates\n" , levels_with_touches_all_dates )
    count_low_values_merged_full_df['Date'] = pd.to_datetime ( count_low_values_merged_full_df['Date'] )
    count_low_values_merged_full_df.set_index ( 'Date' , inplace = True )

    levels_with_touches_all_dates['Date'] = pd.to_datetime ( levels_with_touches_all_dates['Date'] )
    levels_with_touches_all_dates.set_index ( 'Date' , inplace = True )
    #print ( "count_low_values_merged_full_df\n" , count_low_values_merged_full_df )
    #print(levels_with_touches['Low'].tolist())
    #print(tuple([0.5]*len(levels_with_touches)))
    print ( "count_low_values_merged_full_df\n" ,
            count_low_values_merged_full_df.head ( 10000 ).to_string () )
    print ( "levels_with_touches\n" ,
            levels_with_touches.head ( 10000 ).to_string () )
    print ( "levels_with_touches_all_dates\n" ,
            levels_with_touches_all_dates.head ( 10000 ).to_string () )
    conn.close ()
    return count_low_values_merged_full_df , levels_with_touches , levels_with_touches_all_dates

#calculate_horizontal_levels_using_only_lows()





