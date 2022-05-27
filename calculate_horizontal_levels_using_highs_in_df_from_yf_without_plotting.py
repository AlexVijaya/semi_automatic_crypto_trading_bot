import os
import time

import numpy as np

import sqlite3
import pandas as pd


def calculate_horizontal_levels_using_only_highs(symbol,round_to_this_number_of_decimal_places,number_of_same_highs):
    start_time=time.time()
    conn=sqlite3.connect( os.path.join( os.getcwd() , 'datasets','sql_databases','yf_db_historical_data_new.db' ) )
    cur=conn.cursor()
    conn.row_factory=sqlite3.Row
    print (f'symbol is {symbol}')

    #cur.execute('''select * from stock_prices where stock_id=10 order by id''')



    stock_df=pd.read_sql(f'''select * from yf_gerchik_tickers_and_prices where symbol="{symbol}" ;''',
                         conn,parse_dates = 'Date')
    #stock_df=stock_df.tail(253)
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
    # print ( "count_high_values_merged_full_df\n",count_high_values_merged_full_df.head ( 10000).to_string () )
    # print("levels_with_touches\n",levels_with_touches.head ( 10000).to_string () )
    # print ( "df_count_high_values_merged\n" ,
    #         df_count_high_values_merged.head ( 10000 ).to_string () )
    count_high_values_merged_full_df["NaN_Highs"] = np.nan




    levels_with_touches=\
        count_high_values_merged_full_df.loc[count_high_values_merged_full_df["number_of_same_highs"]>=number_of_same_highs]
    #print("levels_with_touches\n",levels_with_touches)

    levels_with_touches_all_dates=pd.merge(count_high_values_merged_full_df.loc[:,["Date","NaN_Highs"]],
                                            levels_with_touches.loc[:,["Date","High"]],
                                           on="Date",how='left')
    #print ( "levels_with_touches_all_dates\n" , levels_with_touches_all_dates )
    count_high_values_merged_full_df['Date'] = pd.to_datetime ( count_high_values_merged_full_df['Date'] )
    count_high_values_merged_full_df.set_index ( 'Date' , inplace = True )

    levels_with_touches_all_dates['Date'] = pd.to_datetime ( levels_with_touches_all_dates['Date'] )
    levels_with_touches_all_dates.set_index ( 'Date' , inplace = True )
    #print ( "count_high_values_merged_full_df\n" , count_high_values_merged_full_df )
    #print(levels_with_touches['High'].tolist())
    #print(tuple([0.5]*len(levels_with_touches)))
    print ( "count_high_values_merged_full_df\n" ,
            count_high_values_merged_full_df.head ( 10000 ).to_string () )
    #time.sleep(20)
    print ( "levels_with_touches\n" ,
            levels_with_touches.head ( 10000 ).to_string () )
    print ( "levels_with_touches_all_dates\n" ,
            levels_with_touches_all_dates.head ( 10000 ).to_string () )
    return count_high_values_merged_full_df , levels_with_touches , levels_with_touches_all_dates
#calculate_horizontal_levels_using_only_highs()





