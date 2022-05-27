import os
import time

import sqlite3
import pandas as pd
def calculate_horizontal_levels_using_only_lows():
    start_time=time.time()
    conn=sqlite3.connect( os.path.join( os.getcwd() , 'datasets','sql_databases','yf_db_historical_data_new.db' ) )
    cur=conn.cursor()
    conn.row_factory=sqlite3.Row
    #cur.execute('''select * from stock_prices where stock_id=10 order by id''')
    stock_df=pd.read_sql('''select * from yf_gerchik_tickers_and_prices where symbol="UBER" ;''',
                         conn,parse_dates = 'Date')
    round_to_this_number_of_decimal_places=3
    stock_df_rounded=stock_df.round({'Open':round_to_this_number_of_decimal_places,
                                     'High':round_to_this_number_of_decimal_places,
                                     'Low':round_to_this_number_of_decimal_places,
                                     'Close':round_to_this_number_of_decimal_places})
    stock_df_rounded_ohlc=stock_df_rounded.loc[:,['Date','Open','High','Low','Close']]
    stock_df_rounded_ohlc_with_equal_highs_keep_false=\
        stock_df_rounded_ohlc[stock_df_rounded_ohlc.duplicated ( subset = ["High"] , keep = False )]


    stock_df_rounded_ohlc_with_equal_lows_keep_false = \
        stock_df_rounded_ohlc[stock_df_rounded_ohlc.duplicated ( subset = ["Low"] ,keep = False)]


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

    print(count_low_values_merged_full_df.head(400).to_string())
    count_low_values_merged_full_df.update(stock_df)
    print ( count_low_values_merged_full_df.head ( 10000).to_string () )

    end_time=time.time()
    execution_time=end_time-start_time
    print(execution_time)

calculate_horizontal_levels_using_only_lows()