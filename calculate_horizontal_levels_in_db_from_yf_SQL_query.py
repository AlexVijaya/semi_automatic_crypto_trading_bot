import os
import time
import pprint
import sqlite3
import pandas as pd
def calculate_horizontal_levels():
    start_time=time.time()
    conn=sqlite3.connect( os.path.join( os.getcwd() , 'datasets','sql_databases','yf_db_historical_data_new.db' ) )
    cur=conn.cursor()
    conn.row_factory=sqlite3.Row
    round_to_this_number_of_decimal_places = 4
    #cur.execute('''select * from stock_prices where stock_id=10 order by id''')
    stock_df=pd.read_sql('''select * from yf_gerchik_tickers_and_prices where symbol="TSLA)" 
                            ;''',
                         conn,parse_dates = 'Date')
    round_to_this_number_of_decimal_places=4

    end_time=time.time()
    execution_time=end_time-start_time
    print(execution_time)
    #
    # pprint.pprint ( stock_df )
    # pprint.pprint(stock_df_rounded)
    # pprint.pprint ( stock_df_rounded_ohlc )
    # pprint.pprint(current_high)
    # pprint.pprint(number_of_rows_in_stock_df)


calculate_horizontal_levels()