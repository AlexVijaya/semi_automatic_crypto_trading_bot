import time
from calculate_horizontal_levels_using_lows_in_db_from_yf import calculate_horizontal_levels_using_only_lows
from calculate_horizontal_levels_using_highs_in_db_from_yf_new import calculate_horizontal_levels_using_only_highs
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from yield_binance_trading_pair import yield_binance_trading_pair
import sqlite3
import os
import pandas as pd
def plot_binance_highs_and_lows_with_plotly():

    i=0
    denominator = 10
    number_of_charts = 0
    symbols=[ticker for ticker in yield_binance_trading_pair()]
    print(symbols)
    connection_to_db_to_include_sar_td_and_local_min_and_max = sqlite3.connect (
        os.path.join ( os.getcwd () , "datasets" ,
                       "sql_databases" ,
                       "binance_historical_data_with_sar_and_td_and_local_min_and_max.db" ) )
    for symbol in symbols:

        print(symbols.index ( symbol ))
        if (symbols.index ( symbol ) + 1) % denominator == 0:
            number_of_charts = number_of_charts + 1
    print ( number_of_charts )
    #time.sleep(1)




    fig = make_subplots ( rows = 1 , cols = number_of_charts ,
                          shared_xaxes = True,subplot_titles=tuple(symbols),
                          horizontal_spacing =0.001 )
    # fig.show()
    # time.sleep(10)

        # ,
        #                   subplot_titles = (['plot{}'.format(number_of_row)
        #                                      for number_of_row in range(1,number_of_charts+1)]))
    for symbol in yield_binance_trading_pair():
        print ( "symbol" , symbol )
        try:

            if (symbols.index ( symbol ) + 1) % denominator == 0:
                i = i + 1
                print ( "i%2+1=" , i % 2 + 1 )

                print ( "symbols.index(symbol)+1" , symbols.index ( symbol ) + 1 )
                print("i=",i)
                #time.sleep(2)

                sql_query_ohlc = pd.read_sql_query ( f'''select * from crypto_assets_ohlc_plus_sar_and_td 
                                                                        where trading_pair="{symbol}"''' ,
                                                     connection_to_db_to_include_sar_td_and_local_min_and_max )
                historical_data_for_trading_pair_df = pd.DataFrame ( sql_query_ohlc )

                fig.add_trace ( go.Candlestick (
                    x = historical_data_for_trading_pair_df['index'] ,
                    open = historical_data_for_trading_pair_df['open'] ,
                    high = historical_data_for_trading_pair_df['high'] ,
                    low = historical_data_for_trading_pair_df['low'] ,
                    close = historical_data_for_trading_pair_df['close'] ,
                    increasing_line_color = 'green' , decreasing_line_color = 'red'
                ) , row=1 , col=i )

                fig.add_scatter ( x = historical_data_for_trading_pair_df['index'] ,
                                  y = historical_data_for_trading_pair_df["sar"] , mode = "markers" ,
                                  marker = dict ( color = 'blue' , size = 2 ) ,
                                  name = "sar" , row = 1 , col = i )
                fig.add_scatter ( x = historical_data_for_trading_pair_df['index'] ,
                                  y = historical_data_for_trading_pair_df["local_max"] , mode = "markers" ,
                                  marker = dict ( color = 'cyan' , size = 5 ) ,
                                  name = "sar" , row = 1 , col = i )

                fig.add_scatter ( x = historical_data_for_trading_pair_df['index'] ,
                                  y = historical_data_for_trading_pair_df["local_min"] , mode = "markers" ,
                                  marker = dict ( color = 'magenta' , size = 5 ) ,
                                  name = "sar" , row = 1 , col = i )

                #fig.layout.xaxis.type='category'
                # fig.add_shape(type='line',x0=count_high_values_merged_full_df.iloc[-10].name,
                #               x1=count_high_values_merged_full_df.iloc[-1].name,y0=40,y1=40)

                fig.update_xaxes ( patch = dict ( type = 'category' ) , row=1 , col=i )
                fig.update_xaxes ( rangeslider = {'visible': False} , row=1 , col=i )
                fig.update_layout ( height = 700  , width = 5000 * i, title_text = 'Charts of some stocks' )
                fig['layout'][f'xaxis{i}']['title'] = 'dates for ' + symbol
                fig.layout.annotations[i - 1].update ( text = symbol )
                fig.print_grid ()

        except Exception as e:
            print(f"problem with {symbol}")
            continue
    fig.show ()
plot_binance_highs_and_lows_with_plotly()
