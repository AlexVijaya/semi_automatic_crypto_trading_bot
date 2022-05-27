import time
from calculate_horizontal_levels_using_lows_in_db_from_yf import calculate_horizontal_levels_using_only_lows
from calculate_horizontal_levels_using_highs_in_db_from_yf_new import calculate_horizontal_levels_using_only_highs
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from yield_gerchik_symbol import yield_gerchik_symbol
def plot_levels_with_plotly():
    level_for_this_period=60
    round_to_this_number_of_decimal_places = 2

    number_2level_touches_with_low = 2
    number_of_more_than_3level_touches_with_low = 3

    number_2level_touches_with_high = 2
    number_of_more_than_3level_touches_with_high = 3
    i=0
    denominator = 10
    number_of_charts = 0
    symbols=[ticker for ticker in yield_gerchik_symbol()]
    print(symbols)
    for symbol in symbols:
        print(symbols.index ( symbol ))
        if (symbols.index ( symbol ) + 1) % denominator == 0:
            number_of_charts = number_of_charts + 1
    print ( number_of_charts )
    #time.sleep(1)




    fig = make_subplots ( rows = 1 , cols = number_of_charts ,
                          shared_xaxes = True,subplot_titles=tuple(symbols),
                          horizontal_spacing =0.003 )
    # fig.show()
    # time.sleep(10)

        # ,
        #                   subplot_titles = (['plot{}'.format(number_of_row)
        #                                      for number_of_row in range(1,number_of_charts+1)]))
    for symbol in yield_gerchik_symbol():
        print ( "symbol" , symbol )
        try:
            # if symbol == "BIDU":
            #     print('symbol is', symbol)
            #     time.sleep(10)
            #     continue

            # it is hard to plot many charts with plotly. Because the browser freezes. So I
            # chose to use denominator to reduce the number of plots. I don't plot all
            # charts. The number of plots is all_downloaded stocks modulo denominator




            count_low_values_merged_full_df , low_levels_with_2touches , low_levels_with_2touches_all_dates = \
                calculate_horizontal_levels_using_only_lows ( symbol ,
                                                              round_to_this_number_of_decimal_places ,
                                                              number_2level_touches_with_low,
                                                              level_for_this_period )
            count_low_values_merged_full_df , low_levels_with_3touches , low_levels_with_3touches_all_dates = \
                calculate_horizontal_levels_using_only_lows ( symbol ,
                                                              round_to_this_number_of_decimal_places ,
                                                              number_of_more_than_3level_touches_with_low,
                                                              level_for_this_period )

            count_high_values_merged_full_df , high_levels_with_2touches , high_levels_with_2touches_all_dates = \
                calculate_horizontal_levels_using_only_highs ( symbol ,
                                                               round_to_this_number_of_decimal_places ,
                                                               number_2level_touches_with_high,
                                                               level_for_this_period )
            count_high_values_merged_full_df , high_levels_with_3touches , high_levels_with_3touches_all_dates = \
                calculate_horizontal_levels_using_only_highs ( symbol ,
                                                               round_to_this_number_of_decimal_places ,
                                                               number_of_more_than_3level_touches_with_high,
                                                               level_for_this_period )
            # if count_high_values_merged_full_df.empty:
            #     print("symbol with empty df=", symbol)
            #     time.sleep(2)
            #     continue
            # if count_low_values_merged_full_df.empty:
            #     print ( "symbol with empty df=" , symbol )
            #     time.sleep ( 2 )
            #     continue
            print ( "count_high_values_merged_full_df\n" ,
                    count_high_values_merged_full_df.head ( 10000 ).to_string () )
            print ( "high_levels_with_3touches\n" ,
                    high_levels_with_3touches.head ( 10000 ).to_string () )
            print ( "high_levels_with_3touches_all_dates\n" ,
                    high_levels_with_3touches_all_dates.head ( 10000 ).to_string () )
            print ( "high_levels_with_2touches_all_dates\n" ,
                    high_levels_with_2touches_all_dates.head ( 10000 ).to_string () )
            print ( "low_levels_with_3touches\n" ,
                    low_levels_with_3touches.head ( 10000 ).to_string () )
            print ( "low_levels_with_3touches_all_dates\n" ,
                    low_levels_with_3touches_all_dates.head ( 10000 ).to_string () )
            print ( "low_levels_with_2touches_all_dates\n" ,
                    low_levels_with_2touches_all_dates.head ( 10000 ).to_string () )

            if (symbols.index ( symbol ) + 1) % denominator == 0:
                i = i + 1
                print ( "i%2+1=" , i % 2 + 1 )

                print ( "symbols.index(symbol)+1" , symbols.index ( symbol ) + 1 )
                print("i=",i)



                fig.add_trace ( go.Candlestick (
                    x = count_high_values_merged_full_df.index ,
                    open = count_high_values_merged_full_df['Open'] ,
                    high = count_high_values_merged_full_df['High'] ,
                    low = count_high_values_merged_full_df['Low'] ,
                    close = count_high_values_merged_full_df['Close'] ,
                    increasing_line_color = 'green' , decreasing_line_color = 'red'
                ) , row=1 , col=i )
                #fig.layout.xaxis.type='category'
                # fig.add_shape(type='line',x0=count_high_values_merged_full_df.iloc[-10].name,
                #               x1=count_high_values_merged_full_df.iloc[-1].name,y0=40,y1=40)
                for row_number,element in enumerate(high_levels_with_2touches):
                    fig.add_shape ( type = 'line' , x0 = high_levels_with_2touches["Date"].iloc[row_number] ,
                                               x1=count_high_values_merged_full_df.iloc[-1].name,
                                    y0=high_levels_with_2touches["High"].iloc[row_number],
                                    y1=high_levels_with_2touches["High"].iloc[row_number],
                                    line=dict(color="aqua",width=1), row=1 , col=i )

                for row_number,element in enumerate(low_levels_with_2touches):
                    fig.add_shape ( type = 'line' , x0 = low_levels_with_2touches["Date"].iloc[row_number] ,
                                               x1=count_low_values_merged_full_df.iloc[-1].name,
                                    y0=low_levels_with_2touches["Low"].iloc[row_number],
                                    y1=low_levels_with_2touches["Low"].iloc[row_number],
                                    line=dict(color="purple",width=1), row=1 , col=i)

                for row_number,element in enumerate(high_levels_with_3touches):
                    fig.add_shape ( type = 'line' , x0 = high_levels_with_3touches["Date"].iloc[row_number] ,
                                               x1=count_high_values_merged_full_df.iloc[-1].name,
                                    y0=high_levels_with_3touches["High"].iloc[row_number],
                                    y1=high_levels_with_3touches["High"].iloc[row_number],
                                    line=dict(color="aqua",width=1), row=1 , col=i )

                for row_number,element in enumerate(low_levels_with_3touches):
                    fig.add_shape ( type = 'line' , x0 = low_levels_with_3touches["Date"].iloc[row_number] ,
                                               x1=count_low_values_merged_full_df.iloc[-1].name,
                                    y0=low_levels_with_3touches["Low"].iloc[row_number],
                                    y1=low_levels_with_3touches["Low"].iloc[row_number],
                                    line=dict(color="purple",width=1), row=1 , col=i)


                fig.add_scatter(x=high_levels_with_2touches["Date"],
                                y=high_levels_with_2touches["High"],mode="markers",
                                marker=dict(color='blue',size=5),
                                name="high_levels_with_2touches", row=1 , col=i)
                fig.add_scatter ( x = high_levels_with_3touches["Date"] ,
                                  y = high_levels_with_3touches["High"] , mode = "markers" ,
                                  marker = dict ( color = 'black' , size = 5 ) ,
                                  name = "high_levels_with_3_or_more_touches" , row=1 , col=i)
                fig.add_scatter ( x = low_levels_with_2touches["Date"] ,
                                  y = low_levels_with_2touches["Low"] , mode = "markers" ,
                                  marker = dict ( color = 'cyan' , size = 5 ) ,
                                  name = "low_levels_with_2touches" , row=1 , col=i)
                fig.add_scatter ( x = low_levels_with_3touches["Date"] ,
                                  y = low_levels_with_3touches["Low"] , mode = "markers" ,
                                  marker = dict ( color = 'magenta' , size = 5 ) ,
                                  name = "low_levels_with_3_or_more_touches" , row=1 , col=i)
                fig.update_xaxes ( patch = dict ( type = 'category' ) , row=1 , col=i )
                fig.update_xaxes ( rangeslider = {'visible': False} , row=1 , col=i )
                fig.update_layout ( height = 700  , width = 20000 * i, title_text = 'Charts of some stocks' )
                fig['layout'][f'xaxis{i}']['title'] = 'dates for ' + symbol
                fig.layout.annotations[i - 1].update ( text = symbol )
                fig.print_grid ()
                print(count_high_values_merged_full_df.iloc[-1].name)
                print(count_high_values_merged_full_df.iloc[0].name)
        except Exception as e:
            print(f"problem with {symbol}")
            continue
    fig.show ()
plot_levels_with_plotly()
