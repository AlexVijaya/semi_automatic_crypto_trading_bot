# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import time
import datetime
# import async_postgres_fetch_historical_USDT_pairs_from_all_exchanges_with_ccxt
# import find_keltner_channel
import find_mirror_levels_in_12h_postgres_database
import drop_duplicate_mirror_levels_in_postgres_db_12h
import find_if_high_or_low_yesterday_coincides_with_mirror_level_postgres_used_12h
#import plot_ohlcv_chart_with_mirror_levels_from_given_exchange
# import plot_ohlcv_chart_with_mirror_levels_from_async_db_with_triple_table_name
# import plot_ohlcv_chart_with_mirror_levels_from_given_exchange_with_recent_highs_and_lows
#import second_fetch_historical_USDT_pairs_ohlc_from_all_exchanges_with_ccxt
import plot_mirror_levels_postgres_database_is_used_12h
import find_levels_formed_by_highs_and_lows_postgres_12h
import plot_levels_formed_by_high_data_in_postgres_db_12h
import plot_levels_formed_by_low_data_in_postgres_db_12h
import plot_levels_formed_by_last_three_or_two_highs_data_in_postgres_12h
import plot_levels_formed_by_last_three_or_two_lows_data_in_postgres_12h
import find_if_asset_is_approaching_mirror_level_percentage_wise_postgres_db_12h
import plot_ohlcv_chart_with_mirror_levels_equal_to_yesterday_highs_and_lows_postgres_used_12h
import plot_ohlcv_chart_with_price_approaching_mirror_levels_postgres_used_12h
import check_if_current_bar_had_high_or_low_within_acceptable_range_to_mirror_level_which_was_a_certain_period_of_time_ago_postgres_used_12h
import plot_ready_for_rebound_ohlcv_chart_with_mirror_levels_with_triple_table_name_postgres_used
import drop_tables_with_stablecoins_as_the_first_pair_rounded_ohlcv_also_used
import drop_tables_with_stablecoins_as_the_first_pair
import plot_mirror_levels_with_lower_timeframe_multiple_plots_postgres_database_is_used_12h
import convert_multiple_jpeg_files_into_several_pdf_files
import async_postgres_fetch_historical_USDT_pairs_for_12h_and_lower_timeframes
import drop_duplicates_in_postgres_db_rounded_ohlcv_is_used
import find_levels_formed_by_highs_and_lows_postgres_rounded_ohlcv_is_used
import find_if_rounded_high_or_low_yesterday_coincides_with_rounded_mirror_level_postgres_used
from multiprocessing import Process
import find_rounded_mirror_levels_in_postgres_database
import insert_lower_timeframe_ohlcv_data_for_USDT_pair_mirror_levels_into_postgres_db_12h
def main():
    start_time=time.time()

    try:
        timeframe = '12h'
        async_postgres_fetch_historical_USDT_pairs_for_12h_and_lower_timeframes. \
            fetch_all_ohlcv_tables (timeframe)

        # async_postgres_fetch_historical_ohlcv_of_USDT_pairs_round_them_and_insert_into_separate_db.\
        #     fetch_all_ohlcv_tables()
        #
        database_with_stablecoin_names_as_first_name = "ohlcv_data_for_usdt_pairs_for_12h_and_lower_tf"

        drop_tables_with_stablecoins_as_the_first_pair.\
            drop_tables_in_db_if_first_pair_is_stablecoin(database_with_stablecoin_names_as_first_name)

        #
        # drop_tables_with_stablecoins_as_the_first_pair_rounded_ohlcv_also_used.\
        #     drop_tables_in_db_if_first_pair_is_stablecoin()
        #
        #


        #
        find_levels_process_list=[]
        proc1=Process(target=find_mirror_levels_in_12h_postgres_database.find_mirror_levels_in_database)
        proc1.start ()
        find_levels_process_list.append(proc1)
        so_many_last_days_for_level_calculation=30
        proc2 = Process ( target = find_levels_formed_by_highs_and_lows_postgres_12h.find_levels_formed_by_highs_and_lows,
                          args=(so_many_last_days_for_level_calculation,)  )
        proc2.start ()
        find_levels_process_list.append ( proc2 )

        for proc in find_levels_process_list:
            proc.join()

        drop_duplicate_mirror_levels_in_postgres_db_12h.\
            drop_duplicates_in_db()


        list_after_mirror_low_and_high_levels_were_found=[]

        proc3 = Process (
            target = find_if_high_or_low_yesterday_coincides_with_mirror_level_postgres_used_12h.find_mirror_levels_in_database )
        proc3.start ()
        list_after_mirror_low_and_high_levels_were_found.append ( proc3 )

        proc4=Process(target=find_if_asset_is_approaching_mirror_level_percentage_wise_postgres_db_12h.\
            find_if_asset_is_approaching_mirror_level_percentage_wise)
        proc4.start()
        list_after_mirror_low_and_high_levels_were_found.append ( proc4 )

        lower_timeframe_for_mirror_level_rebound_trading = '1h'
        proc5 = Process ( target = check_if_current_bar_had_high_or_low_within_acceptable_range_to_mirror_level_which_was_a_certain_period_of_time_ago_postgres_used_12h.\
            check_if_current_bar_closed_within_acceptable_range_to_mirror_level_which_last_was_this_period_ago,args=(lower_timeframe_for_mirror_level_rebound_trading,) )
        proc5.start ()
        list_after_mirror_low_and_high_levels_were_found.append ( proc5 )

        lower_timeframe_for_mirror_level_rebound_trading = '1h'
        proc6 = Process (
            target = insert_lower_timeframe_ohlcv_data_for_USDT_pair_mirror_levels_into_postgres_db_12h. \
                find_mirror_levels_in_database ,
            args = (lower_timeframe_for_mirror_level_rebound_trading ,) )
        proc6.start ()
        list_after_mirror_low_and_high_levels_were_found.append ( proc6 )

        for proc in list_after_mirror_low_and_high_levels_were_found:
            proc.join()

        lower_timeframe_for_mirror_level_rebound_trading = '1h'
        list_of_plotting_functions=[plot_levels_formed_by_high_data_in_postgres_db_12h.\
                                    plot_ohlcv_chart_with_levels_formed_by_highs,
                                    plot_levels_formed_by_low_data_in_postgres_db_12h. \
                                    plot_ohlcv_chart_with_levels_formed_by_lows,
                                    plot_levels_formed_by_last_three_or_two_highs_data_in_postgres_12h. \
                                    plot_ohlcv_chart_with_levels_formed_by_highs,
                                    plot_levels_formed_by_last_three_or_two_lows_data_in_postgres_12h. \
                                    plot_ohlcv_chart_with_levels_formed_by_lows,
                                    plot_mirror_levels_with_lower_timeframe_multiple_plots_postgres_database_is_used_12h. \
                                    plot_ohlcv_chart_with_mirror_levels_from_given_exchange,
                                    plot_ohlcv_chart_with_mirror_levels_equal_to_yesterday_highs_and_lows_postgres_used_12h. \
                                    plot_ohlcv_chart_with_mirror_levels_from_given_exchange,
                                    plot_mirror_levels_postgres_database_is_used_12h. \
                                    plot_ohlcv_chart_with_mirror_levels_from_given_exchange,
                                    plot_ohlcv_chart_with_price_approaching_mirror_levels_postgres_used_12h. \
                                    plot_ohlcv_chart_with_price_approaching_mirror_levels,
                                    ]
        plotting_functions_process_list=[]
        for target_function in list_of_plotting_functions:
            proc=Process(target=target_function)
            proc.start()
            plotting_functions_process_list.append(proc)

        proc6 = Process (
            target = plot_ready_for_rebound_ohlcv_chart_with_mirror_levels_with_triple_table_name_postgres_used.\
                        plot_ohlcv_chart_with_mirror_levels_from_given_exchange ,
            args = (lower_timeframe_for_mirror_level_rebound_trading ,) )
        proc6.start ()
        plotting_functions_process_list.append ( proc6 )

        for proc in plotting_functions_process_list:
            proc.join()




        # find_mirror_levels_in_postgres_database.\
        #     find_mirror_levels_in_database()
        #
        # find_levels_formed_by_highs_and_lows_postgres. \
        #     find_levels_formed_by_highs_and_lows ()

        #
        # find_rounded_mirror_levels_in_postgres_database. \
        #     find_mirror_levels_in_database ()
        #
        #
        #

        # find_if_high_or_low_yesterday_coincides_with_mirror_level_postgres_used.\
        #     find_mirror_levels_in_database()
        #

        # find_if_rounded_high_or_low_yesterday_coincides_with_rounded_mirror_level_postgres_used.\
        #     find_mirror_levels_in_database()

        # find_levels_formed_by_highs_and_lows_postgres_rounded_ohlcv_is_used. \
        #     find_levels_formed_by_highs_and_lows ()
        #


        # drop_duplicate_mirror_levels_in_postgres_db.\
        #     drop_duplicates_in_db()

        # drop_duplicates_in_postgres_db_rounded_ohlcv_is_used.\
        #     drop_duplicates_in_db()
        #






        #
        # plot_levels_formed_by_high_data_in_postgres_db.\
        #     plot_ohlcv_chart_with_levels_formed_by_highs()
        #
        #
        # plot_levels_formed_by_low_data_in_postgres_db.\
        #     plot_ohlcv_chart_with_levels_formed_by_lows()
        #
        #
        #
        # plot_levels_formed_by_last_three_or_two_highs_data_in_postgres.\
        #     plot_ohlcv_chart_with_levels_formed_by_highs()
        #
        #
        #
        # plot_levels_formed_by_last_three_or_two_lows_data_in_postgres.\
        #     plot_ohlcv_chart_with_levels_formed_by_lows()
        #
        #
        # plot_mirror_levels_with_lower_timeframe_multiple_plots_postgres_database_is_used.\
        #     plot_ohlcv_chart_with_mirror_levels_from_given_exchange()
        #
        #

        #
        #
        # plot_ohlcv_chart_with_mirror_levels_equal_to_yesterday_highs_and_lows_postgres_used.\
        #     plot_ohlcv_chart_with_mirror_levels_from_given_exchange()
        #
        # plot_mirror_levels_postgres_database_is_used. \
        #     plot_ohlcv_chart_with_mirror_levels_from_given_exchange ()
        #



        convert_multiple_jpeg_files_into_several_pdf_files.\
            convert_multiple_jpegs_into_a_single_pdf_for_daily_and_lower_timeframes()



        #
        # find_if_asset_is_approaching_mirror_level_percentage_wise_postgres_db.\
        #   find_if_asset_is_approaching_mirror_level_percentage_wise()
        #
        #
        # plot_ohlcv_chart_with_price_approaching_mirror_levels_postgres_used.\
        #     plot_ohlcv_chart_with_price_approaching_mirror_levels()
        #
        #
        #
        # lower_timeframe_for_mirror_level_rebound_trading = '1h'
        # check_if_current_bar_had_high_or_low_within_acceptable_range_to_mirror_level_which_was_a_certain_period_of_time_ago_postgres_used.\
        #     check_if_current_bar_closed_within_acceptable_range_to_mirror_level_which_last_was_this_period_ago (
        #     lower_timeframe_for_mirror_level_rebound_trading )
        #
        # plot_ready_for_rebound_ohlcv_chart_with_mirror_levels_with_triple_table_name_postgres_used.\
        #     plot_ohlcv_chart_with_mirror_levels_from_given_exchange (lower_timeframe_for_mirror_level_rebound_trading)
        #
        #


    except Exception as e:
        print(e)


    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time of the main program in minutes=' , overall_time / 60.0 )
    print ( 'overall time of the main program in hours=' , overall_time / 3600.0 )
    print ( 'overall time of the main program=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( start_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ))
    print ( 'end_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( end_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ))

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
