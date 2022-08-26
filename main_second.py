# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import time
import datetime
import async_postgres_fetch_historical_USDT_pairs_from_all_exchanges_with_ccxt
# import find_keltner_channel
import find_mirror_levels_in_postgres_database
import drop_duplicates_in_postgres_db
import find_if_high_or_low_yesterday_coincides_with_mirror_level_postgres_used
#import plot_ohlcv_chart_with_mirror_levels_from_given_exchange
# import plot_ohlcv_chart_with_mirror_levels_from_async_db_with_triple_table_name
# import plot_ohlcv_chart_with_mirror_levels_from_given_exchange_with_recent_highs_and_lows
#import second_fetch_historical_USDT_pairs_ohlc_from_all_exchanges_with_ccxt
import plot_mirror_levels_postgres_database_is_used
import find_levels_formed_by_highs_and_lows_postgres
import plot_levels_formed_by_high_data_in_postgres_db
import plot_levels_formed_by_low_data_in_postgres_db
import plot_levels_formed_by_last_three_or_two_highs_data_in_postgres
import plot_levels_formed_by_last_three_or_two_lows_data_in_postgres
import find_if_asset_is_approaching_mirror_level_percentage_wise_postgres_db
import plot_ohlcv_chart_with_mirror_levels_equal_to_yesterday_highs_and_lows_postgres_used
import plot_ohlcv_chart_with_price_approaching_mirror_levels_postgres_used
import check_if_current_bar_had_high_or_low_within_acceptable_range_to_mirror_level_which_was_a_certain_period_of_time_ago_postgres_used
import plot_ready_for_rebound_ohlcv_chart_with_mirror_levels_with_triple_table_name_postgres_used
def main():
    start_time=time.time()

    try:

        async_postgres_fetch_historical_USDT_pairs_from_all_exchanges_with_ccxt.\
            fetch_all_ohlcv_tables()


        find_mirror_levels_in_postgres_database.\
            find_mirror_levels_in_database()



        drop_duplicates_in_postgres_db.\
            drop_duplicates_in_db()




        find_levels_formed_by_highs_and_lows_postgres.\
            find_levels_formed_by_highs_and_lows()



        plot_levels_formed_by_high_data_in_postgres_db.\
            plot_ohlcv_chart_with_levels_formed_by_highs()


        plot_levels_formed_by_low_data_in_postgres_db.\
            plot_ohlcv_chart_with_levels_formed_by_lows()



        plot_levels_formed_by_last_three_or_two_highs_data_in_postgres.\
            plot_ohlcv_chart_with_levels_formed_by_highs()



        plot_levels_formed_by_last_three_or_two_lows_data_in_postgres.\
            plot_ohlcv_chart_with_levels_formed_by_lows()


        plot_mirror_levels_postgres_database_is_used.\
            plot_ohlcv_chart_with_mirror_levels_from_given_exchange()


        find_if_high_or_low_yesterday_coincides_with_mirror_level_postgres_used.\
            find_mirror_levels_in_database()


        plot_ohlcv_chart_with_mirror_levels_equal_to_yesterday_highs_and_lows_postgres_used.\
            plot_ohlcv_chart_with_mirror_levels_from_given_exchange()


        find_if_asset_is_approaching_mirror_level_percentage_wise_postgres_db.\
            find_if_asset_is_approaching_mirror_level_percentage_wise()


        plot_ohlcv_chart_with_price_approaching_mirror_levels_postgres_used.\
            plot_ohlcv_chart_with_price_approaching_mirror_levels()

        lower_timeframe_for_mirror_level_rebound_trading = '1h'
        check_if_current_bar_had_high_or_low_within_acceptable_range_to_mirror_level_which_was_a_certain_period_of_time_ago_postgres_used.\
            check_if_current_bar_closed_within_acceptable_range_to_mirror_level_which_last_was_this_period_ago (
            lower_timeframe_for_mirror_level_rebound_trading )

        plot_ready_for_rebound_ohlcv_chart_with_mirror_levels_with_triple_table_name_postgres_used.\
            plot_ohlcv_chart_with_mirror_levels_from_given_exchange (lower_timeframe_for_mirror_level_rebound_trading)




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
