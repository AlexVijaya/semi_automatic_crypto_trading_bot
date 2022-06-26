# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import time
import datetime
import async_fetch_historical_USDT_pairs_from_all_exchanges_with_ccxt
import find_keltner_channel
import find_mirror_levels_with_given_params_in_database
import drop_duplicates_in_db
import find_if_high_or_low_yesterday_coincides_with_mirror_level
#import plot_ohlcv_chart_with_mirror_levels_from_given_exchange
import plot_ohlcv_chart_with_mirror_levels_from_async_db_with_triple_table_name
import plot_ohlcv_chart_with_mirror_levels_from_given_exchange_with_recent_highs_and_lows
#import second_fetch_historical_USDT_pairs_ohlc_from_all_exchanges_with_ccxt
import plot_ohlcv_chart_with_price_approaching_mirror_levels
def main():
    start_time=time.time()
    async_var=True
    try:
        async_fetch_historical_USDT_pairs_from_all_exchanges_with_ccxt.fetch_historical_usdt_pairs_asynchronously()
        find_mirror_levels_with_given_params_in_database.find_mirror_levels_in_database(async_var)
        # if async_var==False:
        #     second_fetch_historical_USDT_pairs_ohlc_from_all_exchanges_with_ccxt.find_mirror_levels_with_given_params_in_database.find_mirror_levels_in_database(async_var)
        drop_duplicates_in_db.drop_duplicates_in_db()
        find_keltner_channel.find_mirror_levels_in_database_and_add_kc_to_db(async_var)
        plot_ohlcv_chart_with_mirror_levels_from_async_db_with_triple_table_name.plot_ohlcv_chart_with_mirror_levels_from_given_exchange()
        find_if_high_or_low_yesterday_coincides_with_mirror_level.find_mirror_levels_in_database(async_var)
        plot_ohlcv_chart_with_mirror_levels_from_given_exchange_with_recent_highs_and_lows.plot_ohlcv_chart_with_mirror_levels_from_given_exchange(async_var)
        plot_ohlcv_chart_with_price_approaching_mirror_levels.plot_ohlcv_chart_with_price_approaching_mirror_levels()
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
