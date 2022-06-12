# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import time
import datetime
import async_fetch_historical_USDT_pairs_from_all_exchanges_with_ccxt
import find_mirror_levels_with_given_params_in_database
import drop_duplicates_in_db
import find_if_high_or_low_yesterday_coincides_with_mirror_level
import plot_ohlcv_chart_with_mirror_levels_from_given_exchange_with_recent_highs_and_lows
def main():
    start_time=time.time()
    async_fetch_historical_USDT_pairs_from_all_exchanges_with_ccxt.fetch_historical_usdt_pairs_asynchronously()
    find_mirror_levels_with_given_params_in_database.find_mirror_levels_in_database()
    drop_duplicates_in_db.drop_duplicates_in_db()
    find_if_high_or_low_yesterday_coincides_with_mirror_level.find_if_high_or_low_yesterday_coincides_with_mirror_level()
    plot_ohlcv_chart_with_mirror_levels_from_given_exchange_with_recent_highs_and_lows.plot_ohlcv_chart_with_mirror_levels_from_given_exchange()

    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 3600.0 )
    print ( 'overall time=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time=' , start_time )
    print ( 'end_time=' , end_time )

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
