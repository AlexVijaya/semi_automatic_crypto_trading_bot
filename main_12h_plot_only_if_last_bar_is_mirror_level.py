
import time
import os
import datetime

import find_mirror_levels_in_12h_postgres_database_only_if_last_bar_is_mirror_level
import drop_duplicate_mirror_levels_in_postgres_db_12h_only_last_bar_is_counted

import drop_tables_with_stablecoins_as_the_first_pair
import plot_mirror_levels_with_lower_timeframe_multiple_plots_postgres_database_is_used_12h_if_last_bar_is_mirror_level
import convert_multiple_jpeg_files_into_several_pdf_files_3_folders_as_arguments
import async_postgres_fetch_historical_USDT_pairs_for_12h_and_lower_timeframes

import insert_lower_timeframe_ohlcv_data_for_USDT_pair_mirror_levels_into_postgres_db_12h_last_bar_mirror_level
def main():
    start_time=time.time()

    try:
        timeframe = '12h'
        async_postgres_fetch_historical_USDT_pairs_for_12h_and_lower_timeframes. \
            fetch_all_ohlcv_tables (timeframe)


        database_with_stablecoin_names_as_first_name = "ohlcv_data_for_usdt_pairs_for_12h_and_lower_tf"

        drop_tables_with_stablecoins_as_the_first_pair.\
            drop_tables_in_db_if_first_pair_is_stablecoin(database_with_stablecoin_names_as_first_name)


        find_mirror_levels_in_12h_postgres_database_only_if_last_bar_is_mirror_level.\
            find_mirror_levels_in_database()



        drop_duplicate_mirror_levels_in_postgres_db_12h_only_last_bar_is_counted.\
            drop_duplicates_in_db()
        lower_timeframe_for_mirror_level_rebound_trading='1h'
        insert_lower_timeframe_ohlcv_data_for_USDT_pair_mirror_levels_into_postgres_db_12h_last_bar_mirror_level.\
            find_mirror_levels_in_database(lower_timeframe_for_mirror_level_rebound_trading)

        lower_timeframe_for_mirror_level_rebound_trading = '1h'
        plot_mirror_levels_with_lower_timeframe_multiple_plots_postgres_database_is_used_12h_if_last_bar_is_mirror_level.\
            plot_ohlcv_chart_with_mirror_levels_from_given_exchange (lower_timeframe_for_mirror_level_rebound_trading)






        folder_where_jpeg_files_are_located = os.path.join ( os.getcwd () ,
                                                             'datasets' ,
                                                             'plots' ,
                                                             'crypto_exchange_mirror_levels_plots_with_lower_time_frames_12h_only_if_last_bar_is_mirror_level' ,
                                                             'crypto_exchange_plots_jpg' )
        path_to_folder_with_pdfs_converted_from_jpegs = os.path.join ( os.getcwd () ,
                                                                       'datasets' ,
                                                                       'plots' ,
                                                                       'crypto_exchange_mirror_levels_plots_with_lower_time_frames_12h_only_if_last_bar_is_mirror_level' ,
                                                                       'crypto_exchange_plots_pdf' )
        path_to_folder_with_a_single_final_pdf = os.path.join ( os.getcwd () ,
                                                                'datasets' ,
                                                                'plots' ,
                                                                'crypto_exchange_mirror_levels_plots_with_lower_time_frames_12h_only_if_last_bar_is_mirror_level' ,
                                                                'single_merged_pdf' )
        convert_multiple_jpeg_files_into_several_pdf_files_3_folders_as_arguments.\
            convert_multiple_jpegs_into_a_single_pdf_for_daily_and_lower_timeframes(folder_where_jpeg_files_are_located,
                                                                            path_to_folder_with_pdfs_converted_from_jpegs,
                                                                            path_to_folder_with_a_single_final_pdf)

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
