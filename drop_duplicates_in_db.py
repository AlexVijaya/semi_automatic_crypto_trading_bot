import sqlite3
import pandas as pd
import os
import tzlocal
import datetime as dt
import time
import re
def drop_duplicates_in_db():
    start_time = time.time ()
    path_to_db_with_USDT_and_btc_pairs = os.path.join ( os.getcwd () , "datasets" ,
                                                        "sql_databases" ,
                                                        "btc_and_usdt_pairs_from_all_exchanges.db" )

    connection_to_btc_and_usdt_trading_pairs = \
        sqlite3.connect ( path_to_db_with_USDT_and_btc_pairs )
    mirror_levels_df=pd.read_sql_query("select * from mirror_levels_calculated_separately",
                                       connection_to_btc_and_usdt_trading_pairs)
    print("number of trading pairs with duplicates=",
          len(mirror_levels_df))

    mirror_levels_df.drop_duplicates(subset = ["USDT_pair","mirror_level"],
                                     keep = "first",
                                     inplace = True)
    # create additional boolian coulumn
    # which says if there are multiple mirror levels in this pair
    mirror_levels_df["pair_with_multiple_mirror_levels_on_one_exchange","exchange"]=\
        mirror_levels_df.duplicated(subset=["USDT_pair","exchange"],keep=False)

    mirror_levels_df.reset_index(inplace = True,drop = True)

    #drop pairs which begin with usd
    for row in range(0,len(mirror_levels_df.index)):
        print(row)
        trading_pair_string=mirror_levels_df.loc[row,"USDT_pair"]
        print(mirror_levels_df.loc[row,"USDT_pair"])
        print(re.search('^USD.*',trading_pair_string))
        if re.search('^US[D,T].*',trading_pair_string):
            mirror_levels_df.loc[row , "not_begins_with_USD_or_UST"]=False
        else:
            mirror_levels_df.loc[row , "not_begins_with_USD_or_UST"] = True

    mirror_levels_df=mirror_levels_df[mirror_levels_df["not_begins_with_USD_or_UST"]==True]
    mirror_levels_df.reset_index ( inplace = True , drop = True )

    mirror_levels_df.to_sql("mirror_levels_without_duplicates",
                            connection_to_btc_and_usdt_trading_pairs,
                            if_exists ='replace')
    print ( "number of trading pairs without duplicates=" ,
            len ( mirror_levels_df ) )

    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 3600.0 )
    print ( 'overall time=' , str ( dt.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time=' , start_time )
    print ( 'end_time=' , end_time )
    unix_timestamp_start = float ( start_time )
    unix_timestamp_end = float ( end_time )
    local_timezone = tzlocal.get_localzone ()  # get pytz timezone
    local_time_start = dt.datetime.fromtimestamp ( unix_timestamp_start , local_timezone )
    local_time_end = dt.datetime.fromtimestamp ( unix_timestamp_end , local_timezone )
    print ( 'local_time_start=' , local_time_start )
    print ( 'local_time_end=' , local_time_end )
    pass
drop_duplicates_in_db()