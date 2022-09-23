import sqlite3
import pandas as pd
import os
import tzlocal
import datetime as dt
import time
import re

from sqlalchemy import create_engine
from sqlalchemy_utils import create_database,database_exists
import db_config
# from sqlalchemy import MetaData
from sqlalchemy import inspect
import logging
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base



def connect_to_postres_db(database = "btc_and_usdt_pairs_from_all_exchanges"):
    dialect = db_config.dialect
    driver = db_config.driver
    password = db_config.password
    user = db_config.user
    host = db_config.host
    port = db_config.port

    dummy_database = db_config.dummy_database

    engine = create_engine ( f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}" ,
                             isolation_level = 'AUTOCOMMIT' , echo = True )
    print ( f"{engine} created successfully" )

    # Create database if it does not exist.
    if not database_exists ( engine.url ):
        create_database ( engine.url )
        print ( f'new database created for {engine}' )
        connection=engine.connect ()
        print ( f'Connection to {engine} established after creating new database' )

    connection = engine.connect ()

    print ( f'Connection to {engine} established. Database already existed.'
            f' So no new db was created' )
    return engine , connection

def drop_table(table_name,engine):
    engine.execute (
        f"DROP TABLE IF EXISTS {table_name};" )

    # base = declarative_base()
    # metadata = MetaData(engine)
    #
    # table = metadata.tables.get(table_name)
    # if table is not None:
    #    logging.info(f'Deleting {table_name} table')
    #    base.metadata.drop_all(engine, [table], checkfirst=True)



def drop_duplicates_in_db():
    start_time = time.time ()

    engine_for_usdt_trading_pairs_ohlcv_db , connection_to_btc_and_usdt_trading_pairs = \
        connect_to_postres_db ( "btc_and_usdt_pairs_from_all_exchanges" )


    mirror_levels_df=pd.read_sql_query("select * from rounded_mirror_levels_calculated_separately",
                                       connection_to_btc_and_usdt_trading_pairs)
    print("number of trading pairs with duplicates=",
          len(mirror_levels_df))

    mirror_levels_df.drop_duplicates(subset = ["USDT_pair","mirror_level"],
                                     keep = "first",
                                     inplace = True)
    # create additional boolian coulumn
    # which says if there are multiple mirror levels in this pair
    mirror_levels_df["pair_with_multiple_mirror_levels_on_one_exchange"]=\
        mirror_levels_df.duplicated(subset=["USDT_pair","exchange"],keep=False)

    mirror_levels_df.reset_index(inplace = True,drop = True)
    print("len(mirror_levels_df.index)=",len(mirror_levels_df.index))

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

    mirror_levels_df.to_sql("rounded_mirror_levels_without_duplicates",
                            connection_to_btc_and_usdt_trading_pairs,
                            if_exists ='replace')
    print ( "number of trading pairs without duplicates=" ,
            len ( mirror_levels_df ) )
    connection_to_btc_and_usdt_trading_pairs.close()
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
if __name__=="__main__":
    drop_duplicates_in_db()