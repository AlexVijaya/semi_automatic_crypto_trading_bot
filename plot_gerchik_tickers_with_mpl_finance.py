import pandas as pd
import mplfinance as mpf
import os
import sqlite3
from pathlib import Path
def plot_assets_with_mplfinance_with_yahoofinance():
    '''plot assets using mplfinance'''

    #symbols.csv contains a list of all tickers which we downloaded from yahoofince
    path_to_ticker_list = os.path.join(os.getcwd(), "list_of_gerchik_tickers.txt")
    print(path_to_ticker_list)
    print('''os.path.join(os.getcwd(),'datasets","plots")''', os.path.join(os.getcwd(),"datasets","plots"))
    print('''os.mkdir(os.path.join(os.getcwd(), "datasets", "daily"))''',os.path.join(os.getcwd(), "datasets", "daily"))
    #create an empty folder /datasets/daily if it does not exist
    path=os.path.join(os.getcwd(), "datasets", "daily")
    Path(path).mkdir(parents=True,exist_ok = True)

    conn=sqlite3.connect( os.path.join( os.getcwd() , 'datasets','sql_databases','yf_db_historical_data_new.db' ) )
    cur = conn.cursor ()
    conn.row_factory = sqlite3.Row


    #open file symbols.csv with tickers and make a list called symbols
    with open(path_to_ticker_list) as f:
        symbols = f.read().splitlines()
        print("*****************\n",type(symbols))
        #print("symbols", symbols)

        #create a folder with mplfinance plots if it does not exist
        path2=os.path.join(os.getcwd(),"datasets","plots")
        Path(path2).mkdir(parents=True,exist_ok = True)


        # go over each file in /datasets/daily folder and make dataframe out of each csv file
        # in each iteration

        symbol="TSLA"

        try:
            asset_df = pd.read_sql(f'''select * from yf_gerchik_tickers_and_prices where symbol="{symbol}" ;''' ,
                             conn , parse_dates = 'Date', index_col = "Date" )
            print(asset_df)
            mpf.plot(asset_df.tail(180),type='candle',style="charles", volume=True,
                     title='{}'.format(symbol),
                     savefig=os.path.join(os.getcwd(),"datasets","plots",'{}.png'.format(symbol)))
            print("I am plotting {}".format(symbol))
        except Exception:
            print ("problem with {} dataframe".format(symbol))

plot_assets_with_mplfinance_with_yahoofinance()