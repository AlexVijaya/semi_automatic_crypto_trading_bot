from extract_tickers_from_gerchik import extract_tickers_from_gerchik

gerchik_tickers_list=extract_tickers_from_gerchik()
def input_gerchik_tickers_into_txt(gerchik_tickers_list):
    print(gerchik_tickers_list)
    with open('list_of_gerchik_tickers.txt','w') as gerchik_file:
        for ticker in gerchik_tickers_list:
            gerchik_file.write(ticker+"\n")
input_gerchik_tickers_into_txt(gerchik_tickers_list)