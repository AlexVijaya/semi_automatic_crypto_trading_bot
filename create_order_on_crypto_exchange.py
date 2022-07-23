import ccxt  # noqa: E402
from api_config import api_dict_for_all_exchanges


def create_order():
    global symbol , exchange
    print ( 'CCXT version:' , ccxt.__version__ )
    exchange_id = "kucoin"
    exchange_class = getattr ( ccxt , exchange_id ) ()
    # exchange_class.load_markets()
    # list_of_all_symbols=exchange_class.symbols
    symbol = 'NDAU/USDT'
    # if symbol in list_of_all_symbols:
    #     print("true")
    # else:
    #     print ( "false" )
    # print(exchange_class.symbols)
    public_api_key = api_dict_for_all_exchanges[exchange_id][0]
    api_secret = api_dict_for_all_exchanges[exchange_id][1]
    password = api_dict_for_all_exchanges[exchange_id][2]
    exchange = ccxt.kucoin ( {
        'apiKey': public_api_key ,
        'secret': api_secret ,
        'timeout': 30000 ,
        'enableRateLimit': True ,
        'password': password
    } )
    # exchange.set_sandbox_mode(True)
    type = 'Limit'  # or 'market', other types aren't unified yet
    side = 'buy'
    amount = 69  # your amount
    price = 0.21  # your price
    # overrides
    # params = {
    #     'stopPrice': 0.20,  # your stop price
    #     'type': 'stopLimit',
    # }
    order = exchange.create_order ( symbol , type , side , amount , price )


create_order ()