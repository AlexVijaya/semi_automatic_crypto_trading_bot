import alpaca_trade_api as tradeapi
import alpaca_config
def get_asset_list():
    # claim your api here: https://app.alpaca.markets/paper/dashboard/overview
    api = tradeapi.REST ( alpaca_config.api_key , alpaca_config.secret_key,
                          alpaca_config.api_url)
    assets = api.list_assets ()


        #print ( asset )
    return assets
#get_asset_list()