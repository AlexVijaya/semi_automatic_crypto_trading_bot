import traceback

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def extract_list_of_stablecoins_from_coingecko():
    options=Options()
    options.headless=True

    driver = webdriver.Chrome ( service=
                                Service(ChromeDriverManager().install()) ,options =options)

    stablecoins_list=[]


    try:
        url = f'https://www.coingecko.com/en/categories/stablecoins?'


        print ( url )
        driver.get ( url )
        # driver.execute_script ( "window.scrollTo(0,document.body.scrollHeight)" )

        stablecoin_elements=\
                    driver.find_elements(By.XPATH,'//a[@class="tw-flex tw-items-start md:tw-flex-row tw-flex-col"]/span[2]')
        print ( "len ( stablecoin_elements )" )

        stablecoins_list=[]
        for stable_coin_element in stablecoin_elements:
            stablecoin_text = stable_coin_element.get_property ( 'innerHTML' )
            print(stablecoin_text)
            stablecoin_text=stablecoin_text.replace("\n","")
            stablecoins_list.append ( stablecoin_text )
            # print(stablecoin_text)
        print(len(stablecoin_elements))
        print ( len ( stablecoins_list ) )
        print ( "stablecoins_list" )
        print ( stablecoins_list  )


    except:
        traceback.print_exc()

    driver.close()
    return stablecoins_list

if __name__=='__main__':
    extract_list_of_stablecoins_from_coingecko()
