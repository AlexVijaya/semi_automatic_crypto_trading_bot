import pprint
import time
from selenium import webdriver
import requests
import lxml
import lxml.etree
import pandas as pd
from lxml import etree
import xmltodict
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

#gerchik_tickers_list=[]
def extract_tickers_from_gerchik():
    options=Options()
    options.headless=True
    url='http://gerchik.co/traders/speczifikacziya-kontraktov'
    driver = webdriver.Chrome ( './chromedriver_94' ,options =options)
    driver.get(url)
    tickers_list=[]
    #all_tickers=driver.find_elements_by_xpath("//table/tbody/tr/td[1]")
    all_tickers2=driver.find_elements(By.XPATH,"//table/tbody/tr/td[1]")
    for ticker in all_tickers2:
        original_ticker=ticker.get_property('innerHTML')
        truncated_ticker=original_ticker.replace('.us','')
        #print(ticker.get_property('innerHTML'))
        tickers_list.append(truncated_ticker)
    gerchik_tickers_list=tickers_list
    #print ( tickers_list)

    driver.close()
    return gerchik_tickers_list





