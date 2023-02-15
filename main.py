
from kiteconnect import KiteConnect, KiteTicker
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import os
from pyotp import TOTP
import json
import pandas as pd
import logging
import db

cwd = os.chdir('/home/suhas/Downloads/kite/algo_app/algo_app_v1')

with open('config.json', 'r') as config_file:
    config_data = json.load(config_file)

request_token = None

logging.basicConfig(level=logging.INFO)

kite = KiteConnect(api_key=config_data['keys']['api_key'])

kt = None
tokens = None
tickers = ["INFY", "ACC", "RELIANCE"]
st_list = []


def login():
    
    logging.info('Login process started...')
    
    global request_token
    
    service = webdriver.chrome.service.Service('./chromedriver')
    service.start()
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options = options.to_capabilities()
    driver = webdriver.Remote(service.service_url, options)
    driver.get(kite.login_url())
    driver.implicitly_wait(10)
    usernameEl = driver.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[1]/input')
    passwordEl = driver.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/input')
    usernameEl.send_keys(config_data['keys']['username'])
    passwordEl.send_keys(config_data['keys']['password'])
    driver.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[4]/button').click()
    logging.info('TOTP process started.')
    time.sleep(5)
    pin = driver.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/input')
    
    totp = TOTP(config_data['keys']['totp_key'])
    token = totp.now()
    # print(token)
    pin.send_keys(token)
    
    driver.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[3]/button').click()
    logging.info('Successfully logged in.')
    time.sleep(4)
    # print(driver.current_url)
    logging.info('getting request token...')
    request_token=driver.current_url.split('request_token=')[1][:32]
    # print(request_token)
    config_data['keys']['request_token'] = request_token
    with open('config.json', 'w') as cf:
        json.dump(config_data, cf)
    driver.quit()
    logging.info('generating access token')
    data = kite.generate_session(request_token, api_secret=config_data['keys']['api_secret'])
    # print(data["access_token"])
    kite.set_access_token(data["access_token"])
    config_data['keys']['access_token'] = data["access_token"]
    with open('config.json', 'w') as cf:
        json.dump(config_data, cf)
        
    db.get_db()
    db.create_table()
        
def getHoldings():
    holdings = kite.holdings()
    h_df = pd.DataFrame(holdings)
    h_df.to_csv('holdings.csv', index=False)
    
def getLtp():
    ltp = kite.ltp(config_data['instruments'])
    print(ltp)
    
def getOHLC():
    ohlc = kite.ohlc(config_data['instruments'])
    # print(ohlc)
    for instru in config_data['instruments']:
        data = { 
                "instrument": instru.split(':')[1],
                "exchange": instru.split(':')[0],
                "instrument_token": ohlc[instru]['instrument_token'],
                "last_price": ohlc[instru]['last_price'],
                "open": ohlc[instru]['ohlc']['open'],
                "high": ohlc[instru]['ohlc']['high'],
                "low": ohlc[instru]['ohlc']['low'],
                "close": ohlc[instru]['ohlc']['close']
                }
        print(data)
        
    
def tokenLookup(instrument_df,symbol_list):
    token_list = []
    global st_list
    for symbol in symbol_list:
        t = int(instrument_df[instrument_df.tradingsymbol==symbol].instrument_token.values[0])
        token_list.append(t)
        st_list.append({'symbol': symbol, 'token': t})
    return token_list
    
def startTick():
    global kt, tokens
    instrument_dump = kite.instruments("NSE")
    instrument_df = pd.DataFrame(instrument_dump)
    kt = KiteTicker(api_key=config_data['keys']['api_key'], access_token=config_data['keys']['access_token'])
    tokens = tokenLookup(instrument_df,tickers)
    db.create_tables(tokens)
    
    kt.on_ticks=on_ticks
    kt.on_connect=on_connect
    kt.connect()
    
def on_ticks(ws,ticks):
    # print(ticks)
    db.insert_ticks(ticks)

def on_connect(ws,response):
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_FULL,tokens)
    logging.info('Ticker started.')
    
def on_close(ws, code, reason):
    ws.stop()

  
login()
startTick()
