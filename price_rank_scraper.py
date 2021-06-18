import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

def get_price_ranks():
    current_dt = datetime.datetime.now().strftime("%Y-%m-%d %X")
    current_dts = [current_dt for _ in range(200)]
    stock_types = ["tse", "otc"]
    price_rank_urls = ["https://tw.stock.yahoo.com/d/i/rank.php?t=pri&e={}&n=100".format(st) for st in stock_types]
    tickers = []
    stocks = []
    prices = []
    volumes = []
    mkt_values = []
    ttl_steps = 10*100
    each_step = 10
    for pr_url in price_rank_urls:
        r = requests.get(pr_url)
        soup = BeautifulSoup(r.text, 'html.parser')
        ticker = [i.text.split()[0] for i in soup.select(".name a")]
        tickers += ticker
        stock = [i.text.split()[1] for i in soup.select(".name a")]
        stocks += stock
        price = [float(soup.find_all("td")[2].find_all("td")[i].text) for i in range(5, 5+ttl_steps, each_step)]
        prices += price
        volume = [int(soup.find_all("td")[2].find_all("td")[i].text.replace(",", "")) for i in range(11, 11+ttl_steps, each_step)]
        volumes += volume
        mkt_value = [float(soup.find_all("td")[2].find_all("td")[i].text)*100000000 for i in range(12, 12+ttl_steps, each_step)]
        mkt_values += mkt_value
    types = ["上市" for _ in range(100)] + ["上櫃" for _ in range(100)]
    ky_registered = [True if "KY" in st else False for st in stocks]
    df = pd.DataFrame()
    df["scrapingTime"] = current_dts
    df["type"] = types
    df["kyRegistered"] = ky_registered
    df["ticker"] = tickers
    df["stock"] = stocks
    df["price"] = prices
    df["volume"] = volumes
    df["mktValue"] = mkt_values
    return df

price_ranks = get_price_ranks()
conn = sqlite3.connect('/home/ubuntu/yahoo_stock.db')
price_ranks.to_sql("price_ranks", conn, if_exists="append", index=False)