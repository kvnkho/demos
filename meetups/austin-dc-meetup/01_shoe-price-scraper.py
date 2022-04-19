import requests
import re
from bs4 import BeautifulSoup
import time

def find_nike_price(url):
    k = requests.get(url).text
    soup = BeautifulSoup(k,'html.parser')
    price_string = soup.find('div', {"class":"product-price"}).text
    price_string = price_string.replace(' ','')
    price = int(re.search('[0-9]+',price_string).group(0))
    return price

def compare_price(price, budget):
    if price <= budget:
       print(f"Buy the shoes! Good deal!")
    else:
        print(f"Don't buy the shoes. They're too expensive")

def nike_flow(url, budget):
    price = find_nike_price(url)
    compare_price(price, budget)

url = "https://www.nike.com/t/air-max-270-womens-shoes-Pgb94t/AH6789-601"
budget = 120
nike_flow(url, budget)