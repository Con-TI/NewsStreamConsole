from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from io import StringIO
from selenium.webdriver.common.keys import Keys
import sqlite3
import yfinance as yf

def find_stock_codes(driver:webdriver,wait:WebDriverWait):
    idx = "https://www.idx.co.id/en/market-data/stocks-data/stock-list"
    driver.get(idx)
    select_element = wait.until(EC.presence_of_element_located((By.TAG_NAME,"select")))
    select = Select(select_element)
    select.select_by_visible_text("All")
    table_element = wait.until(EC.presence_of_element_located((By.TAG_NAME,"table")))
    table_html = table_element.get_attribute('outerHTML')
    df = pd.read_html(StringIO(table_html))[0]
    stock_codes = df.iloc[:,1].to_list()
    return stock_codes

def find_news(driver:webdriver,wait:WebDriverWait, term):
    path = "https://www.google.com"
    driver.get(path)
    search_bar_element = wait.until(EC.presence_of_element_located((By.NAME,"q")))
    search_bar_element.send_keys(term)
    search_bar_element.send_keys(Keys.ENTER)
    news_tab = wait.until(EC.presence_of_element_located((By.XPATH,'//*[contains(text(),"News")]')))
    news_tab.click()
    tools = wait.until(EC.presence_of_element_located((By.XPATH,'//*[contains(text(),"Tools")]')))
    tools.click()
    sort = wait.until(EC.presence_of_element_located((By.XPATH,'//*[contains(text(),"Sorted by relevance")]')))
    sort.click()
    sort_by_date = wait.until(EC.presence_of_element_located((By.XPATH,'//*[contains(text(),"Sorted by date")]')))
    sort_by_date.click()
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR,'div.SoaBEf')))
    headings = driver.find_elements(By.CSS_SELECTOR,'div.SoaBEf div div a div div.SoAPf div[role = "heading"]')
    times = driver.find_elements(By.CSS_SELECTOR,'div.SoaBEf div div a div div.SoAPf div[style = "bottom:0px"] span')
    links = driver.find_elements(By.CSS_SELECTOR,'div.SoaBEf div div a')
    return headings, times, links

def main():
    conn = sqlite3.connect('news.db')
    cursor = conn.cursor()
    driver = webdriver.Chrome()
    wait = WebDriverWait(driver,5)
    stock_codes = find_stock_codes(driver,wait)
    stock_codes = [f"{code}.JK" for code in stock_codes]
    price_data = yf.download(tickers=stock_codes, period="3mo")
    turnover = (((price_data['Close']+price_data['Open'])/2)*price_data['Volume']).mean(axis=0)
    stock_codes = [code[:4] for code in list(turnover[turnover>10*(10**9)].index)]
    search_terms = [f"{code} saham kontan" for code in stock_codes]
    other_search_terms = ["IDX kontan","BI kontan","OJK kontan","PPATK kontan","KPK kontan","DPR kontan","DEPKEU kontan","DJP kontan","POLRI kontan"]
    topic = ["IDX", "BI", "OJK", "PPATK", "KPK", "DPR", "DEPKEU", "DJP", "POLRI"]

    for term in search_terms:
        cursor.execute(f'''SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{term[:4]}';''')
        if cursor.fetchone()[0] == 1:
            cursor.execute(f'''DROP TABLE {term[:4]};''')
        cursor.execute(f'''CREATE TABLE {term[:4]} (
                        id INTEGER PRIMARY KEY,
                        Headings TEXT,
                        Times TEXT,
                        Links TEXT
                    );''')
        
        headings,times,links = find_news(driver,wait,term)

        insert_statement = f'''INSERT INTO {term[:4]} (Headings, Times, Links) VALUES (?, ?, ?);'''
        values = [(headings[i].text,times[i].text,links[i].get_attribute('href')) for i in range(len(headings))]
        cursor.executemany(insert_statement,values)
        conn.commit()

    for idx,term in enumerate(other_search_terms):
        cursor.execute(f'''SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{topic[idx]}';''')
        if cursor.fetchone()[0] == 1:
            cursor.execute(f'''DROP TABLE {topic[idx]};''')
        cursor.execute(f'''CREATE TABLE {topic[idx]} (
                        id INTEGER PRIMARY KEY,
                        Headings TEXT,
                        Times TEXT,
                        Links TEXT
                    );''')
        
        headings,times,links = find_news(driver,wait,term)

        insert_statement = f'''INSERT INTO {topic[idx]} (Headings, Times, Links) VALUES (?, ?, ?);'''
        values = [(headings[i].text,times[i].text,links[i].get_attribute('href')) for i in range(len(headings))]
        cursor.executemany(insert_statement,values)
        conn.commit()
    conn.close()

def main2():
    conn = sqlite3.connect('news.db')
    cursor = conn.cursor()
    query = '''SELECT name FROM sqlite_master WHERE type='table';'''
    cursor.execute(query)
    table_names = cursor.fetchall()
    dataframes = []
    for name in table_names:
        query = f"SELECT * FROM {name[0]}"
        df = pd.read_sql_query(query,conn)
        df = df.iloc[:,1:]
        dataframes.append(df)
    df = pd.concat(dataframes,axis=1)
    column_names = []
    for name in table_names:
        column_names.append((name[0],"Headings"))
        column_names.append((name[0],"Times"))
        column_names.append((name[0],"Links"))
    df.columns = pd.MultiIndex.from_tuples(column_names)
    return df