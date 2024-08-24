#!usr/bin/env python3

from pandas import read_pickle as rp
import pandas as pd
from yfinance import Ticker
import numpy as np
from csv import reader
from os import path
from datetime import date, datetime
from math import ceil
from pykalman import KalmanFilter

import warnings
warnings.filterwarnings('ignore')


###########################################
# functions for use to get ETL/ELT tables #
###########################################

# read curated stock symbols from static file
def read_symbols_csv():

    file_path = path.join("static", "symbols.csv")
   
    result = []
    
    # Check if the file exists
    if not path.exists(file_path):
        print(f"The file {file_path} does not exist.")
        return result
    
    with open(file_path, 'r') as file:
        csv_reader = reader(file)
        
        for row in csv_reader:
            # Assuming the CSV file has two columns: symbol and date
            if len(row) == 2:
                symbol, date = row
                result.append((symbol, date))
            else:
                print(f"Skipping invalid row: {row}")
    
    return result

# returns now timestamp
def current_time():
    
    today = date.today()
    now = datetime.now().strftime("%H:%M:%S")
    
    return f'{today} {now}'
    

def fetch_data_starter_data(stock_list = read_symbols_csv()):
    """
    Gets max data history available
    """
    
    start_time = current_time()
    
    ctn = 0
    
    for item in stock_list:
        
        stock = Ticker(item[0])
        
        #get max 1 day data
        stock_1d_df = stock.history(start = item[1],  # may not be necessary as period='max'
                                    end = None,
                                    interval = '1d',  # time spacing interval
                                    period='max',  # historical period, can adjust start and end
                                    auto_adjust=False, # new as of 1/23/24
                                    prepost=True, # include pre market and post market data
                                   )
        stock_1d_df.to_pickle(f'./data/{item[0]}_1d_df.pkl')
        
        #get max 1 hour data
        stock_1h_df = stock.history(interval = '1h',  # time spacing interval
                                    period='2y',  # historical period, can use start and end 730d 
                                    auto_adjust=False, # new as of 1/23/24
                                    prepost=True, # include pre market and post market data
                                   )
        stock_1h_df.to_pickle(f'./data/{item[0]}_1h_df.pkl')
        
        
        #get max 30 minutes data
        stock_30m_df = stock.history(interval = '30m',  # time spacing interval
                                    period='1mo',  # historical period, can use start and end
                                    auto_adjust=False, # new as of 1/23/24
                                    prepost=True, # include pre market and post market data
                                   )
        stock_30m_df.to_pickle(f'./data/{item[0]}_30m_df.pkl')
        
        
        #get max 15 minutes data
        stock_15m_df = stock.history(interval = '15m',  # time spacing interval
                                    period='1mo',  # historical period, can use start and end 60d
                                    auto_adjust=False, # new as of 1/23/24
                                    prepost=True, # include pre market and post market data
                                   )
        stock_15m_df.to_pickle(f'./data/{item[0]}_15m_df.pkl')
        
        ctn += 1
    
    end_time = current_time()
        
    return print(f'Start time: {start_time}\nDownloaded {ctn} max daily, hourly, 30m, and 15m stock data\nEnd Time: {end_time}')

  if __name__ == "__main__":
    ...