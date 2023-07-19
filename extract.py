
import os
import requests
import pandas as pd
import config

# URL="https://archives.nseindia.com/content/indices/ind_nifty50list.csv"

# data = requests.get(URL)
# with open('nifty50.csv', 'wb')as file:
#  file.write(data.content)

# stock_df= pd.read_csv('nifty50.csv')
# stock_list=stock_df['Symbol'].tolist()

def download_data():
   APi_Key=config.APi_Key

   stock_list=["AXP", "AMGN", 'AAPL', 'INTC', 'JPM']

   for stock in stock_list:


      url = 'https://www.alphavantage.co/query' 
      data_params= {
      'function':'TIME_SERIES_MONTHLY',
      'symbol': stock,
         'apikey':APi_Key,
         'outputsize':'full',
         'datatype':'csv'
      }


      response = requests.get(url, params=data_params)
      with open(f'Data/Raw_Data/{stock}_data.csv', 'wb')as file2:
         file2.write(response.content)