#!/usr/bin/env python
# coding: utf-8

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit


import pandas as pd
import pyarrow
import os
import pyspark

from pyspark.sql import types
import os
from glob import glob



  
# use glob to get all the csv files 
# in the folder

def transform_data():
    path = os.getcwd()
    directory = f'{path}/Data/Raw_Data'
 
# iterate over files in
# that directory
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f):
            file_name = os.path.basename(f).split('.')[0]
            df = pd.read_csv(f)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.to_parquet(f'Data/Parqueted_Data/{file_name}.parquet')
            




    spark = SparkSession.builder \
        .appName('test') \
        .getOrCreate() 

    path = os.getcwd()
    directory = f'{path}/Data/Parqueted_Data'
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f):
            file_name = os.path.basename(f).split('.')[0]
            df_schema=types.StructType([types.StructField('timestamp',
                        types.TimestampType(),
                        True),
                        types.StructField('open',
                        types.DoubleType(),
                        True),
                        types.StructField('high',
                        types.DoubleType(),
                        True),
                        types.StructField('low',
                        types.DoubleType(),
                        True),
                        types.StructField('close',
                        types.DoubleType(),
                        True),
                        types.StructField('volume',
                        types.LongType(),
                        True)])
            df = spark.read \
                .option("header", "true") \
                .schema(df_schema) \
                .parquet(f'Data/Parqueted_Data/{file_name}.parquet')
            
            df.registerTempTable('stock_data')
            df_stock_data = spark.sql("""
                            SELECT 
                                timestamp AS Date, 
                                ROUND(open, 2) AS Opened_At,
                                ROUND(high, 2) AS Days_High,
                                ROUND(low, 2) AS Days_low,
                                ROUND(close, 2) AS Closed_At
                            FROM
                                stock_data

                            """)
            df_stock_data.withColumn("Stock", lit(file_name.split('_')[0])).coalesce(1).write.mode('overwrite').parquet(f'Data/Transformed_Data/{file_name}')
            folder=f'Data/Transformed_Data/{file_name}'
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                if '/.' in glob(file_path)[0] or '/_' in glob(file_path)[0] :
                    os.remove(file_path)
                else:
                    os.rename(file_path,f'Data/Transformed_Data/{file_name}/{file_name}.parquet')
       
transform_data()
