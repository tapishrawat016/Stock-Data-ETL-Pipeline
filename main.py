import os
import requests
import pandas as pd



from extract import download_data
from transform import transform_data
from load import load, write_gcs
from load_to_bq import transform, etl_gcs_to_bq, write_bq


def main():
    download_data()
    transform_data()
    load()
    etl_gcs_to_bq()
    
if __name__=="__main__":
    main()



