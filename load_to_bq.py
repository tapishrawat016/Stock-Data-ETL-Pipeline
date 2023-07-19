from pathlib import Path
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import os

@task(log_prints=True, retries=3)
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Writing to BQ"""
    gcp_credentials_block = GcpCredentials.load("gcp-credentials")
    df.to_gbq(
        destination_table="stock_data_bq.stock_data_table",
        project_id="stock-data-392403",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append")


@flow 
def etl_gcs_to_bq():
    """Main ETL to load data to BQ"""
    folder=f'Data/Transformed_Data'

    for filename in os.listdir(folder):
            folder_path = os.path.join(folder, filename)
            for filename in os.listdir(folder_path):
                    file_path = os.path.join(folder_path, filename)
                    df = transform(file_path)
                    write_bq(df)

if __name__=="__main__":
    etl_gcs_to_bq()