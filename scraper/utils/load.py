import os
import polars as pl
from datetime import datetime
from dotenv import load_dotenv

from load.load_data import KoroshiDataBaseLoader, KoroshiDataCloudLoader



# ============================================================================= #
# ============================== LOAD INTO DATABASE =========================== #
# ============================================================================= #

def load_data_to_db(data: pl.DataFrame,
                    log_file: str) -> None :
    """
    Job description
        
        Args
            data_fp : [string] : file path that contains data to insert to the database
            log_file : [string] : file path where log will be write
    """

    # Environment variables
    load_dotenv()
    DB_USER = os.getenv("DB_USER", "")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    HOST = os.getenv("HOST", "")
    PORT = os.getenv("PORT", '')
    DBNAME = os.getenv("DBNAME", "")
    SCHEMA = os.getenv("SCHEMA", None)
    TABLE_NAME = os.getenv("TABLE_NAME", "")
    
    # Connection to the PostgreSQL database
    dataloader = KoroshiDataBaseLoader(connection_url=f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{HOST}:{PORT}/{DBNAME}",
                                   schema=SCHEMA,
                                   table=f"{TABLE_NAME}_{datetime.now().date().__str__().replace('-', '_')}",
                                   file_log=log_file)
    # Insert data into the database
    if data is not None:
        dataloader.insert_data(data)


def load_data_to_bigquery(service_account_fp: str,
                          project: str,
                          dataset: str,
                          table: str,
                          data_fp: str,
                          log_file: str,
                          batch_size:int=100) :
    
    dataloader = KoroshiDataCloudLoader(service_account_fp=service_account_fp,
                                       project=project,
                                       dataset=dataset,
                                       table=table,
                                       file_log=log_file)
    # batch load data from csv file
    raw_data_batched = pl.read_csv_batched(data_fp, has_header=True, batch_size=1000)
    
    while True :
        small_batches = raw_data_batched.next_batches(100)
        
        if not small_batches : break
        
        data_batched = []
        
        # Insert data into the database
        for single_batch in small_batches :
            data = single_batch.select([pl.col(column) for column in data.columns if column != ''])
            data_batched = pl.concat(data)
            dataloader.load_data(data_batched)
        
        
        