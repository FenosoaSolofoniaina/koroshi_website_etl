import os
import polars as pl
from datetime import datetime
from dotenv import load_dotenv

from load.load_data import KoroshiDataLoader



# ============================================================================= #
# ============================== LOAD INTO DATABASE =========================== #
# ============================================================================= #

def load_data_to_db(data_fp: str,
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
    dataloader = KoroshiDataLoader(connection_url=f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{HOST}:{PORT}/{DBNAME}",
                                   schema=SCHEMA,
                                   table=f"{TABLE_NAME}_{datetime.now().date().__str__().replace('-', '_')}",
                                   file_log=log_file)
    # Extract data from json file
    df = dataloader.convert_json_to_dataframe(fp=data_fp)

    # Insert data into the database
    if df is not None:
        #df.write_csv('koroshi_data.csv')
        #logging.info(f"Data saved into csv file")

        dataloader.insert_data(df)
