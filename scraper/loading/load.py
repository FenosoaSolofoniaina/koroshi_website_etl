import logging
from typing import Any, Union
import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError



class KoroshiDataLoader() :


    def __init__(self,
                 connection_url: str,
                 table:str,
                 file_log: str,
                 schema: Union[str, None]=None) -> None :

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler(filename=file_log, mode='w')
        handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s.%(funcName)s()] %(message)s'))
        self.logger.addHandler(handler)

        self.table = table
        self.schema = schema

        try :
            self.db_engine = create_engine(connection_url)
            self.init_db()
            self.logger.info("DataLoader initialized")

        except SQLAlchemyError as e :
            self.logger.error(f"An error occurred when trying to connet to database : {e}")


    def from_json(self, fp: Any) -> Union[pl.DataFrame, None] :

        result = None

        try :
            result = pl.read_json(fp)
            self.logger.info(f"Reading the json file '{fp}' successfully")
                
        except FileNotFoundError :
            self.logger.error(f"Cannot find the json file in location : '{fp}'")

        except PermissionError :
            self.logger.error(f"Cannot access to the json file in location : '{fp}'")

        except Exception as error :
            self.logger.error(f"An error occured during reading the json file '{fp}' : {error}")
            
        finally :
            return result
        

    def init_db(self) -> None :

        with self.db_engine.connect() as connection :
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema};"))
            connection.execute(text(f"DROP TABLE IF EXISTS {self.schema}.{self.table};"))
            connection.execute(text(f"CREATE TABLE IF NOT EXISTS {self.schema}.{self.table}();"))
            self.logger.info("Database initialized")


    def insert_data(self, dataframe: pl.DataFrame) -> None :

        df = dataframe.to_pandas()
        
        if self.db_engine is not None :
            df.to_sql(name=self.table,
                        schema=self.schema, 
                        con=self.db_engine,
                        if_exists="replace",
                        index=False)
                
            self.logger.info(f"Data injected into the table '{self.schema}.{self.table}'")