import logging
from typing import Any, Union
import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError



# ==================================================================================================================================================================== #
# ======================================================================= KoroshiDataLoader ======================================================================= #
# ==================================================================================================================================================================== #
class KoroshiDataLoader() :


    def __init__(self,
                 connection_url: str,
                 table: str,
                 file_log: str,
                 schema: Union[str, None]=None) -> None :
        """
        Provide some feature to connect and insert data into a database (in this case, a PostgreSQL)
            
            Args
                connection_url : [string] : the url that provides information about how to connect to the database
                table : [string] : the table name where insert data into the database
                file_log : [string] : file path where write the log
                schema : [string or None] : the schema which contains the table
                        if None, that means the schema is public or default schema
                        :default:None

            Raises
                [SQLAlchemyError] : when an error occurred during connecting to the database
        """

        # Set the log management
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler(filename=file_log, mode='w')
        handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s.%(funcName)s()] %(message)s'))
        self.logger.addHandler(handler)

        # Table and schema of the database
        self.table = table
        self.schema = schema

        try :
            # Initialize the database connection
            self.db_engine = create_engine(connection_url)
            with self.db_engine.connect() as connection :
                connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema};"))
                connection.execute(text(f"DROP TABLE IF EXISTS {self.schema}.{self.table};"))
                connection.execute(text(f"CREATE TABLE IF NOT EXISTS {self.schema}.{self.table}();"))
                self.logger.info("Database initialized")
            self.logger.info("DataLoader initialized")

        except SQLAlchemyError as e :
            self.logger.error(f"An error occurred when trying to connet to database : {e}")

    
    def insert_data(self,
                    dataframe: pl.DataFrame) -> None :
        """
        Insert data into the database
            
            Args
                dataframe : [pl.DataFrame] : the dataframe that contains data
        """

        # Converte to a pandas dataframe to have the `to_sql()` class method
        df = dataframe.to_pandas()
        
        if self.db_engine is not None :
            # insertion
            df.to_sql(name=self.table,
                      schema=self.schema, 
                      con=self.db_engine,
                      if_exists="replace",
                      index=False)
                
            self.logger.info(f"Data injected into the table '{self.schema}.{self.table}'")


    