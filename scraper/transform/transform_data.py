import re
import logging
import polars as pl
from bs4 import BeautifulSoup


class KoroshiDataTransformator() :

  def __init__(self,
               file_log: str) -> None :
  
    # Set the log management
    self.logger = logging.getLogger(self.__class__.__name__)
    self.logger.setLevel(logging.INFO)
    handler = logging.FileHandler(filename=file_log, mode='w')
    handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s.%(funcName)s()] %(message)s'))
    self.logger.addHandler(handler)
    

  def remove_tags(self,
                  data: pl.DataFrame,
                  columns: list[str]) -> pl.DataFrame :
    
    for col in columns :
      data = data.with_columns(
        pl.col(col)
          .map_elements(function=lambda text: BeautifulSoup(text, "html.parser").get_text(" ", strip=True),
                        return_dtype=pl.String)
          .alias(col)
      )
      self.logger.info(f"Remove tags on column : '{col}'")

    return data
  

  def stringify_values(self,
                       data: pl.DataFrame,
                       columns: list[str]) -> pl.DataFrame :
    
    for col in columns :
      data = data.with_columns(
        pl.col(col)
          .cast(pl.String)
          .alias(col)
      )
      self.logger.info(f"Stringify column : '{col}'")

    return data
  

  def to_uppercase(self,
                   data: pl.DataFrame,
                   columns: list[str]) -> pl.DataFrame :
    
    for col in columns :
      data = data.with_columns(
        pl.col(col).str.to_uppercase().alias(col)
      )
      self.logger.info(f"Uppercase column : '{col}'")

    return data


  def to_lowercase(self,
                   data: pl.DataFrame,
                   columns: list[str]) -> pl.DataFrame :
    
    for col in columns :
      data = data.with_columns(
        pl.col(col).str.to_lowercase().alias(col)
      )
      self.logger.info(f"Lowercase column : '{col}'")

    return data
  

  def remove_white_space(self,
                         data: pl.DataFrame,
                         columns: list[str]) -> pl.DataFrame :
    
    for col in columns :
      data = data.with_columns(
        pl.col(col).str.strip_chars().alias(col)
      )
      self.logger.info(f"Remove white space on column : '{col}'")

    return data


  def check_url(self,
                data: pl.DataFrame,
                column: str,
                pattern: str) -> pl.DataFrame :
      

    output_column = 'is_valid_' + column
    data = data.with_columns(        
      pl.col(column)
        .map_elements(function=lambda url: bool(re.match(pattern=pattern, string=url)),
                      return_dtype=pl.Boolean
                      )
        .alias(output_column)
    )
    self.logger.info(f"Check url on the column '{column}', see the column '{output_column}' with value False to look up unmatched data")
    
    return data


  def convert_to_price(self,
                       data: pl.DataFrame,
                       column: str,
                       currency: str='EUR',
                       currency_col_name: str='currency') -> pl.DataFrame :
      
    # Convert price and add currency
    data = data.with_columns(
        pl.col(column).cast(pl.Float64) /  100,
        pl.lit(currency, dtype=pl.String).alias(currency_col_name)
    )
    self.logger.info(f"Convert in price format '{column}' and associate to the currency (have a look on column '{currency_col_name}')")

    return data
