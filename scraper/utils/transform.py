import polars as pl
from transform.transform_data import KoroshiDataTransformator


def preprocessing(data: pl.DataFrame,
                  log_file: str) -> pl.DataFrame:

    data_transformer = KoroshiDataTransformator(file_log=log_file)

    data = data_transformer.remove_tags(data=data,
                                        columns=['product_description'])
    
    data = data_transformer.stringify_values(data=data,
                                             columns=list(data.columns))
    
    data = data_transformer.remove_white_space(data=data,
                                               columns=list(data.columns))
    
    data = data_transformer.to_uppercase(data=data,
                                         columns=['product_size', 'product_color', 'product_sku'])
    
    data = data_transformer.check_url(data=data,
                                      column='product_url',
                                      pattern=r"(?:^https\:\/\/koroshishop\.com\/fr-fi\/products\/)(.+)(?:)")
    
    data = data_transformer.check_url(data=data,
                                      column='product_image',
                                      pattern=r"(?:^https\:\/\/cdn\.shopify\.com\/s\/files)(.+)(?:)")
    
    data = data.with_columns(
        pl.col('product_gross_price').fill_null(pl.col('product_net_price'))
    )
    data = data.with_columns(
        pl.col('product_id').cast(pl.Int64)
    )

    data = data_transformer.convert_to_price(data=data,
                                             column='product_gross_price',
                                             currency_col_name='product_currency')
    
    data = data_transformer.convert_to_price(data=data,
                                             column='product_net_price',
                                             currency_col_name='product_currency')
    
    return data