import os
import time
import logging
from datetime import datetime
from typing import Any
import polars as pl

from utils.utilities import read_json, to_json
from extract.extract_data import KoroshiProductsListExtractor, KoroshiProductDataExtractor



# ============================================================================= #
# ============================== PRODUCTS LIST ================================ #
# ============================================================================= #
def get_all_products_list(configuration: Any,
                          log_file: str,
                          is_test: bool=False) -> str :
    """
    Extract products' link, save into a json file and return the path of this file
        
        Args
            configuration : [Any type] : object data that contains configuration how to extract data
            log_file : [string] : the file path where log will be saved

        Return
            [string] : file path where data is stored

    """

    # Variable that contains all products' link
    all_products = []

    koroshi_products_list_scraper = KoroshiProductsListExtractor(configuration=configuration,
                                                                 file_log=log_file)

    # Loop to extract links along url
    for url in configuration["urls"] :
        
        page = 1

        # First url
        while True :

            if is_test and page > 3 : break
        
            # Get next page url
            if page > 1 :
                url = koroshi_products_list_scraper.next_page(url=url,
                                                            n_page=page)
            
            # Extraction de la liste des produits
            logging.info(f" === Extraction of product list started ===")
            logging.info(f"Entering in the webpage with url : '{url}'")
            current_page_products = koroshi_products_list_scraper.get_products_list(url=url)
            
            # On casse la boucle au cas où il n'y a plus de produits sur la page
            if len(current_page_products) == 0 :
                logging.warning(f"No products found, stop exploring website")
                break

            all_products.extend(current_page_products)
            logging.info(f"Got ({len(current_page_products)}) products : {current_page_products[:10]}")
            logging.info(f" === Extraction of product list finished. Exit with code 0 ===\n")
            page += 1
            time.sleep(1)

    logging.info(f"Get ({len(all_products)}) total of products from the website")
    
    # Save data into a json file
    output_fp = os.path.abspath(os.path.join(os.path.dirname(__file__),
                            '..',
                            f'raw_data/products_list_{datetime.now().date().__str__().replace('-', '')}.json'))
    to_json(output_fp, all_products)

    return output_fp


# ============================================================================= #
# ============================== PRODUCTS DATA ================================ #
# ============================================================================= #

def get_all_products_data(products_list_fp: str,
                          log_file: str) -> str:
    
    """
    Extract data about the product provided by his url
        
        Args
            products_list_fp : [string] : file path where data that contains products'link
            log_file : [string] : the file path where log will be saved

        Return
            [string] : the file path where data is saved
    """
    
    # Contains all data of each product
    products_data = []

    # Extraction de la liste des produits contenu dans un fichier json
    products_list = read_json(fp=products_list_fp)

    # Get products' link
    if products_list is not None :
        
        koroshi_products_data_scraper = KoroshiProductDataExtractor(file_log=log_file)
        
        # Loop to extract data about each product provided by the product_url
        for product_url in products_list :
            logging.info(f" === Extraction of product data started ===")
            current_product_data = koroshi_products_data_scraper.extract_product_data(product_url=product_url)
            products_data.extend(current_product_data)
            logging.info(f" === Extraction of product data finished. Exit with code 0 === \n")

    # Save data into a json file
    output_fp = os.path.abspath(os.path.join(os.path.dirname(__file__),
                             '..',
                             f'raw_data/products_data_{datetime.now().date().__str__().replace('-', '')}.csv'))
    
    dataframe = pl.DataFrame(data=products_data)
    dataframe.to_pandas().to_csv(output_fp)
    logging.info(f"Products Data saved into csv file '{output_fp}'")

    return output_fp