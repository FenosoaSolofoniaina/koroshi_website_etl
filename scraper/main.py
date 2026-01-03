import os
import logging
from datetime import datetime
from typing import Any
from dotenv import load_dotenv

from utils.utilities import read_json, to_json
from extraction.extract import KoroshiProductsListExtractor, KoroshiProductDataExtractor
from loading.load import KoroshiDataLoader



# ============================================================================= #
# ============================== PRODUCTS LIST ================================ #
# ============================================================================= #
def get_all_products_list(configuration: Any, log_file: str) -> str :

    # Output
    all_products = []

    koroshi_products_list_scraper = KoroshiProductsListExtractor(configuration=configuration,
                                                                 file_log=log_file)
    page = 1

    while True :

        # /!\ A enlever ce bout de code si necessaire /!\
        # Pour le test, on va se limiter à faire 3 pagination
        if page > 3 :
            logging.warning(f"Aborting pagination")
            break
        # /!\

        # L'url par défaut
        url = configuration["main-url"]
        
        # Cas où on procède à la pagination
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

    logging.info(f"Get ({len(all_products)}) total of products from the website")
    
    # Save data into a json file
    output_fp = os.path.join(os.path.dirname(__file__),
                             f'json/products_list_{datetime.now().date()}.json')
    to_json(output_fp,
            all_products)

    return output_fp


# ============================================================================= #
# ============================== PRODUCTS DATA ================================ #
# ============================================================================= #

def get_all_products_data(products_list_fp: str, log_file: str) -> str:
    
    products_data = []

    # Extraction de la liste des produits contenu dans un fichier json
    products_list = read_json(fp=products_list_fp)

    if products_list is not None :
        
        koroshi_products_data_scraper = KoroshiProductDataExtractor(file_log=log_file)
        
        for product_url in products_list :

            logging.info(f" === Extraction of product data started ===")
            
            current_product_data = koroshi_products_data_scraper.extract_product_data(product_url=product_url)
            products_data.extend(current_product_data)

            logging.info(f" === Extraction of product data finished. Exit with code 0 === \n")

    # Save data into a json file
    output_fp = os.path.join(os.path.dirname(__file__),
                             f'json/products_data_{datetime.now().date()}.json')
    to_json(output_fp,
            products_data)

    return output_fp


# ============================================================================= #
# ============================== LOAD INTO DATABASE =========================== #
# ============================================================================= #

def load_data_to_db(data_fp: str, log_file: str) -> None :

    load_dotenv()
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    HOST = os.getenv("HOST")
    PORT = os.getenv("PORT")
    DBNAME = os.getenv("DBNAME")
    SCHEMA = os.getenv("SCHEMA")
    TABLE_NAME = os.getenv("TABLE_NAME")
    
    connection_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{HOST}:{PORT}/{DBNAME}"
    
    dataloader = KoroshiDataLoader(connection_url=connection_url,
                                   schema=SCHEMA,
                                   table=f"{TABLE_NAME}_{datetime.now().date().__str__().replace('-', '_')}",
                                   file_log=log_file)
    df = dataloader.from_json(fp=data_fp)

    if df is not None:
        #print(df.head(20))

        df.write_csv('koroshi_data.csv')
        logging.info(f"Data saved into csv file")

        dataloader.insert_data(df)
            

# ================================================================================================================== #
# ============================================ MAIN FUNCTION ======================================================= #
# ================================================================================================================== #

def main() -> None :

    # Variables d'environnements
    load_dotenv()
    CONFIGURATION_FP = os.getenv('CONFIGURATION_FILE_PATH')
    BASE_DIR = os.path.dirname(__file__)

    logging.basicConfig(filename=os.path.join(BASE_DIR, 'logs/main.log'),
                        filemode='w',
                        format='[%(asctime)s] [%(levelname)s] [%(funcName)s()] %(message)s',
                        level=logging.INFO)
    
    logging.info("======================= PROGRAM STARTED =======================")

    # fichier de configuration nécessaire au scraping du site
    configuration_fp = os.path.join(BASE_DIR, CONFIGURATION_FP)
    json_config = read_json(fp=configuration_fp)

    if json_config is not None :
        
        # Extraction and saving products list
        products_list_fp = get_all_products_list(configuration=json_config,
                                                 log_file = os.path.join(BASE_DIR, 'logs/products_list.log')
                                                )
                                                
        
        # Extraction and saving products data
        products_data_fp = get_all_products_data(products_list_fp=products_list_fp,
                                                 log_file=os.path.join(BASE_DIR, 'logs/products_data.log')
                                                )
        
        #products_data_fp = "/home/fenosoa/Projects/data_engineer/koroshi_etl/koroshi_website_etl/./scripts/json/products_data_2025-12-20.json"
        print(products_data_fp)
        load_data_to_db(data_fp=products_data_fp,
                        log_file=os.path.join(BASE_DIR, 'logs/to_db.log'))
    
    logging.info("======================= PROGRAM FINISHED =======================")



# ============================================================================= #
# ============================== ENTRY POINT ================================== #
# ============================================================================= #
main()