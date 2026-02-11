import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from utils.utilities import read_json, convert_json_to_dataframe
from utils.extract import get_all_products_data, get_all_products_list
from utils.transform import preprocessing
from utils.load import load_data_to_db, load_data_to_bigquery
            

# ================================================================================================================== #
# ============================================ MAIN FUNCTION ======================================================= #
# ================================================================================================================== #
def main() -> None :
    """
        The main function that execute the pipeline
    """
    
    logging.info("======================= PROGRAM STARTED =======================")
    print("======================= PROGRAM STARTED =======================")

    current_time = datetime.now()
    
    # Variables d'environnements
    load_dotenv()
    CONFIGURATION_FP = os.getenv('CONFIGURATION_FILE_PATH', '')
    BASE_DIR = os.path.dirname(__file__)

    logging.basicConfig(filename=os.path.join(BASE_DIR, 'logs/main.log'),
                        filemode='w',
                        format='[%(asctime)s] [%(levelname)s] [%(funcName)s()] %(message)s',
                        level=logging.INFO,
                        force=True)

    # fichier de configuration nécessaire au scraping du site
    configuration_fp = os.path.join(BASE_DIR, CONFIGURATION_FP)
    json_config = read_json(fp=configuration_fp)

    if json_config is not None :
        
        # Extract and save products list
        products_list_fp = get_all_products_list(configuration=json_config,
                                                 log_file = os.path.join(BASE_DIR, 'logs/products_list.log')
                                                )
                                                
        
        # Extract and save products data
        products_data_fp = get_all_products_data(products_list_fp=products_list_fp,
                                                 log_file=os.path.join(BASE_DIR, 'logs/products.log')
                                                )
        # Transform data
        products_data_fp = "/home/fenosoa/Projects/data_engineer/koroshi/scraper/json/products_data_2026-02-11.json"
        df = convert_json_to_dataframe(fp=products_data_fp)
        df = preprocessing(data=df,
                           log_file=os.path.join(BASE_DIR, 'logs/data_transformations.log')
                           )

        # Load data to Big Query
        load_data_to_db(data=df,
                        log_file=os.path.join(BASE_DIR, 'logs/to_postgres.log')
                        )
        PROJECT_ID = os.getenv('BG_PROJECT_ID', '')
        DATASET_ID = os.getenv('BQ_DATASET_ID', '')
        TABLE_NAME = os.getenv('BQ_TABLE_NAME', '')
        service_account_fp = "dbt-learn-bq-472715-f4-392e0df00188.json"

        load_data_to_bigquery(service_account_fp=os.path.join(BASE_DIR, service_account_fp),
                              project=PROJECT_ID,
                              dataset=DATASET_ID,
                              table=TABLE_NAME,
                              data=df,
                              log_file=os.path.join(BASE_DIR, 'logs/big_query.log')
                              )

        
    logging.info("======================= PROGRAM FINISHED =======================")
    print("======================= PROGRAM FINISHED =======================")
    time_diff = datetime.now() - current_time
    logging.info(f"Program takes {time_diff}")
    print(f"Program takes {time_diff}")


# ============================================================================= #
# ============================== ENTRY POINT ================================== #
# ============================================================================= #
main()