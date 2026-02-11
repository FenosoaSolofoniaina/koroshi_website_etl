import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from utils.utilities import read_json, convert_json_to_dataframe
from utils.extract import get_all_products_data, get_all_products_list
            

# ================================================================================================================== #
# ============================================ MAIN FUNCTION ======================================================= #
# ================================================================================================================== #
def main() -> None :
    """
        The main function that execute the pipeline
    """
    
    logging.info("======================= PROGRAM STARTED =======================")

    current_time = datetime.now()
    
    # Variables d'environnements
    load_dotenv()
    CONFIGURATION_FP = os.getenv('CONFIGURATION_FILE_PATH', '')
    BASE_DIR = os.path.dirname(__file__)

    logging.basicConfig(filename=os.path.join(BASE_DIR, 'logs/main.log'),
                        filemode='w',
                        format='[%(asctime)s] [%(levelname)s] [%(funcName)s()] %(message)s',
                        level=logging.INFO)

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
                                                 log_file=os.path.join(BASE_DIR, 'logs/products_data.log')
                                                )
        
        # Load data extracted into a PostgreSQL database
        # load_data_to_db(data_fp=products_data_fp,
        #                 log_file=os.path.join(BASE_DIR, 'logs/to_db.log'))

        df = convert_json_to_dataframe(fp=products_data_fp)
        
    logging.info("======================= PROGRAM FINISHED =======================")
    time_diff = datetime.now() - current_time
    logging.info(f"Program takes {time_diff}")
    print(f"Program takes {time_diff}")


# ============================================================================= #
# ============================== ENTRY POINT ================================== #
# ============================================================================= #
main()