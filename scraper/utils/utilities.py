import json
import logging
from typing import Any



def read_json(fp: str) -> Any :

    json_config = None
    
    try :
        with open(fp, 'r') as config_file :
            json_config = json.load(config_file)
            logging.info(f"Reading the json file '{fp}' successfully")
            config_file.close()
            
    except FileNotFoundError :
        logging.error(f"Cannot find the json file in location : '{fp}'")

    except PermissionError :
        logging.error(f"Cannot access to the json file in location : '{fp}'")

    except Exception as error :
        logging.error(f"An error occured during reading the json file '{fp}' : {error}")
        
    finally :
        return json_config
    

def to_json(fp: str, obj: Any) -> Any :
    
    try :
        with open(fp, 'w') as config_file :
            json.dump(fp=config_file, obj=obj)
            logging.info(f"Data saved into the json file '{fp}' successfully")
            config_file.close()

    except PermissionError :
        logging.error(f"Cannot access to the json file in location : '{fp}'")

    except Exception as error :
        logging.error(f"An error occured during writting into the json file '{fp}' : {error}")