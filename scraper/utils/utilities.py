import json
import logging
from typing import Any



def read_json(fp: str) -> Any :

    """
    Read a json file and get the data inside
        
        Args
            fp : [string] : path to the json file

        Return
            [Any type] : the data into of the json into dictionary format

        Raises
            [FileNotFoundError] : when the file at the location :param:fp is missing
            [PermissionError] : when having no permission on reading the file
            [Exception] : for other exceptions
    """

    # The output data
    json_object = None
    
    try :
        # Reading the file
        with open(fp, 'r') as file :
            json_object = json.load(file)
            file.close()
            logging.info(f"Success on reading the json file at location '{fp}'")
            
    except FileNotFoundError :
        logging.error(f"Cannot find the json file at location : '{fp}'")

    except PermissionError :
        logging.error(f"Access denied to the json file at location : '{fp}'")

    except Exception as error :
        logging.error(f"An error occured during reading the json file '{fp}' : {error}")
        
    finally :
        return json_object
    

def to_json(fp: str,
            obj: Any) -> None :

    """
    
    Load into a json file a data
        
        Args
            fp : [string] : the path where to store data
            obj : [Any type] : the data to be stored

        Return

        Raises
            [PermissionError] : when having no permission on writting into the file
            [Exception] : for other exception
        
        Assertions
            obj : raise an error when his value is None
    """

    assert obj is not None, "Cannot save empty object into json file"

    try :
        # Writting into the file
        with open(fp, 'w') as file :
            json.dump(fp=file, obj=obj)
            file.close()
            logging.info(f"Data saved successfully into the json file at location '{fp}'")

    except PermissionError :
        logging.error(f"Access denied to the json file at location : '{fp}'")

    except Exception as error :
        logging.error(f"An error occured during writting into the json file '{fp}' : {error}")