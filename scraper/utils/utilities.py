import json
import logging
import polars as pl
from typing import Any, Union



# ============================================================================= #
# ================================= READ JSON FILE ============================ #
# ============================================================================= #
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
    

# # ============================================================================= #
# # ======================= SAVE DATA INTO JSON FORMAT ========================== #
# # ============================================================================= #
# def to_json(fp: str,
#             obj: Any) -> None :

#     """
    
#     Load into a json file a data
        
#         Args
#             fp : [string] : the path where to store data
#             obj : [Any type] : the data to be stored

#         Raises
#             [PermissionError] : when having no permission on writting into the file
#             [Exception] : for other exception
        
#         Assertions
#             obj : raise an error when his value is None
#     """

#     assert obj is not None, "Cannot save empty object into json file"

#     try :
#         # Writting into the file
#         with open(fp, 'w') as file :
#             json.dump(fp=file, obj=obj)
#             file.close()
#             logging.info(f"Data saved successfully into the json file at location '{fp}'")

#     except PermissionError :
#         logging.error(f"Access denied to the json file at location : '{fp}'")

#     except Exception as error :
#         logging.error(f"An error occured during writting into the json file '{fp}' : {error}")


# # ============================================================================= #
# # =============================== JSON TO DATAFRAME =========================== #
# # ============================================================================= #
# def convert_json_to_dataframe(fp: Any) -> Union[pl.DataFrame, None] :
#         """
#         Read json file to put them into DataFrame format
            
#             Args
#                 fp : [string] : the file path where the data was saved

#             Return
#                 [pl.DataFrame or None] : if not None, we got the data into DataFrame format

#             Raises
#                 [FileNotFoundError] : when the file at the location :param:fp is missing
#                 [PermissionError] : when having no permission on reading the file
#                 [Exception] : for other exceptions
            
#         """

#         # The output data
#         result = None

#         try :
#             result = pl.read_json(fp)
#             self.logger.info(f"Reading the json file '{fp}' successfully")
                
#         except FileNotFoundError :
#             self.logger.error(f"Cannot find the json file in location : '{fp}'")

#         except PermissionError :
#             self.logger.error(f"Cannot access to the json file in location : '{fp}'")

#         except Exception as error :
#             self.logger.error(f"An error occured during reading the json file '{fp}' : {error}")
            
#         finally :
#             return result