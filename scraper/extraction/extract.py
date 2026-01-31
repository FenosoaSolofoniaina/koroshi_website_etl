import re
import json
import logging
import requests
from typing import Any, List, Dict
from bs4 import BeautifulSoup



# ==================================================================================================================================================================== #
# ======================================================================= AbstractAPIExtractor ======================================================================= #
# ==================================================================================================================================================================== #
class AbstractAPIExtractor() :
    """
        Base class to use when getting data from an API, do not implement this class, it'is for making an abstract base class
    """

    def __init__(self,
                 configuration: Any=None,
                 file_log: str=None) -> None :
        """
        Constructor : initialise the log management and the configuration file
            
            Args
                configuration : [Any type] : object that store configurations using by the function inside this class and his inheritance
                file_log : [string] : the path where to store logs during the runtime execution when calling/using this class

            Return
        """

        self.configuration = configuration

        # Set the log management
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler(file_log, mode='w')
        handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s.%(funcName)s()] %(message)s'))
        self.logger.addHandler(handler)


    def send_request(self,
                     url: str) -> Any :
        """
        Send a request to API specified by the url :param:url
            
            Args
                url : [string] : The API's url

            Return
                [Any type] : the response of the request into a Response type

            Raises
                [requests.ConnectionError] : when we got bad response or error from server
                [Exception] : for other exceptions
            
            Assertions
                url : raise an error when the url is an empty string or a None value
        """

        assert url is not None and url != '', "Cannot send a request to a 'None' or empty string, please check"

        # the output data
        response = None

        try :
            # Send a call API
            self.logger.info(f"Try to send a request to '{url}'")
            response = requests.get(url=url)
            response.raise_for_status()

            if response.status_code == 200 :
                self.logger.info(f"Got response ({response.status_code}) from '{url}'")
            
            # Got bad response or error from server
            else:
                raise requests.ConnectionError(f"{response.status_code} {response.reason}")

        except requests.ConnectionError as error:
            self.logger.error(f"A request type error occurred during sending a request to '{url}' : {error}")

        except Exception as error :
            self.logger.error(f"An internal error occurred for durong sending a request to '{url}' : {error}")

        finally :
            return response



# ==================================================================================================================================================================== #
# ======================================================================= AbstractAPIExtractor ======================================================================= #
# ==================================================================================================================================================================== #
class KoroshiProductsListExtractor(AbstractAPIExtractor) :
    """
        Provide some feature to extract list of url of products 
    """

    def __init__(self, *args, **kwargs) -> None :
        """
            Initialize super class and the configuration to use for performing the behaviour of each function inside the class
        """

        AbstractAPIExtractor.__init__(self, *args, **kwargs)

        # Configuration
        self.products_list_config = self.configuration["products-list"]


    def get_products_list(self,
                          url: str) -> List[str] :
        """
        Get all the product's links and store them into a list
            
            Args
                url : [string] : the url where product's links will be extracted

            Return
                [list of string] : list that contains all product links
        """

        # The output data
        products_list = []

        products = self.products_list_config["products"]
        response = self.send_request(url=url)

        # Got the good response and extract data from it
        if response is not None :
            soup = BeautifulSoup(response.text, 'html.parser')
            productElements = soup.select(selector=products["selector"])

            if len(productElements):
                for prodElm in productElements :
                    # Extract link from html element
                    try :
                        products_list.append(f"{products['url-prefix']}{prodElm.attrs[products['attribute']].strip()}")
                    
                    # When the html element does not have the attribute that we expected, skip this element
                    except KeyError :
                        self.logger.error(f"Element '{products["selector"]}' has no attribute '{products['attribute']}'")
                        products_list.append('')
                        continue
            else:
                self.logger.error(f"No elements \"{products["selector"]}\" found in the page")
 
        return products_list

    
    def next_page(self,
                  url: str,
                  n_page: int) -> str :
        """
        Get the next page link
            
            Args
                url : [string] : the url where product's links will be extracted
                n_page : [integer] : the value of incremental parameter

            Return
                [string] : the next page link

            Assertions
                n_page : Raise an error when this parameter is not an integer
        """

        assert isinstance(n_page, int), "Expected of value of 'n_page' is integer, please check"

        # pagination configuration
        pagination_config = self.products_list_config['pagination']

        # Extract the value of the next parameter
        suffix = re.sub(pattern=r"<PNum>",
                        repl=str(n_page),
                        string=pagination_config['value'],
                        count=1)
        # Set the new url to the next page url
        next_page_url = f"{url}{suffix}"
        self.logger.info(f"Next page link : {next_page_url}")

        return next_page_url
    


# ==================================================================================================================================================================== #
# ======================================================================= AbstractAPIExtractor ======================================================================= #
# ==================================================================================================================================================================== #
class KoroshiProductDataExtractor(AbstractAPIExtractor):
    """
        Provide some feature to extract data of each product
    """

    def __init__(self, *args, **kwargs) -> None :
        """
            Initialize super class and the configuration to use for performing the behaviour of each function inside the class
        """

        AbstractAPIExtractor.__init__(self, *args, **kwargs)


    def extract_product_variants(self,
                                 data: Any, 
                                 url: str) -> List[Dict[str, Any]] :
        """
        Extract variants about the product and save into a list of dictionary
            
            Args
                data : [Any type] : Response from the call API into object format
                url : [string] : the url to the product

            Return
                [List of object] : list that stores data about the product into dictionary
        """

        # The output data
        product_variants = []

        if data is not None :
            product_variants = list(
                map(
                    lambda variant : {
                            "product_url" : url,
                            "product_id" : variant['id'],
                            "product_sku" : variant['sku'],
                            "product_name" : data['title'],
                            "product_color" : variant['option1'],
                            "product_size" : variant['option2'],
                            "product_image" : variant['featured_image']['src'],
                            "product_description" : data['description'],
                            "product_net_price" : variant['price'],
                            "product_gross_price" : variant['compare_at_price'],
                            "product_stock_status" : variant['available'],
                            "product_barcode" : variant['barcode']
                        },
                    data['variants']
                )
            )
            
        return product_variants
    

    def extract_product_data(self,
                             product_url: str) -> List[Dict[str, Any]] :

        """
        When we got the url of the product, we extract all data about that product
            
            Args
                product_url : [url] : 

            Return
                [list of object] : 
        """
        
        # The output data
        product_variants = []
        
        self.logger.info(f"Entering in the webpage with url : '{product_url}'")
        url = product_url.split('?variant=')[0]
        response = self.send_request(url=f"{url}.js")
        
        if response is not None :
            product_data = json.loads(response.text)

            # Extract all variants
            product_variants = self.extract_product_variants(data=product_data, url=url)
            self.logger.info(f"Got ({len(product_variants)}) variants from url '{product_url}'")
            
        return product_variants