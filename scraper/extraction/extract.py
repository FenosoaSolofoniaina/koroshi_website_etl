import re
import json
import logging
import requests
from typing import Any, List, Dict
from bs4 import BeautifulSoup



class AbstractAPIExtractor() :


    def __init__(self, configuration: Any=None, file_log: str=None) -> None :

        self.configuration = configuration
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

        handler = logging.FileHandler(file_log, mode='w')
        handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s.%(funcName)s()] %(message)s'))
        self.logger.addHandler(handler)


    def send_request(self, url: str) -> Any :

        response = None

        try :
            self.logger.info(f"Try to send a request to '{url}'")
            response = requests.get(url=url)
            response.raise_for_status()

            if response.status_code == 200 :
                self.logger.info(f"Got response ({response.status_code}) from '{url}'")
            
            else:
                raise requests.ConnectionError(f"{response.status_code} {response.reason}")

        except requests.ConnectionError as error:
            self.logger.error(f"A request type error occurred during sending a request to '{url}' : {error}")

        except Exception as error :
            self.logger.error(f"An internal error occurred for durong sending a request to '{url}' : {error}")

        finally :
            return response



class KoroshiProductsListExtractor(AbstractAPIExtractor) :


    def __init__(self, *args, **kwargs) -> None :

        AbstractAPIExtractor.__init__(self, *args, **kwargs)
        self.products_list_config = self.configuration["products-list"]


    def get_products_list(self, url) -> List[str] :
        
        products_list = []

        products = self.products_list_config["products"]
        response = self.send_request(url=url)

        if response is not None :
            soup = BeautifulSoup(response.text, 'html.parser')
            productElements = soup.select(selector=products["selector"])

            if len(productElements):
                for prodElm in productElements :
                    try :
                        products_list.append(f"{products['url-prefix']}{prodElm.attrs[products['attribute']]}")
                
                    except KeyError :
                        self.logger.error(f"Element \"{products["selector"]}\" has no attribute '{products['attribute']}'")
                        products_list.append('')
                        continue

            else:
                self.logger.error(f"No elements \"{products["selector"]}\" found in the page")
 
        return products_list

    
    def next_page(self, url: str, n_page: int) -> str :

        pagination_config = self.products_list_config['pagination']

        suffix = re.sub(pattern=r"<PNum>",
                        repl=str(n_page),
                        string=pagination_config['value'],
                        count=1)
        next_page_url = f"{url}{suffix}"
        self.logger.info(f"Next page link : {next_page_url}")

        return next_page_url
    


class KoroshiProductDataExtractor(AbstractAPIExtractor):


    def __init__(self, *args, **kwargs) -> None :

        AbstractAPIExtractor.__init__(self, *args, **kwargs)


    def extract_product_variants(self, data: str, url: str) -> List[Dict[str, Any]] :

        product_variants = []

        if data is not None :
            product_variants = list(map(lambda variant : {
                "product_url" : url,
                "product_name" : data['title'],
                "product_description" : data['description'],
                "product_id" : variant['id'],
                "product_sku" : variant['sku'],
                "product_color" : variant['option1'],
                "product_size" : variant['option2'],
                "product_image" : variant['featured_image']['src'],
                "product_net_price" : variant['price'],
                "product_gross_price" : variant['compare_at_price'],
                "product_stock_status" : variant['available'],
                "product_barcode" : variant['barcode']
        }, data['variants']))
            
        return product_variants
    

    def extract_product_data(self, product_url: str) -> List[Dict[str, Any]] :

        product_variants = []
        
        self.logger.info(f"Entering in the webpage with url : '{product_url}'")
        url = product_url.split('?variant=')[0]
        response = self.send_request(url=f"{url}.js")
        
        if response is not None :
            product_data = json.loads(response.text)
            product_variants = self.extract_product_variants(data=product_data, url=url)

            self.logger.info(f"Got ({len(product_variants)}) variants from url '{product_url}'")
            
        return product_variants