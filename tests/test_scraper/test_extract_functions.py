import os
import sys

# Point to the scraper directory
SCRAPER_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                            '..',
                                            '..')
                            )

sys.path.append(SCRAPER_PATH)


from scraper.extract.extract_data import KoroshiProductsListExtractor,KoroshiProductDataExtractor
from scraper.utils.utilities import read_json


# Point to the tests directory
TESTS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


def test_get_products_list() :

    json_config = read_json(fp=os.path.join(TESTS_PATH, "test_scraper/config.json"))
    koroshi_products_list_scraper = KoroshiProductsListExtractor(configuration=json_config,
                                                                 file_log=os.path.join(TESTS_PATH, 'test_scraper/logs/products_list.log')
                                                                )
    
    # Extraction from url that is a real page web
    products_list = koroshi_products_list_scraper.get_products_list(json_config['main-url'])
    assert len(products_list) > 0

    # Fake url
    products_list = koroshi_products_list_scraper.get_products_list('https://fake-url.com')
    assert len(products_list) == 0



def test_get_product_data() :

    koroshi_products_data_scraper = KoroshiProductDataExtractor(file_log=os.path.join(TESTS_PATH, 'test_scraper/logs/products.log'))

    product_data = koroshi_products_data_scraper.extract_product_data(product_url="https://koroshishop.com/fr-fi/products/ceinture-femme-effet-cuir?variant=55971618685303")
    assert len(product_data) > 0
    assert product_data[0]["product_name"] == "Ceinture femme effet cuir"

    product_data = koroshi_products_data_scraper.extract_product_data(product_url="https://fake-url")
    assert len(product_data) == 0