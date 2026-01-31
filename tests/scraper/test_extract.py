import os
import sys

SCRAPER_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                            '..',
                                            '..')
                            )

sys.path.append(SCRAPER_PATH)


from scraper.extract.extract_data import KoroshiProductsListExtractor
from scraper.utils.utilities import read_json


def test_get_products_list() :
    

    TESTS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    json_config = read_json(fp=os.path.join(TESTS_PATH, "scraper/config.json"))
    koroshi_products_list_scraper = KoroshiProductsListExtractor(configuration=json_config,
                                                                 file_log=os.path.join(TESTS_PATH, 'scraper/logs/products_list.log')
                                                                )
    products_list = koroshi_products_list_scraper.get_products_list(json_config['main-url'])
    assert len(products_list) > 0 