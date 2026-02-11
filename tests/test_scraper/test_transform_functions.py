import os
import sys

SCRAPER_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                            '..',
                                            '..')
                            )
sys.path.append(SCRAPER_PATH)


from scraper.transform.transform_data import remove_tags, remove_white_space, process_price, stringify, check_url

def test_remove_tags():

    assert remove_tags("<div>Hello<strong>World</strong></div>")
    assert remove_tags("No tags please") == "No tags please"


def test_process_price() :
    
    assert process_price(2299) == 22.99
    assert process_price(22990) == 229.9
    assert process_price(229.9) == 2.299
    assert isinstance(process_price(2299), float)


def test_check_url() :

    assert check_url("https://koroshishop.com/fr-fi/products/t-shirt-femme-a-manches-longues-en-tulle-elastique-semi-transparent-avec-doublure-et-imprime-floral-effet-velours", r"(?:^https\:\/\/koroshishop\.com\/fr-fi\/products\/)(.+)(?:)")
    assert not check_url("https://google.com", r"(?:^https\:\/\/koroshishop\.com\/fr-fi\/products\/)(.+)(?:)")
    assert check_url("https://cdn.shopify.com/s/files/1/0922/7858/7767/files/2522ML07_000003_1.jpg?v=1760547809", r"(?:^https\:\/\/cdn\.shopify\.com\/s\/files)(.+)(?:)")


def test_remove_white_space() :

    assert remove_white_space("T-shirt femme \u00e0 manches longues en tulle") == "T-shirt femme à manches longues en tulle"
    assert remove_white_space("T-shirt femme ") == "T-shirt femme"
    assert remove_white_space(" T-shirt femme") == "T-shirt femme"
    assert remove_white_space(" T-shirt femme ") == "T-shirt femme"


def test_stringify():

    assert stringify(55965419143543) == "55965419143543"
    assert stringify(559654.19143543) == "559654.19143543"
    assert stringify("55965419143543") == "55965419143543"
    assert isinstance(stringify(55965419143543), str)