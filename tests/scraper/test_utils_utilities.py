import os
import sys

SCRAPER_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                            '..',
                                            '..')
                            )
sys.path.append(SCRAPER_PATH)

from scraper.utils.utilities import read_json


def test_read_json() :
    """
        Tester la fonction `read_json` pour vérifier son comportement
        Le test se déroule en 2 temps :
            - On test sur un fichier qui n'existe pas --> on s'attent à un résultat None
            - On test sur un fichier qui existe --> on s'attent à un résultat qui n'est pas None
    """

    TESTS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    
    # test `read_json` function with a not existing file
    json_config = read_json(fp=os.path.join(TESTS_PATH, "scraper/configuration.json"))
    assert json_config is None
    
    # test `read_json` function with an existing file
    json_config = read_json(fp=os.path.join(TESTS_PATH, "scraper/config.json"))
    assert json_config is not None