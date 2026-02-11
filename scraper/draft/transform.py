from typing import Any
import re
from bs4 import BeautifulSoup



def remove_tags(text: str) -> str :
    """
    Remove html tags from a text
        
        Args
            text : [string] : text that contains html tags

        Return
            [string] : raw text without html tags
    """

    return BeautifulSoup(text, "html.parser").get_text(" ", strip=True)


def remove_white_space(text: str) -> str :
    """
    Uniform text into title type format
        
        Args
            text : [string] : the text to processed

        Return
            [string] : formated text
    """

    return text.strip()


def process_price(value: Any) -> float:
    """
    Back to the real value of the price
        
        Args
            value : [Any type] : the value to be converted, it can be in string, int or float format

        Return
            [float] : the real value
    """
    return float(value) / 100


def check_url(url: str,
              pattern: str) -> bool :
    """
    Check if the url matches with the pattern
        
        Args
            url : [string] : the url to check
            pattern : [string] : the pattern that the url must match

        Return
            [bool] : if the :param:url match with the :param:pattern
    """

    return re.match(pattern=pattern,
                    string=url)


def stringify(value: Any) -> str:
    """
    Stringify a value
        
        Args
            value : [Any type] : the objet value to be converted into string, it can be a int or float

        Return
            [string] : the string value of :param:value
    """

    return str(value)


