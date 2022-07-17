from bs4 import BeautifulSoup, SoupStrainer
from requests.adapters import HTTPAdapter, Retry
import requests
from google.cloud import storage, datastore
from fastavro import writer, reader, parse_schema
import json
from decimal import Decimal
import re
# from dateutil.parser import parse
from datetime import datetime
from dateutil.parser import parse
# import re


def request_with_retries(method, url,retries=5,**kwargs):
    session = requests.Session()
    retries = Retry(total=retries, backoff_factor=1)
    session.mount('https://',HTTPAdapter(max_retries=retries))
    response = session.request(method, url,timeout=10, **kwargs)
    return response

def get_with_retries(url,retries=5,**kwargs):
    return request_with_retries('GET', url, retries, **kwargs)

def post_with_retries(url,retries=5,**kwargs):
    return request_with_retries('POST', url, retries, **kwargs)
    
    
def to_soup(markup, parser:str='html.parser', parse_filter:tuple|None=None) -> BeautifulSoup:
    soup_strainer = SoupStrainer(parse_filter[0],parse_filter[1]) if parse_filter else None
    soup = BeautifulSoup(markup, features=parser, parse_only=soup_strainer)
    return soup


def get_strained_soup(html,strain_element:str ='div',strain_attribute:dict={}, parser='html.parser'):
    soup_strainer = SoupStrainer(strain_element,strain_attribute)
    soup = BeautifulSoup(html,parser,parse_only=soup_strainer)
    return soup

def money_to_decimal(money:str) -> Decimal:
    return Decimal(re.sub(r'[^\d.]', '', money))

def extract_str(html):
    if html:
        return str(html.text).strip()

def extract_date(html,
                       target_formatting:str='%Y-%m-%d') -> str:
    '''
    Parses date string then returns it as a string in target format
    '''
    date = parse(extract_str(html))
    return datetime.strftime(date, format=target_formatting)

