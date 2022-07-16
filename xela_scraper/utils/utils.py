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

def get_entity(client:datastore.Client(), kind, identifier):
    key = client.key(kind, identifier)
    data = client.get(key)
    return data

def money_to_decimal(money:str) -> Decimal:
    return Decimal(re.sub(r'[^\d.]', '', money))

def load_json_from_blob(bucket:storage.Bucket, path):
    blob = bucket.blob(path)
    data = json.loads(blob.download_as_string(client=None))
    return data


def blob_exists(bucket, prefix, filename):
    for blob in bucket.list_blobs(prefix=prefix):
        suffix = blob.name.split('/')[-1]
        if suffix == filename:
            return True
    return False

def allow_write_to_blob(bucket:storage.Bucket, prefix, filename, auction_status, overwrite_setting):
    file_exists = blob_exists(bucket, prefix, filename)

    write_rules = [
        not file_exists,
        overwrite_setting == 'y',
        overwrite_setting.replace('_only', '') == auction_status
        ]

    if any(write_rules):
        return True
    return False


def create_avro_file(data:list, path:str, avro_schema:dict):
    with open(path, 'wb') as out:
        writer(out, avro_schema, data)

def to_blob_storage(bucket, source_path:str,blob_path:str):
    new_blob = bucket.blob(blob_path)
    new_blob.upload_from_filename(filename=source_path)

def entities_to_dict(parent_entity):
    return {x: parent_entity[x] for x in parent_entity}

def to_cloud_storage(auction, bucket, listings, avro_schema):
    blob_location = auction.filename(loc='auctions/')
    temp_location = auction.filename(loc='./temp/')

    create_avro_file(
        data=listings,
        path=temp_location,
        avro_schema=avro_schema)

    to_blob_storage(bucket,
                            source_path=temp_location,
                            blob_path=blob_location)

def format_date_string(date_string:str,
                       target_formatting:str='%Y-%m-%d') -> str:
    '''
    Parses date string then returns it as a string in target format
    '''
    date = parse(date_string)
    return datetime.strftime(date, format=target_formatting)

