import requests
from urllib.parse import urlencode, urlparse, parse_qsl
import random
from time import sleep

class XelaScraper:
    def __init__(self, response:requests.Response):
        self.content = response.content
        self.url = response.url
        self.status_code = response.status_code

    @classmethod
    def request(cls, method, url, **kwargs):
        response = requests.request(method, url, **kwargs)
        return cls(response)

    @classmethod
    def get(cls, url, **kwargs):
        return cls.request('GET', url, **kwargs)

    @classmethod
    def post(cls, url, **kwargs):
        return cls.request('POST', url, **kwargs)

    def request_new_page(self, method, parameters:dict|None, 
                    url_components:dict|None=None, **kwargs):
        parsed_url = urlparse(self.url)

        if parameters:
            parsed_qsl = parse_qsl(parsed_url[4])
            original_parameters = dict(parsed_qsl)
            new_parameters = original_parameters | parameters

            if url_components:
                url_components['query'] = urlencode(new_parameters)
            else:
                url_components = {'query': urlencode(new_parameters)}
            
        if url_components:
            new_url = parsed_url._replace(**url_components).geturl()
        else:
            new_url = self.url

        return self.request(method, new_url, **kwargs)

    def get_new_page(self, parameters:dict=None, 
                url_components:dict=None, **kwargs):
        return self.request_new_page('GET', parameters,
                                     url_components, **kwargs)
    
    def post_new_page(self, parameters:dict, 
                      url_components:dict=None, **kwargs):
        return self.request_new_page('POST', parameters, 
                                     url_components, **kwargs)

    def request_next_page(self, method, page_name:str='page',
                          increment=1, **kwargs):
        parsed_url = urlparse(self.url)
        parsed_qsl = parse_qsl(parsed_url[4])
        parameters = dict(parsed_qsl)
        current_page = int(parameters[page_name])
        parameters[page_name] = str(current_page + increment)
        print(urlencode(parameters))
        new_url = parsed_url._replace(query=urlencode(parameters)).geturl()
        return self.request(method, new_url, **kwargs)

    def get_next_page(self, page_name:str='page', increment = 1, **kwargs):
        return self.request_next_page('GET', page_name, increment, **kwargs)
    
    def post_next_page(self, page_name:str='page', increment = 1, **kwargs):
        return self.request_next_page('POST', page_name, increment, **kwargs)

    def print_html(self):
        print(self.content)
    
    def parse_url(self):
        return urlparse(self.url)

    def parse_qsl(self):
        parsed_url = self.parse_url()
        return parse_qsl(parsed_url[4])

    def random_sleep(self, sleep_min=5, sleep_max=15) -> None:
        random_seconds = random.randint(sleep_min + 1, sleep_max)
        random_miliseconds = float(random.randint(0, 100)) / float(100)
        sleep_for = float(random_seconds) - random_miliseconds

        print(f'Sleeping:{sleep_for}')
        sleep(sleep_for)