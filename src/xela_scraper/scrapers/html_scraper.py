import requests
import bs4
from xela_scraper import XelaScraper


class HtmlScraper(XelaScraper):
    def __init__(self, response:requests.Response):
        super().__init__(response)
        self.parsed_html:bs4.BeautifulSoup = None
    
    def parse_bs4(self, filter:tuple|None = None) -> bs4.BeautifulSoup:
        soup_strainer = bs4.SoupStrainer(
            filter[0],filter[1]) if filter else None

        self.parsed_html = bs4.BeautifulSoup(
            self.content, 'html.parser', parse_only=soup_strainer)

        return self

    def parse(self, parser:str = 'bs4', filter:None=None):
        if parser == 'bs4':
            return self.parse_bs4(filter)

    def is_soup(self):
        return isinstance(self.parsed_html, (bs4.BeautifulSoup, bs4.element, bs4.Tag))

    def extract(self, selector, limit=None):
        if self.is_soup():
            parsed_content = self.parsed_html.select(selector, limit=limit)
            return parsed_content
    
    def extract_one(self, selector, inplace=False):
        parsed_content = self.extract(selector, limit=1)
        if parsed_content:
            if inplace:
                self.parsed_html = parsed_content[0]
                self.content = str(parsed_content[0])

            return parsed_content[0]
        self.content = None
        return None

    def extract_str(self, selector):
        if self.is_soup():
            parsed_html = self.extract_one(selector)
            if parsed_html:
                parsed_str = str(parsed_html.text).strip()
                parsed_html.decompose()
                return parsed_str
            return None