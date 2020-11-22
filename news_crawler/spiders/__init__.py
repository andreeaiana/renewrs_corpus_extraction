# -*- coding: utf-8 -*-

from datetime import datetime
from itertools import combinations
from scrapy.spiders import CrawlSpider
from scrapy.exceptions import NotConfigured
from scrapy.utils.project import get_project_settings

class BaseSpider(CrawlSpider):
    """
    Base class for all spiders; inherits from CrawlSpider and implements article validation methods

    :attr start_date: string, the date from which an article is relevant
    :attr end_date: string, the date until which an article is relevant
    :attr article_length: int, minimum article length required
    :attr keywords: list[string], query keyword stems 
    :attr keywords_min_frequency: int, minimum number of keyword stems that should be contained in a relevant article
    :attr keywords_min_distance: int, minimum token difference between any two words containing a keyword stem
    :attr query_keywords: list[string], list of keyword stems found in the article
    """

    def __init__(self):
        settings = get_project_settings()

        if not settings.get('START_DATE'):
            raise NotConfigured
        self.start_date = settings.get('START_DATE')
        self.start_date = datetime.strptime(self.start_date, '%d.%m.%Y')
    
        if not settings.get('END_DATE'):
            raise NotConfigured
        self.end_date = settings.get('END_DATE')
        self.end_date = datetime.strptime(self.end_date, '%d.%m.%Y')
        
        if not settings.get('ARTICLE_LENGTH'):
            raise NotConfigured
        self.article_length = settings.get('ARTICLE_LENGTH')

        if not settings.get('KEYWORDS'):
            raise NotConfigured
        self.keywords = settings.get('KEYWORDS')

        if not settings.get('KEYWORDS_MIN_FREQUENCY'):
            raise NotConfigured
        self.keywords_min_frequency = settings.get('KEYWORDS_MIN_FREQUENCY')

        if not settings.get('KEYWORDS_MIN_DISTANCE'):
            raise NotConfigured
        self.keywords_min_distance = settings.get('KEYWORDS_MIN_DISTANCE')

        self.query_keywords= list()

        super(BaseSpider, self).__init__()

    def is_out_of_date(self, date):
        """ 
        Check if the article's date is in the required range

        param: date: str, publication date of the article
        return: bool: True if date is inside required range, False otherwise
        """
        return date < self.start_date or date > self.end_date

    def has_min_length(self, text):
        """ 
        Check if the article's length has minimum required length

        param: text: str, article's body of text
        return: bool: True length meets minimum required length, False otherwise
        """
        return len(text.split()) >= self.article_length

    def has_valid_keywords(self, text):
        """ 
        Check if any of the required keywords are found at least twice in the article
        If true, check if the token distance between them meets the required minimum threshold (i.e. valid article)
        If article if valid, update list of found query keywords 

        param:  text: str, article's body of text
        return: bool: True if keyword requirements met, False otherwise
        """
        tokens = text.lower().split()

        # Extract matching positions and tokens
        matching_pos_tokens = [(tokens.index(token), token) for token in tokens if any(keyword in token for keyword in self.keywords)]
        
        # Check if there are any query keyword stems in the text
        if matching_pos_tokens:
            matching_positions, matching_tokens = map(list, zip(*matching_pos_tokens))

            # Check the frequency of query keyword stems in the text
            if len(matching_positions) >= self.keywords_min_frequency:
                
                # Check the token difference between query keyword stems 
                if any(abs(pos_1-pos_2) >= self.keywords_min_distance for (pos_1, pos_2) in list(combinations(matching_positions, 2))):
                    
                    # Update the list of query keyword stems found
                    self.query_keywords = list(set(filter(lambda x: any(x in token for token in matching_tokens), self.keywords)))
                    return True
        return False

    def get_query_keywords(self):
        """ 
        return: list: the query keywords found in the article
        """
        return self.query_keywords

    def parse(self, response):
        pass
