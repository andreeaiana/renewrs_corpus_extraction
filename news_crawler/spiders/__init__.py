# -*- coding: utf-8 -*-

from datetime import datetime
from itertools import combinations
from scrapy.spiders import CrawlSpider
from scrapy.exceptions import NotConfigured
from scrapy.utils.project import get_project_settings

class BaseSpider(CrawlSpider):
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
        
        super(BaseSpider, self).__init__()

    def filter_by_date(self, date):
        """ Check if the article's date is in the required range."""
        return date >= self.start_date and date <= self.end_date

    def filter_by_length(self, text):
        """ Check if the article's length meets minimum required length."""
        return len(text.split()) >= self.article_length

    def filter_by_keywords(self, text):
        """ 
        Check if any of the required keywords are found at least twice in the article.
        If true, check if the token distance between them meets the required minimum threshold.
        """
        tokens = text.lower().split()
        matching_positions = [tokens.index(token) for token in tokens if any(keyword in token for keyword in self.keywords)]

        if matching_positions and len(matching_positions) >= self.keywords_min_frequency:
            return any(abs(pos_1-pos_2) >= self.keywords_min_distance for (pos_1, pos_2) in list(combinations(matching_positions, 2)))
        return False

    def parse(self, response):
        pass
