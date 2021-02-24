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

        # Check if there are compound keywords (e.g. bedingungslos* einkommen*), and if so, separate single-token and multiple-token keywords
        self.compound_keywords = [keyword for keyword in self.keywords if len(keyword.split())>1]
        if self.compound_keywords:
            self.keywords = [keyword for keyword in self.keywords if not keyword in self.compound_keywords]

        self.keywords_combinations = list()

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
        if not self.keywords_combinations:
            return self._has_valid_keywords(tokens)
        else:
            return self._has_valid_combinations_keywords(tokens)
        

    def _has_valid_keywords(self, tokens):
        """
        Check if single or compound keywords appear in the given list of tokens and meet the validity requirements.

        param: tokens: list, article's body of text as list of tokens
        return: bool: True if keyword requirements met, False otherwise
        """
        # Extract matching positions and tokens
        matching_pos_tokens = [(tokens.index(token), token) for token in tokens if any(keyword in token for keyword in self.keywords)]

        # Extract matching positions and tokens for compound keyword stems (e.g. bedingungslos* einkommen*)
        compound_query_keywords = list()

        if self.compound_keywords:
            double_keywords = [keyword for keyword in self.compound_keywords if len(keyword.split())==2]
            triple_keywords = [keyword for keyword in self.compound_keywords if len(keyword.split())==3]

            if double_keywords:
                matching_double_pos_tokens = [(tokens.index(token), token, keyword) for token in tokens for keyword in double_keywords if ((keyword.split()[0] in token) and (keyword.split()[1] in tokens[tokens.index(token)+1]))]
                if matching_double_pos_tokens:
                    matching_double_pos = [pos for (pos, _, _) in matching_double_pos_tokens]
                
                    # Remove matches that might result from querying using both 'keywords' and 'double keywords'
                    matching_pos_tokens = [(pos, token) for (pos, token) in matching_pos_tokens if (pos-1) not in matching_double_pos]

                    # Add matches from compound keywords to all matches
                    matching_pos_tokens.extend(list(set([(pos, token) for (pos, token, _) in matching_double_pos_tokens])))

                    # Update used query compound keywords
                    compound_query_keywords.extend(list(set([keyword for (_, _, keyword) in matching_double_pos_tokens])))

            
            if triple_keywords:
                matching_triple_pos_tokens = [(tokens.index(token), token, keyword) for token in tokens for keyword in triple_keywords if ((keyword.split()[0] in token) and (keyword.split()[1] in tokens[tokens.index(token)+1]) and (keyword.split()[-1] in tokens[tokens.index(token)+2]))]
            
                if matching_triple_pos_tokens:
                    matching_triple_pos = [pos for (pos, _, _) in matching_triple_pos_tokens]
                
                    # Remove matches that might result from querying using both 'double keywords' and 'triple keywords'
                    matching_pos_tokens = [(pos, token) for (pos, token) in matching_pos_tokens if ((pos not in matching_triple_pos) and ((pos+1) not in matching_triple_pos))]

                    # Add matches from compound keywords to all matches
                    matching_pos_tokens.extend(list(set([(pos, token) for (pos, token, _) in matching_triple_pos_tokens])))

                    # Update used query compound keywords
                    compound_query_keywords.extend([keyword for (_, _, keyword) in matching_triple_pos_tokens])

        # Check if there are any query keyword stems in the text
        if matching_pos_tokens:
            matching_positions, matching_tokens = map(list, zip(*matching_pos_tokens))

            # Check the frequency of query keyword stems in the text
            if len(matching_positions) >= self.keywords_min_frequency:
                
                # Check the token difference between query keyword stems 
                if any(abs(pos_1-pos_2) >= self.keywords_min_distance for (pos_1, pos_2) in list(combinations(matching_positions, 2))):
                    
                    # Update the list of query keyword stems used
                    self.query_keywords = list(set(filter(lambda x: any(x in token for token in matching_tokens), self.keywords)))
                    if compound_query_keywords:
                        self.query_keywords.extend(list(set(compound_query_keywords)))
                    return True
        return False

    def _has_valid_combinations_keywords(self, tokens):
        pass


    def get_query_keywords(self):
        """ 
        return: list: the query keywords found in the article
        """
        return self.query_keywords

    def parse(self, response):
        pass
