from bs4 import BeautifulSoup
import dateutil.parser as date_parser
import re

class ArticleValidator:
    def __init__(self, article, config):
        self.article = article
        self.config = config

    def is_out_of_date(self, article):
        from_date = date_parser.parse(self.config["from_date"])
        to_date = date_parser.parse(self.config["to_date"])
        pub_date = date_parser.parse(article["creation_date"])
        if pub_date >= from_date and pub_date <= to_date:
            return False
        else:
            return True

    def keyword_count(self, whole_text, query):
        keyword_count = 0
        return len(list(re.finditer(query, ".".join(whole_text), re.IGNORECASE)))

    def is_valid_inter_keyword_length(self, whole_text, query):
        keyword_locs = []
        for match_obj in re.finditer(query, ".".join(whole_text), re.IGNORECASE):
            keyword_locs.append(match_obj.start())
        #max distance btw keywords
        if max(keyword_locs) - min(keyword_locs) >= self.config['min_length_btw_keywords']:
            return True
        else:
            return False

    def is_valid_article(self, article, query, whole_text):
        title = article["content"]
        title = title["title"]
        if self.is_out_of_date(article):
            print("Out of date: ", title)
            return False
        elif self.keyword_count(whole_text, query) < self.config['min_keyword_occurrence']:
            print("Keyword count failure: ", title)
            return False
        elif not self.is_valid_inter_keyword_length(whole_text, query):
            print("Invalid inter keyword length: ", title)
            return False
        else:
            return True
