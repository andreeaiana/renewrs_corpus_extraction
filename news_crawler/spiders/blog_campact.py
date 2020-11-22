# -*- coding: utf-8 -*-

import os
import sys
import json
from news_crawler.spiders import BaseSpider
from scrapy.spiders import Rule 
from scrapy.linkextractors import LinkExtractor
from datetime import datetime

sys.path.insert(0, os.path.join(os.getcwd(), "..",))
from news_crawler.items import NewsCrawlerItem
from news_crawler.utils import remove_empty_paragraphs


class BlogCampactSpider(BaseSpider):
    """Spider for blog.campact"""
    name = 'blog_campact'
    rotate_user_agent = True
    allowed_domains = ['blog.campact.de']
    start_urls = ['https://blog.campact.de/']
    
    # Exclude pages without relevant articles 
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'blog\.campact\.de\/\w.*'),
                    deny=(r'blog\.campact\.de\/page\/\d+\/',
                        r'www\.campact\.de\/\w.*',
                        r'weact\.campact\.de\/',
                        r'support\.campact\.de\/',
                        r'aktion\.campact\.de\/\w.*',
                        r'blog\.campact\.de\/\w.*\/comment\-page\-\d+\/\#comments'
                        )
                    ),
                callback='parse_item',
                follow=True
                ),
            )

    def parse_item(self, response):
        """Scrapes information from pages into items"""
     
        data_json = response.xpath('//script[@type="application/ld+json"]/text()').get()
        if not data_json:
            return
        data = json.loads(data_json)

        # Filter by date
        if not 'datePublished' in data['@graph'][2].keys():
            return
        creation_date = data['@graph'][2]['datePublished']
        if not creation_date:
            return
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//p[not(contains(@class, "news-header__excerpt")) and not(contains(@class, "form-error margin--bottom")) and not(@class="footer__text") and not(ancestor::div[@class="comments__item-content-container" or @class="author__content"]) and not(preceding-sibling::h5) and not(ancestor::div[@class="comment-respond"])] | //section[@class="text"]/span | //section[@class="text"]/b')]
        text = ' '.join([para for para in paragraphs if para != ' ' and para != ""])

        # Filter by article length
        if not self.filter_by_length(text):
            return

        # Filter by keywords
        if not self.filter_by_keywords(text):
            return

        # Parse the article
        item = NewsCrawlerItem()
        item['provenance'] = response.url
        
        # Get authors
        authors = response.xpath('//div[@class="author"]//a[@class="author__meta-info-author"]/text()').getall()
        item['author'] = authors if authors else list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        last_modified = data['@graph'][2]['dateModified']
        item['last_modified'] = datetime.fromisoformat(last_modified.split('+')[0]).strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get()
        description = response.xpath('//meta[@property="og:description"]/@content').get()

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h2[not(@*)] | //h4'):

            # Extract headlines
            headlines = [h.xpath('string()').get() for h in response.xpath('//h2[not(@*)] | //h4')]

            # Extract the paragraphs and headlines together
            text = [node.xpath('string()').get().strip() for node in response.xpath('//p[not(contains(@class, "news-header__excerpt")) and not(contains(@class, "form-error margin--bottom")) and not(@class="footer__text") and not(ancestor::div[@class="comments__item-content-container" or @class="author__content"]) and not(preceding-sibling::h5) and not(ancestor::div[@class="comment-respond"])] | //section[@class="text"]/span | //section[@class="text"]/b | //h2[not(@*)] | //h4')]
          
            # Extract paragraphs between the abstract and the first headline
            body[''] = text[:text.index(headlines[0])]

            # Extract paragraphs corresponding to each headline, except the last one
            for i in range(len(headlines)-1):
                body[headlines[i]] = text[text.index(headlines[i])+1:text.index(headlines[i+1])]

            # Extract the paragraphs belonging to the last headline
            body[headlines[-1]] = text[text.index(headlines[-1])+1:]

        else:
            # The article has no headlines, just paragraphs
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # No keywords available
        item['keywords'] = list()
        
        # No recommendations related to the article are available
        item['recommendations'] = list()

        # Save article in html format
        save_as_html(response, 'blog_campact.de', title)

        yield item 
