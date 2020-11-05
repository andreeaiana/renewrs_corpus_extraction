# -*- coding: utf-8 -*-

import os
import sys
from news_crawler.spiders import BaseSpider
from scrapy.spiders import Rule 
from scrapy.linkextractors import LinkExtractor
from datetime import datetime

sys.path.insert(0, os.path.join(os.getcwd(), "..",))
from news_crawler.items import NewsCrawlerItem
from news_crawler.utils import save_as_html


class JungleSpider(BaseSpider):
    """Spider for jungle.world"""
    name = 'jungle'
    rotate_user_agent = True
    allowed_domains = ['jungle.world']
    start_urls = ['https://jungle.world/']
    
    # Exclude pages without relevant articles 
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'jungle\.world\/artikel\/.*'),
                    deny=(r'jungle\.world\/abo')
                    ),
                callback='parse_item',
                follow=True
                ),
            )

    def parse_item(self, response):
        """Scrapes information from pages into items"""

        # Exclude paid articles
        if 'Anmeldung erforderlich' in response.xpath('//meta[@name="dcterms.title"]/@content').get():
            return

        # Filter by date
        creation_date = response.xpath('//div/span[@class="date"]/text()').get()
        if not creation_date:
            return
        creation_date = creation_date.strip()
        if creation_date == '':
            return
        creation_date = datetime.strptime(creation_date, '%d.%m.%Y')
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="lead"] | //p[not(ancestor::div[@class="caption"]) and not(descendant::a[@class="btn btn-default scrollTop"])]')]
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
        authors = response.xpath('//meta[@name="dcterms.publisher"]/@content').get()
        item['author'] = authors.split(', ') if authors else list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        item['last_modified'] = creation_date.strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get()
        description = response.xpath('//meta[@property="og:description"]/@content').get().split(' • ')[0]

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        # The articles have no headlines, just paragraphs
        body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract keywords
        keywords = response.xpath('//meta[@name="keywords"]/@content').get()
        item['keywords'] = keywords.split(', ') if keywords else list()

        # No recommendations related to the current article available
        item['recommendations'] = list()

        # Save article in html format
        save_as_html(response, 'jungle.world', title)

        yield item
