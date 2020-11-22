# -*- coding: utf-8 -*-

import os
import sys
import dateparser
from datetime import datetime
from news_crawler.spiders import BaseSpider
from scrapy.spiders import Rule 
from scrapy.linkextractors import LinkExtractor

sys.path.insert(0, os.path.join(os.getcwd(), "..",))
from news_crawler.items import NewsCrawlerItem
from news_crawler.utils import remove_empty_paragraphs


class NachdenkseitenSpider(BaseSpider):
    """Spider for NachDenkSeiten"""
    name = 'nachdenkseiten'
    rotate_user_agent = True
    allowed_domains = ['www.nachdenkseiten.de']
    start_urls = ['https://www.nachdenkseiten.de/']
    
    # Exclude pages without relevant articles 
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'www\.nachdenkseiten\.de\/\?p=\d+$'),
                    deny=(r'www\.nachdenkseiten\.de\/foerdermitgliedschaft\/',
                        r'www\.nachdenkseiten\.de\/spenden\/',
                        r'www\.nachdenkseiten\.de\/\?page_id\=\d+',
                        r'www\.nachdenkseiten\.de\/\?cat\=\d+',
                        r'www\.nachdenkseiten\.de\/\?feed\=podcast',
                        r'www\.nachdenkseiten\.de\/\?p\=60958',
                        r'www\.nachdenkseiten\.de\/\?tag\=\w.*',
                        r'www\.nachdenkseiten\.de\/\?p\=\d+\&pdf\=\d+',
                        r'www\.nachdenkseiten\.de\/\?p=\d+\%.*'
                        )
                    ),
                callback='parse_item',
                follow=True
                ),
            )

    def parse_item(self, response):
        """Scrapes information from pages into items"""
     
        # Filter by date
        creation_date = response.xpath('//span[@class="postMeta"]/text()').get()
        if not creation_date:
            return
        creation_date = creation_date.split(' um')[0]
        creation_date = dateparser.parse(creation_date)
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="articleContent" or @class="footnote"]/p[not(contains(@class, "powerpress_links"))] | //blockquote/p')]
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
        authors = response.xpath('//span[@class="author"]/a/text()').getall()
        item['author'] = [author for author in authors if author != 'Redaktion'] if authors else list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        item['last_modified'] = creation_date.strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get()
        description = response.xpath('//meta[@property="og:description"]/@content').get()

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        # The article has no headlines, just paragraphs
        body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract keywords
        keywords = response.xpath('//a[@rel="tag"]/text()').getall()
        item['keywords'] = keywords if keywords else list()
        
        # No recommendations related to the article are available
        item['recommendations'] = list()

        # Save article in html format
        save_as_html(response, 'nachdenkseiten.de', title)

        yield item
