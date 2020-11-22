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


class MerkurSpider(BaseSpider):
    """Spider for Merkur"""
    name = 'merkur'
    rotate_user_agent = True
    allowed_domains = ['www.merkur.de']
    start_urls = ['https://www.merkur.de/']

    # Exclude English articles and pages without relevant articles 
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'www\.merkur\.de\/\w.*\.html$'),
                    deny=(r'www\.merkur\.de\/maerkte\/',
                        r'www\.merkur\.de\/wetter\/',
                        r'www\.merkur\.de\/ueber-uns\/',
                        r'www\.merkur\.de\/videos-fotostrecken\/',
                        r'www\.merkur\.de\/abo\/',
                        r'www\.merkur\.de\/auto\/verkehrsmeldungen\/',
                        r'promo\.merkur\.de\/',
                        r'autoanzeigen\.merkur\.de\/',
                        r'jobs\.merkur\.de\/',
                        r'trauer\.merkur\.de\/',
                        r'markt\.merkur\.de\/',
                        r'veranstaltungen\.merkur\.de\/',
                        r'weinshop\.merkur\.de\/'
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
        if not 'datePublished' in data.keys():
            return
        creation_date = data['datePublished']
        if not creation_date:
            return
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//p[contains(@class, "id-Article-content-item") and not(contains(@class, "summary")) and not(contains(@class, "copyright"))]')]
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
        authors = response.xpath('//meta[@property="lp.article:author"]/@content').getall()
        item['author'] = authors if authors else list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        last_modified = data['dateModified']
        item['last_modified'] = datetime.fromisoformat(last_modified.split('+')[0]).strftime('%d.%m.%Y') 
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get()
        description = response.xpath('//meta[@property="og:description"]/@content').get()

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h3/span[contains(@class, "id-Article-content-item-headline-text")]'):
            # Extract headlines
            headlines = [h3.xpath('string()').get().strip() for h3 in response.xpath('//h3/span[contains(@class, "id-Article-content-item-headline-text")]')]
            
            # Remove surrounding quotes from headlines
            processed_headlines = [headline.strip('"') for headline in headlines]
            processed_headlines = [headline.strip('“') for headline in processed_headlines]
          
            # If quote inside headline, keep substring from quote onwards
            processed_headlines = [headline[headline.rindex('"')+1:len(headline)] if '"' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.index('„')+1:len(headline)] if '„' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.rindex('“')+1:len(headline)] if '“' in headline else headline for headline in processed_headlines]

            # Extract paragraphs between the abstract and the first headline
            body[''] = [node.xpath('string()').get().strip() for node in response.xpath('//p[contains(@class, "id-Article-content-item") and not(contains(@class, "summary")) and not(contains(@class, "copyright")) and following-sibling::h3/span[contains(@class, "id-Article-content-item-headline-text") and contains(text(), "' + processed_headlines[0] + '")]]')]

            # Extract paragraphs corresponding to each headline, except the last one
            for i in range(len(headlines)-1):
                body[headlines[i]] = [node.xpath('string()').get().strip() for node in response.xpath('//p[contains(@class, "id-Article-content-item") and not(contains(@class, "summary")) and not(contains(@class, "copyright")) and preceding-sibling::h3/span[contains(@class, "id-Article-content-item-headline-text") and contains(text(), "' + processed_headlines[i] + '")] and following-sibling::h3/span[contains(@class, "id-Article-content-item-headline-text") and contains(text(), "' + processed_headlines[i+1] +'")]]')]
           
            # Extract the paragraphs belonging to the last headline
            body[headlines[-1]] = [node.xpath('string()').get().strip() for node in response.xpath('//p[contains(@class, "id-Article-content-item") and not(contains(@class, "summary")) and not(contains(@class, "copyright")) and preceding-sibling::h3/span[contains(@class, "id-Article-content-item-headline-text") and contains(text(), "' + processed_headlines[-1] + '")]]')]

        else:
            # The article has no headlines, just paragraphs
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # No keywords available
        item['keywords'] = list()
        
        # No recommendations related to the article are available
        item['recommendations'] = list()

        # Save article in html format
        save_as_html(response, 'merkur.de', title)

        yield item
