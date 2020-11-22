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


class FocusSpider(BaseSpider):
    """Spider for Focus"""
    name = 'focus'
    rotate_user_agent = True
    allowed_domains = ['www.focus.de']
    start_urls = ['https://www.focus.de/']

    # Exclude pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'focus\.de\/\w.*\.html$'),
                    deny=(r'www\.focus\.de\/service\/',
                        r'www\.focus\.de\/focustv\/',
                        r'www\.focus\.de\/videos\/',
                        r'www\.focus\.de\/\w+\/videos\/',
                        r'www\.focus\.de\/shopping\/',
                        r'www\.focus\.de\/schlagzeilen\/',
                        r'www\.focus\.de\/deals\/',
                        r'www\.focus\.de\/panorama\/lotto\/',
                        r'www\.focus\.de\/gesundheit\/lexikon\/',
                        r'www\.focus\.de\/gesundheit\/testcenter\/',
                        r'www\.focus\.de\/wissen\/natur\/meteorologie\/',
                        r'www\.focus\.de\/finanzen\/boerse\/robo',
                        r'gutscheine\.focus\.de\/',
                        r'kleinanzeige\.focus\.de\/',
                        r'nl\.focus\.de\/',
                        r'praxistipps\.focus\.de\/',
                        r'vergleich\.focus\.de\/',
                        r'www\.focus\.de\/intern\/',
                        r'www\.focus\.de\/finanzen\/focus\-online\-kooperationen\-services\-vergleiche\-rechner\_id'
                        )
                    ),
                callback='parse_item',
                follow=True
                ),
            )

    def parse_item(self, response):
        """Scrapes information from pages into items"""
    
        json_data = response.xpath('//script[@type="application/ld+json"]/text()').get()
        if not json_data:
            return
        data = json.loads(json_data)

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
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="textBlock"]/p[not(contains(@class, "noads")) and not(descendant::em[contains(text(), "Lesen Sie auch")])]')]
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
        authors = response.xpath('//div[@class="authorMeta"]/span/a/text()').getall()
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
        if response.xpath('//h2[not(contains(@class, "mm-h2"))]'):

           # Extract headlines
           headlines = [h2.xpath('string()').get() for h2 in response.xpath('//h2[not(contains(@class, "mm-h2"))]')]

           # Extract the paragraphs and headlines together
           text = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="textBlock"]/p[not(contains(@class, "noads")) and not(descendant::em[contains(text(), "Lesen Sie auch")])] | //h2[not(contains(@class, "mm-h2"))]')]
          
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
        
        # No article-related recommendations
        item['recommendations'] = list()

        # Save article in html format
        save_as_html(response, 'focus.de', title)

        yield item
