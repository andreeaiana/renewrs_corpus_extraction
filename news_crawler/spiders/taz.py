# -*- coding: utf-8 -*-

import os
import sys
from news_crawler.spiders import BaseSpider
from scrapy.spiders import Rule 
from scrapy.linkextractors import LinkExtractor
from datetime import datetime

sys.path.insert(0, os.path.join(os.getcwd(), "..",))
from news_crawler.items import NewsCrawlerItem
from news_crawler.utils import remove_empty_paragraphs


class TazSpider(BaseSpider):
    """Spider for Taz"""
    name = 'taz'
    rotate_user_agent = True
    allowed_domains = ['taz.de']
    start_urls = ['https://taz.de/']

    # Exclude English articles and pages without relevant articles 
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'taz\.de\/\w.*\/\!\d+\/',
                        r'taz\.de\/\!\d+\/'
                        ),
                    deny=(r'taz\.de\/English-Version\/\!\d+\/',
                        r'taz\.de\/taz-in-English\/\!\w.*\/',
                        r'taz\.de\/\!p4697\/',
                        r'taz\.de\/\!p4209\/',
                        r'taz\.de\/\!p4791\/',
                        r'taz\.de\/Info\/',
                        r'taz\.de\/Anzeigen\/',
                        r'taz\.de\/Podcast\/',
                        r'taz\.de\/Hilfe\/',
                        r'shop\.taz\.de\/',
                        r'taz\.de\/eKiosk-AGB\/',
                        r'taz\.de\/AGB',
                        r'taz\.de\/Print\-am\-Wochenende.*',
                        r'taz\.de\/FAQ.*'
                        )
                    ),
                callback='parse_item',
                follow=True
                ),
            )

    def parse_item(self, response):
        """Scrapes information from pages into items"""
     
        # Filter by date
        creation_date = response.xpath('//li[@class="date" and @itemprop="datePublished"]/@content').get()
        if not creation_date:
            return
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//p[@xmlns="" and (@class="article first odd Initial" or @class="article first odd" or @class="article odd" or @class="article even" or @class="article last odd" or @class="article last even")]')]
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
        authors = response.xpath('//meta[@name="author"]/@content').getall()
        item['author'] = [author for author in authors if 'taz' not in author] if authors else list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        last_modified = response.xpath('//meta[@itemprop="dateModified"]/@content').get()
        item['last_modified'] = datetime.fromisoformat(last_modified.split('+')[0]).strftime('%d.%m.%Y') 
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get()
        description = response.xpath('//meta[@property="og:description"]/@content').get()

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h6[@xmlns=""]'):
            # Extract headlines
            headlines = [h6.xpath('string()').get() for h6 in response.xpath('//h6[@xmlns=""]')]
            
            # Remove surrounding quotes from headlines
            processed_headlines = [headline.strip('"') for headline in headlines]
            processed_headlines = [headline.strip('“') for headline in processed_headlines]
          
            # If quote inside headline, keep substring from quote onwards
            processed_headlines = [headline[headline.rindex('"')+1:len(headline)] if '"' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.index('„')+1:len(headline)] if '„' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.rindex('“')+1:len(headline)] if '“' in headline else headline for headline in processed_headlines]

            # Extract paragraphs between the abstract and the first headline
            body[''] = [node.xpath('string()').get().strip() for node in response.xpath('//p[@xmlns="" and (@class="article first odd Initial" or @class="article first odd" or @class="article odd" or @class="article even" or @class="article last odd" or @class="article last even") and following-sibling::h6[contains(text(), "' + processed_headlines[0] + '")]]')]

            # Extract paragraphs corresponding to each headline, except the last one
            for i in range(len(headlines)-1):
                body[headlines[i]] = [node.xpath('string()').get().strip() for node in response.xpath('//p[@xmlns="" and (@class="article first odd Initial" or @class="article first odd" or @class="article odd" or @class="article even" or @class="article last odd" or @class="article last even") and preceding-sibling::h6[contains(text(), "' + processed_headlines[i] + '")] and following-sibling::h6[contains(text(), "' + processed_headlines[i+1] +'")]]')]
           
            # Extract the paragraphs belonging to the last headline
            body[headlines[-1]] = [node.xpath('string()').get().strip() for node in response.xpath('//p[@xmlns="" and (@class="article first odd Initial" or @class="article first odd" or @class="article odd" or @class="article even" or @class="article last odd" or @class="article last even") and preceding-sibling::h6[contains(text(), "' + processed_headlines[-1] + '")]]')]

        else:
            # The article has no headlines, just paragraphs
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract keywords
        keywords = response.xpath('//meta[@name="keywords"]/@content').get()
        item['keywords'] = keywords.split(', ') if keywords else list()
        
        # No recommendations related to the article are available
        item['recommendations'] = list()

        # Save article in html format
        save_as_html(response, 'taz.de', title)

        yield item
