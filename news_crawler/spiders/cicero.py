# -*- coding: utf-8 -*-

import os
import sys
import dateparser
from news_crawler.spiders import BaseSpider
from scrapy.spiders import Rule 
from scrapy.linkextractors import LinkExtractor
from datetime import datetime

sys.path.insert(0, os.path.join(os.getcwd(), "..",))
from news_crawler.items import NewsCrawlerItem
from news_crawler.utils import save_as_html


class CiceroSpider(BaseSpider):
    """Spider for Cicero"""
    name = 'cicero'
    rotate_user_agent = True
    allowed_domains = ['www.cicero.de']
    start_urls = ['https://www.cicero.de/']

    # Exclude paid and English articles and pages without relevant articles 
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'www\.cicero\.de\/\w.*'),
                    deny=(r'www\.cicero\.de\/cicero\-plus',
                        r'www\.cicero\.de\/newsletter\-anmeldung',
                        r'shop\.cicero\.de\/',
                        r'www\.cicero\.de\/rss\.xml$',
                        r'www\.cicero\.de\/comment\/\w.*'
                        )
                    ),
                callback='parse_item',
                follow=True
                ),
            )

    def parse_item(self, response):
        """Scrapes information from pages into items"""
    
        # Exclude paid articles
        if response.xpath('//div[@class="paywall-text"]').get():
            return

        # Filter by date
        metadata = response.xpath('//div[@class="teaser-small__metadata"]/p/text()').getall()
        if not metadata:
            return
        creation_date = metadata[-1].strip()
        if not creation_date:
            return
        creation_date = creation_date.split('am ')[-1]
        creation_date = dateparser.parse(creation_date)
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="field field-name-field-cc-body"]/p')]
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
        metadata = response.xpath('//div[@class="teaser-small__metadata"]/p//text()').getall()
        if not metadata:
            item['author'] = list()
        else:
            authors = metadata[1]
            if len(authors.split()) == 1:
                # No person listed as author
                item['author'] = list()
            elif len(authors.split()) == 2:
                item['author'] = [authors] if "CICERO" not in authors else list()
            elif 'UND' in authors:
                # Just 2 authors
                item['author'] = authors.split(' UND ')
            elif ',' in authors:
                # More than 2 authors
                authors = authors.split(', ')
                authors_list = authors[:-1]
                if 'UND' in authors[-1]:
                    authors_list.extend(authors[-1].split(' UND '))
                else:
                    authors_list.extend(authors[-1])
                item['author'] = authors_list
            else:
                item['author'] = list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        item['last_modified'] = creation_date.strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get()
        description = response.xpath('//meta[@property="og:description"]/@content').get()

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h3[not(contains(text(), "Kommentare"))]'):
            # Extract headlines
            headlines = [h3.xpath('string()').get().strip() for h3 in response.xpath('//h3[not(contains(text(), "Kommentare"))]')]
            
            # Remove surrounding quotes from headlines
            processed_headlines = [headline.strip('"') for headline in headlines]
            processed_headlines = [headline.strip('“') for headline in processed_headlines]
          
            # If quote inside headline, keep substring from quote onwards
            processed_headlines = [headline[headline.rindex('"')+1:len(headline)] if '"' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.index('„')+1:len(headline)] if '„' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.rindex('“')+1:len(headline)] if '“' in headline else headline for headline in processed_headlines]

            # Extract paragraphs between the abstract and the first headline
            body[''] = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="field field-name-field-cc-body"]/p[following-sibling::h3[contains(text(), "' + processed_headlines[0] + '")]]')]

            # Extract paragraphs corresponding to each headline, except the last one
            for i in range(len(headlines)-1):
                body[headlines[i]] = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="field field-name-field-cc-body"]/p[preceding-sibling::h3[contains(text(), "' + processed_headlines[i] + '")] and following-sibling::h3[contains(text(), "' + processed_headlines[i+1] +'")]]')]
           
            # Extract the paragraphs belonging to the last headline
            body[headlines[-1]] = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="field field-name-field-cc-body"]/p[preceding-sibling::h3[contains(text(), "' + processed_headlines[-1] + '")]]')]

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
        save_as_html(response, 'cicero.de', title)

        yield item
