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
from news_crawler.utils import save_as_html


class BildSpider(BaseSpider):
    """Spider for Bild"""
    name = 'bild'
    rotate_user_agent = True
    allowed_domains = ['www.bild.de']
    start_urls = ['https://www.bild.de/']
    
    # Exclude paid and English articles and pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'www\.bild\.de\/\w.*\.bild\.html$'),
                    deny=(r'www\.bild\.de\/\w+\/international\/\w.*\.bild\.html$',
                        r'www\.bild\.de\/bild-plus\/\w.*\.bild\.html$',
                        r'www\.bild\.de\/video\/mediathek\/\w.*',
                        r'www\.bild\.de\/video\/clip\/dokumentation\/\w.*',
                        r'www\.bild\.de\/bild-mobil\/audio\/podcast\/\w.*',
                        r'www\.bild\.de\/\w.*\-doku\-\w.*'
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
        if 'Z' in creation_date:
            if '.' in creation_date:
                creation_date = datetime.fromisoformat(creation_date.split('.')[0])
            else:
                creation_date = datetime.fromisoformat(creation_date[:-1])
        else:
            creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get() for node in response.xpath('//div[@class="txt" or @class="article-body"]/p')]
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
        authors = response.xpath('//div[@class="authors"]//span[@class="authors__name"]/text()').get()
        if authors:
            authors = authors.split(' UND ') if 'UND' in authors else authors
        else:
            authors = response.xpath('//div[@class="author"]//span[@class="author__name"]/text()').get()
            authors = [authors] if authors else authors
        item['author'] = authors if authors else list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        last_modified = data['dateModified']
        if 'Z' in last_modified:
            if '.' in last_modified:
                item['last_modified'] = datetime.fromisoformat(last_modified.split('.')[0]).strftime('%d.%m.%Y')
            else:
                item['last_modified'] = datetime.fromisoformat(last_modified[:-1]).strftime('%d.%m.%Y')
        else:
             item['last_modified'] = datetime.fromisoformat(last_modified.split('+')[0]).strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get().strip()
        description = response.xpath('//meta[@property="og:description"]/@content').get().strip()

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h2[@class="crossheading"]'):
            # Extract headlines
            headlines = [h2.xpath('string()').get().strip() for h2 in response.xpath('//h2[@class="crossheading"]')]
            
            # Remove surrounding quotes from headlines
            processed_headlines = [headline.strip('“') for headline in headlines]
          
            # If quote inside headline, keep substring from quote onwards
            processed_headlines = [headline[headline.index('„')+1:len(headline)] if '„' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.rindex('“')+1:len(headline)] if '“' in headline else headline for headline in processed_headlines]

            # Extract paragraphs between the abstract and the first headline
            body[''] = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="txt" or @class="article-body"]/p[following-sibling::h2[contains(text(), "' + processed_headlines[0] + '")]]')]

            # Extract paragraphs corresponding to each headline, except the last one
            for i in range(len(headlines)-1):
                body[headlines[i]] = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="txt" or @class="article-body"]/p[preceding-sibling::h2[contains(text(), "' + processed_headlines[i] + '")] and following-sibling::h2[contains(text(), "' + processed_headlines[i+1] +'")]]')]
           
            # Extract the paragraohs belonging to the last headline
            body[headlines[-1]] = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="txt" or @class="article-body"]/p[preceding-sibling::h2[contains(text(), "' + processed_headlines[-1] + '")]]')]

        else:
            # The article has no headlines, just paragraphs
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract keywords
        keywords = response.xpath('//meta[@name="keywords"]/@content').get()
        if keywords:
            item['keywords'] = keywords.split(', ') if ', ' in keywords else keywords.split(',')
        else:
            item['keywords'] = list()
        
        # Extract first 5 recommendations towards articles from the same news outlet, if available
        recommendations = response.xpath('//div[@class="related-topics__container"]/article/a/@href').getall()
        if not recommendations:
            recommendations = response.xpath('//div[descendant::h3[contains(text(), "Lesen Sie auch")]]/ul/li//a/@href').getall()
            recommendations = ['https://www.bild.de' + rec for rec in recommendations]
        if recommendations:    
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
            item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        # Save article in htmk format
        save_as_html(response, 'bild.de', title)
        
        yield item
