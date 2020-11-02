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

class SueddeutscheSpider(BaseSpider):
    """Spider for Sueddeutsche"""
    name = 'sueddeutsche'
    rotate_user_agent = True
    allowed_domains = ['www.sueddeutsche.de']
    start_urls = ['https://www.sueddeutsche.de/']
 
    # Exclude paid articles
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'(\/\w+)*.*\.\d+$'),
                    deny=(r'(\/\w+)*\/.*\?reduced=true', 
                        r'.*\/autoren\/.*'
                        )
                    ),
                callback='parse',
                follow=True
                ),
            )


    def parse(self, response):
        """Scrapes information from pages into items"""
       
        data_json = response.xpath('//script[@type="application/ld+json"]/text()').get()
        if not data_json:
            # The page does not contain an article
            return
        data = json.loads(data_json)
        
        # Filter by date
        creation_date = data['datePublished']
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get() for node in response.xpath('//p[@class=" css-0"]')]
        text = ' '.join([para for para in paragraphs if para != ' '])

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
        if response.xpath('//h3[not(@*)]'):
            headline_no = 0
            for h3 in response.xpath('//h3[not(@*)]'):
                # Extract first paragraphs, between title and first headine
                if headline_no == 0:
                    paragraphs = [node.xpath('string()').get() for node in h3.xpath('./preceding-sibling::p')]
                    body[''] = paragraphs
                
                # Extract headline and corresponding paragrphs
                headline = h3.xpath('string()').get()
                paragraphs = [node.xpath('string(').get() for node in h3.xpath('./following-sibling::p')]
                body[headline] = paragraphs
                headline_no += 1
        else:
            # The article has no headlines, just paragraphs
            paragraphs = [node.xpath('string()').get() for node in response.xpath('//p[@class=" css-0"]')]
            body[''] = [para for para in paragraphs if para != ' ']

        item['content'] = {'title': title, 'description': description, 'body':body}

        # Extract keywords, if available
        keywords = response.xpath('//meta[@name="keywords"]/@content').get()
        item['keywords'] = keywords.split(',') if keywords else list()
       
        # Extract first 5 recommendations towards articles from the same news outlet
        recommendations = response.xpath('//aside[@id="more-on-the-subject"]//a/@href').getall()
        if recommendations:
            recommendations = [rec for rec in recommendations if not '?reduced=true' in rec]
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
            item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        # Save article in htmk format
        save_as_html(response, 'sueddeutsche.de')

        yield item
