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


class WeltSpider(BaseSpider):
    """Spider for Welt"""
    name = 'welt'
    rotate_user_agent = True
    allowed_domains = ['www.welt.de']
    start_urls = ['https://www.welt.de/']
    
    # Exclude paid articles and articles in English
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'(\/\w+)*\/article\d+\/.*\.html'),
                    deny=(r'(\/\w+)*\/plus\d+\/.*\.html',
                        r'(\/english-news)\/article\d+\/.*\.html'
                        )
                    ),
                callback='parse_item',
                follow=True
                ),
            )

    def parse_item(self, response):
        """Scrapes information from pages into items"""
        
        # Filter by date
        creation_date = response.xpath('//meta[@name="date"]/@content').get()
        creation_date = datetime.fromisoformat(creation_date[:-1])
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get() for node in response.xpath('//p[not(ancestor::div/@class="c-page-footer__section")]')]
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
        authors = response.xpath('//div/span[@class="c-author__by-line"]/a/text()').getall()
        item['author'] = authors if authors else list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        last_modified = response.xpath('//meta[@name="last-modified"]/@content').get()
        item['last_modified'] = datetime.fromisoformat(last_modified[:-1]).strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//title/text()').get().split(' - WELT')[0] 
        description = response.xpath('//meta[@name="description"]/@content').get()
       
        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h3'):
            headline_no = 0
            for h3 in response.xpath('//h3'):
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
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract keywords, if available
        keywords = response.xpath('//meta[@name="news_keywords"]/@content').get()
        item['keywords'] = keywords.split(', ') if keywords else list()
        
        # Extract first 5 recommendations towards articles from the same news outlet, if available
        recommendations = response.xpath('//li//div/h4/a[@name="morelikethis_a_free_"]/@href').getall()
        if recommendations:
            recommendations = ['welt.de' + rec for rec in recommendations]
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
            item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        # Save article in htmk format
        save_as_html(response, 'welt.de')

        yield item
