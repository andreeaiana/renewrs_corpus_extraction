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


class NtvSpider(BaseSpider):
    """Spider for NTV"""
    name = 'ntv'
    rotate_user_agent = True
    allowed_domains = ['www.n-tv.de']
    start_urls = ['https://www.n-tv.de/']

    # Excude pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'\w.*\/.*\-article\d+\.html'),
                    deny=(r'www\.n-tv\.de\/mediathek\/videos\/\w.*',
                        r'www\.n-tv\.de\/mediathek\/livestream\/\w.*',
                        r'www\.n-tv\.de\/mediathek\/tvprogramm\/',
                        r'www\.n-tv\.de\/mediathek\/magazine\/',
                        r'www\.n-tv\.de\/mediathek\/moderatoren\/',
                        r'www\.n-tv\.de\/mediathek\/teletext\/',
                        r'www\.tvnow\.de\/',
                        r'www\.n-tv\.de\/wetter\/',
                        r'www\.n-tv\.de\/boersenkurse\/',
                        r'www\.n-tv\.de\/wirtschaft\/der_boersen_tag\/',
                        r'www\.n-tv\.de\/sport\/der_sport_tag\/',
                        r'www\.n-tv\.de\/der_tag\/',
                        r'sportdaten\.n-tv\.de\/'
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
        if not creation_date:
            return
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get() for node in response.xpath('//div/p[not(contains(@class, "article__source")) and not(descendant::strong)]')]
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
        authors = response.xpath('//div//span[@class="article__author"]/text()').getall()
        if authors:
            item['author'] = [author.strip().split('Von ')[-1].rsplit(',')[0] for author in authors] 
        else:
            item['author'] = list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        last_modified = response.xpath('//meta[@name="last-modified"]/@content').get()
        item['last_modified'] = datetime.fromisoformat(last_modified.split('+')[0]).strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//title/text()').get().split(' - n-tv.de')[0] 
        description = response.xpath('//meta[@name="description"]/@content').get()
       
        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h2'):
           # Extract headlines
           headlines = [h2.xpath('string()').get() for h2 in response.xpath('//h2')]
           # Remove surrounding quotes from headlines
           processed_headlines = [headline.strip('"') for headline in headlines]
          
           # If quote inside headline, keep substring from quote onwards
           processed_headlines = [headline[headline.rindex('"')+1:len(headline)] if '"' in headline else headline for headline in processed_headlines]

           # Extract paragraphs between the abstract and the first headline
           body[''] = [node.xpath('string()').get() for node in response.xpath('//div/p[following-sibling::h2[contains(text(), "' + processed_headlines[0] + '")] and not(descendant::strong)]')]

           # Extract paragraphs corresponding to each headline, except the last one
           for i in range(len(headlines)-1):
               body[headlines[i]] = [node.xpath('string()').get() for node in response.xpath('//div/p[preceding-sibling::h2[contains(text(), "' + processed_headlines[i] + '")] and following-sibling::h2[contains(text(), "' + processed_headlines[i+1] +'")]]')]
           
           # Extract the paragraphs belonging to the last headline
           body[headlines[-1]] = [node.xpath('string()').get() for node in response.xpath('//div/p[preceding-sibling::h2[contains(text(), "' + processed_headlines[-1] + '")] and not(descendant::em) and not(contains(@class, "article__source"))]')]

        else:
            # The article has no headlines, just paragraphs
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract keywords, if available
        keywords = response.xpath('//meta[@name="news_keywords"]/@content').get()
        item['keywords'] = keywords.split(', ') if keywords else list()
        
        # No article-related recommendations
        item['recommendations'] = list()

        # Save article in htmk format
        save_as_html(response, 'ntv.de', title)

        yield item
