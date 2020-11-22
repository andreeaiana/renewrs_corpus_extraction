# -*- coding: utf-8 -*-

import re   
import os
import sys
from news_crawler.spiders import BaseSpider
from scrapy.spiders import Rule 
from scrapy.linkextractors import LinkExtractor
from datetime import datetime

sys.path.insert(0, os.path.join(os.getcwd(), "..",))
from news_crawler.items import NewsCrawlerItem
from news_crawler.utils import remove_empty_paragraphs


class TagesspiegelSpider(BaseSpider):
    """Spider for Tagesspiegel"""
    name = 'tagesspiegel'
    rotate_user_agent = True
    allowed_domains = ['www.tagesspiegel.de']
    start_urls = ['https://www.tagesspiegel.de/']
    
    # Exclude paid articles and pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'\/w.*\/.*\/\d+\.html$'),
                    deny=(r'plus\.tagesspiegel\.de',
                        r'tagesspiegel\.de\/service\/\w.*\.html$',
                        r'tagesspiegel\.de\/\w+\-\w+\/\d+\.html$',
                        r'tagesspiegel\.de\/dpa\/\d+\.html$',
                        r'tagesspiegel\.de\/mediacenter\/fotostrecken\/\w.*\/\d+\.html$',
                        r'vergleich\.tagesspiegel\.de\/',
                        r'verbraucher\.tagesspiegel\.de\/\w.*',
                        r'interaktiv\.tagesspiegel\.de\/',
                        r'gutscheine\.tagesspiegel\.de\/',
                        r'leserreisen\.tagesspiegel\.de\/',
                        r'jobs\.tagesspiegel\.de\/',
                        r'proptech\.tagesspiegel\.de\/'
                        )
                    ),
                callback='parse_item',
                follow=True
                ),
            )

    def parse_item(self, response):
        """Scrapes information from pages into items"""
        
        # Filter by date
        creation_date = response.xpath('//div/time[@itemprop="datePublished"]/@datetime').get()
        if not creation_date:
            return
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get() for node in response.xpath('//div[@itemprop="articleBody"]/p')]
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
        authors = response.xpath('//address/span/a[@rel="author"]/text()').getall()
        item['author'] = authors if authors else list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        # No last-modified date available 
        item['last_modified'] = creation_date.strftime('%d.%m.%Y') 
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//title/text()').get().split(' - ')[0] 
        description = response.xpath('//meta[@name="description"]/@content').get()
       
        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h3[not(contains(@class, "ts-title"))]'):
            # Extract headlines
            headlines = [h3.xpath('string()').get() for h3 in response.xpath('//h3')]
            
            # Remove surrounding quotes from headlines
            processed_headlines = [headline.strip('"') for headline in headlines]
            processed_headlines = [headline.strip('“') for headline in processed_headlines]
          
            # If quote inside headline, keep substring from quote onwards
            processed_headlines = [headline[headline.rindex('"')+1:len(headline)] if '"' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.index('„')+1:len(headline)] if '„' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.rindex('“')+1:len(headline)] if '“' in headline else headline for headline in processed_headlines]

            # Extract paragraphs between the abstract and the first headline
            body[''] = [node.xpath('string()').get() for node in response.xpath('//div/p[following-sibling::h3[contains(text(), "' + processed_headlines[0] + '")] and not(descendant::strong/span)]')]

            # Extract paragraphs corresponding to each headline, except the last one
            for i in range(len(headlines)-1):
                body[headlines[i]] = [node.xpath('string()').get() for node in response.xpath('//div/p[preceding-sibling::h3[contains(text(), "' + processed_headlines[i] + '")] and following-sibling::h3[contains(text(), "' + processed_headlines[i+1] +'")] and not(descendant::strong/span)]')]
           
            # Extract the paragraphs belonging to the last headline
            body[headlines[-1]] = [node.xpath('string()').get() for node in response.xpath('//div/p[preceding-sibling::h3[contains(text(), "' + processed_headlines[-1] + '")] and not(descendant::strong/span)]')]

        else:
            # The article has no headlines, just paragraphs
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract keywords, if available (from javascript)
        text = response.body.decode('utf-8') 
        pattern = re.compile('keywords\: \[\"\w.+\,.+\"\]')
        match = pattern.search(text)
        if match:
            keywords = text[match.start():match.end()]
            keywords = keywords.split('["')[1].rsplit('"]')[0].split(',')
            item['keywords'] = keywords
        else:
            item['keywords'] = list()
        
        # Extract first 5 recommendations towards articles from the same news outlet, if available
        recommendations = response.xpath('//article[@class="ts-teaser ts-type-article "]/a/@href').getall()
        if recommendations:
            recommendations = ['https://www.tagesspiegel.de' + rec for rec in recommendations]
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
            item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        # Save article in html format
        save_as_html(response, 'tagesspiegel.de', title)

        yield item
