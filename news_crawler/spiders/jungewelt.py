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


class JungeweltSpider(BaseSpider):
    """Spider for Junge Welt"""
    name = 'jungewelt'
    rotate_user_agent = True
    allowed_domains = ['www.jungewelt.de']
    start_urls = ['https://www.jungewelt.de/']
    
    # Exclude pages without relevant articles 
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'www\.jungewelt\.de\/artikel\/\w.*'),
                    deny=(r'www\.jungewelt\.de\/\w.*\/leserbriefe\.php$',
                        r'www\.jungewelt\.de\/verlag',
                        r'www\.jungewelt\.de\/ueber_uns\/',
                        r'www\.jungewelt\.de\/blogs\/',
                        r'www\.jungewelt\.de\/aktion\/',
                        r'www\.jungewelt\.de\/ladengalerie\/',
                        r'www\.jungewelt\.de\/termine\/',
                        r'www\.jungewelt\.de\/unterstuetzen\/',
                        r'www\.jungewelt\.de\/rlk\/',
                        r'www\.jungewelt\-shop\.de\/'
                        )
                    ),
                callback='parse_item',
                follow=True
                ),
            )

    def parse_item(self, response):
        """Scrapes information from pages into items"""

        # Exclude paid articles
        if response.xpath('//form[@action="/login.php"]').get():
            return
     
        # Filter by date
        creation_date = response.xpath('//meta[@name="dcterms.date"]/@content').get()
        if not creation_date or creation_date == '':
            return
        creation_date = datetime.strptime(creation_date, '%Y-%m-%d')
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//p[not(ancestor::div[@class="col-md-8 mx-auto mt-4 bg-light"]) and not(descendant::strong[contains(text(), "Unverzichtbar!")])]')]
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
        authors = response.xpath('//meta[@name="Author"]/@content').get()
        if authors:
            authors = authors.split(', ') if authors else list()
            # Remove location from the author's name, if included
            item['author'] = [author for author in authors if len(author.split())>=2]
        else:
            item['author'] = list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        item['last_modified'] = creation_date.strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get()
        description = response.xpath('//meta[@property="og:description"]/@content').get().split(' • ')[0]

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h3[not(@*) and not(ancestor::footer)]'):
            # Extract headlines
            headlines = [h3.xpath('string()').get() for h3 in response.xpath('//h3[not(@*) and not(ancestor::footer)]')]
            
            # Remove surrounding quotes from headlines
            processed_headlines = [headline.strip('"') for headline in headlines]
            processed_headlines = [headline.strip('“') for headline in processed_headlines]
          
            # If quote inside headline, keep substring from quote onwards
            processed_headlines = [headline[headline.rindex('"')+1:len(headline)] if '"' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.index('„')+1:len(headline)] if '„' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.rindex('“')+1:len(headline)] if '“' in headline else headline for headline in processed_headlines]

            # Extract paragraphs between the abstract and the first headline
            body[''] = [node.xpath('string()').get().strip() for node in response.xpath('//p[not(ancestor::div[@class="col-md-8 mx-auto mt-4 bg-light"]) and not(descendant::strong[contains(text(), "Unverzichtbar!")]) and following-sibling::h3[contains(text(), "' + processed_headlines[0] + '")]]')]

            # Extract paragraphs corresponding to each headline, except the last one
            for i in range(len(headlines)-1):
                body[headlines[i]] = [node.xpath('string()').get().strip() for node in response.xpath('//p[not(ancestor::div[@class="col-md-8 mx-auto mt-4 bg-light"]) and not(descendant::strong[contains(text(), "Unverzichtbar!")]) and preceding-sibling::h3[contains(text(), "' + processed_headlines[i] + '")] and following-sibling::h3[contains(text(), "' + processed_headlines[i+1] +'")]]')]
           
            # Extract the paragraphs belonging to the last headline
            body[headlines[-1]] = [node.xpath('string()').get().strip() for node in response.xpath('//p[not(ancestor::div[@class="col-md-8 mx-auto mt-4 bg-light"]) and not(descendant::strong[contains(text(), "Unverzichtbar!")]) and preceding-sibling::h3[contains(text(), "' + processed_headlines[-1] + '")]]')]

        else:
            # The article has no headlines, just paragraphs
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract keywords
        keywords = response.xpath('//meta[@name="keywords"]/@content').get()
        item['keywords'] = keywords.split(', ') if keywords else list()

        # Extract first 5 recommendations towards articles from the same news outlet, if available
        recommendations = response.xpath('//div[@id="similars"]/ul/li[not(contains(@class, "protected"))]//a[not(ancestor::h3)]/@href').getall()
        if recommendations:
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
            item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        # Save article in html format
        save_as_html(response, 'jungewelt.de', title)

        yield item
