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


class NeuesDeutschlandSpider(BaseSpider):
    """Spider for neues-deutschland"""
    name = 'neues_deutschland'
    rotate_user_agent = True
    allowed_domains = ['www.neues-deutschland.de']
    start_urls = ['https://www.neues-deutschland.de/']
    
    # Exclude pages without relevant articles 
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'www\.neues\-deutschland\.de\/artikel\/\d+\.\w.*\.html'),
                    deny=(r'www\.neues\-deutschland\.de\/shop\/',
                        r'www\.neues\-deutschland\.de\/leserreisen\/',
                        r'www\.neues\-deutschland\.de\/termine\/',
                        r'www\.neues\-deutschland\.de\/anzeigen\/',
                        r'www\.neues\-deutschland\.de\/jobs\/',
                        r'www\.neues\-deutschland\.de\/abo\/',
                        r'www\.neues\-deutschland\.de\/newsletter\/',
                        r'www\.neues\-deutschland\.de\/nd-ticker\/',
                        r'www\.neues\-deutschland\.de\/redaktion\/',
                        r'www\.neues\-deutschland\.de\/gastautoren\/',
                        r'www\.neues\-deutschland\.de\/kontakt\/',
                        r'www\.neues\-deutschland\.de\/tag\/',
                        r'www\.neues\-deutschland\.de\/nd_extra\/',
                        r'www\.neues\-deutschland\.de\/\w.*\.php'
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
        creation_date = datetime.strptime(creation_date, '%Y-%m-%d')
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//h2[preceding-sibling::h1] | //div[@class="Content"]/p')]
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
        authors = response.xpath('//meta[@name="author"]/@content').get()
        if authors:
            authors = authors.split(', ')
            item['author'] = [author for author in authors if len(author.split())>=2]
        else:
            item['author'] = list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        item['last_modified'] = creation_date.strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get()
        if '(neues deutschland)' in title:
            title = title.split(' (neues deutschland)')[0]
        description = response.xpath('//meta[@property="og:description"]/@content').get()

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h4[not(ancestor::div[@class="Wrap" or @class="ndPLUS-Abowerbung"])] | //h3[not(descendant::*)]'):
            # Extract headlines
            headlines = [h.xpath('string()').get() for h in response.xpath('//h4[not(ancestor::div[@class="Wrap" or @class="ndPLUS-Abowerbung"])] | //h3[not(descendant::*)]')]
            
            # Remove surrounding quotes from headlines
            processed_headlines = [headline.strip('"') for headline in headlines]
            processed_headlines = [headline.strip('“') for headline in processed_headlines]
          
            # If quote inside headline, keep substring from quote onwards
            processed_headlines = [headline[headline.rindex('"')+1:len(headline)] if '"' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.index('„')+1:len(headline)] if '„' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.rindex('“')+1:len(headline)] if '“' in headline else headline for headline in processed_headlines]

            # Extract paragraphs between the abstract and the first headline
            body[''] = [node.xpath('string()').get().strip() for node in response.xpath('//h2[preceding-sibling::h1] | //div[@class="Content"]/p[following-sibling::h4[contains(text(), "' + processed_headlines[0] + '")] or following-sibling::h3[contains(text(), "' + processed_headlines[0] + '")]]')]

            # Extract paragraphs corresponding to each headline, except the last one
            for i in range(len(headlines)-1):
                body[headlines[i]] = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="Content"]/p[(preceding-sibling::h4[contains(text(), "' + processed_headlines[i] + '")] or preceding-sibling::h3[contains(text(), "' + processed_headlines[i] + '")]) and (following-sibling::h4[contains(text(), "' + processed_headlines[i+1] +'")] or following-sibling::h3[contains(text(), "' + processed_headlines[i+1] +'")])]')]
           
            # Extract the paragraphs belonging to the last headline
            body[headlines[-1]] = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="Content"]/p[preceding-sibling::h4[contains(text(), "' + processed_headlines[-1] + '")] or preceding-sibling::h3[contains(text(), "' + processed_headlines[-1] + '")]]')]

        else:
            # The article has no headlines, just paragraphs
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract keywords
        keywords = response.xpath('//meta[@name="news_keywords"]/@content').get()
        item['keywords'] = keywords.split(', ') if keywords else list()
        
        # Extract the first 5 recommendations related to the article
        recommendations = response.xpath('//div[@id="List-Similar-Articles"]//a/@href').getall()
        if recommendations:
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
            recommendations = ['https://www.neues-deutschland.de' + rec for rec in recommendations]
            item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        # Save article in html format
        save_as_html(response, 'neues_deutschland.de', title)

        yield item
