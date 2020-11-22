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


class TagesschauSpider(BaseSpider):
    """Spider for Tagesschau"""
    name = 'tagesschau'
    rotate_user_agent = True
    allowed_domains = ['www.tagesschau.de']
    start_urls = ['https://www.tagesschau.de/']
    
    # Exclude pages without relevant articles 
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'www\.tagesschau\.de\/\w+\/\w.*\.html$'),
                    deny=(r'www\.tagesschau\.de\/multimedia\/\w.*\.html$',
                        r'wetter\.tagesschau\.de\/',
                        r'meta\.tagesschau\.de\/',
                        r'www\.tagesschau\.de\/mehr\/\w.*',
                        r'www\.tagesschau\.de\/hilfe\/\w.*',
                        r'www\.tagesschau\.de\/impressum\/',
                        r'www\.tagesschau\.de\/kontakt_und_hilfe\/\w.*',
                        r'www\.tagesschau\.de\/sitemap\/',
                        r'www\.tagesschau\.de\/app\/',
                        r'www\.tagesschau\.de\/atlas\/',
                        r'www\.tagesschau\.de\/allemeldungen\/',
                        r'intern\.tagesschau\.de\/'
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
        if 'datePublished' not in data.keys():
            return
        creation_date = data['datePublished']
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//p[@class="text small" and not(descendant::strong)]')]
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
        authors = data['author']['name']
        if authors and authors != 'tagesschau':
            authors = authors.split(', ')[0]
            if 'und' in authors:
                authors = authors.split(' und ')
            else:
                authors = [authors]
            item['author'] = authors
        else:
            item['author'] = list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        last_modified = data['dateModified']
        item['last_modified'] = datetime.fromisoformat(last_modified.split('+')[0]).strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//title/text()').get().split(' |')[0]
        description = response.xpath('//meta[@name="description"]/@content').get().strip()
       
        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h2[@class="subtitle small "]'):
            # Extract headlines
            headlines = [h2.xpath('string()').get().strip() for h2 in response.xpath('//h2[@class="subtitle small "]')]
            
            # Remove surrounding quotes from headlines
            processed_headlines = [headline.strip('"') for headline in headlines]
            processed_headlines = [headline.strip('“') for headline in processed_headlines]
          
            # If quote inside headline, keep substring from quote onwards
            processed_headlines = [headline[headline.rindex('"')+1:len(headline)] if '"' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.index('„')+1:len(headline)] if '„' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.rindex('“')+1:len(headline)] if '“' in headline else headline for headline in processed_headlines]

            # Extract paragraphs between the abstract and the first headline
            body[''] = [node.xpath('string()').get().strip() for node in response.xpath('//div/p[@class="text small" and not(descendant::strong) and following-sibling::h2[contains(text(), "' + processed_headlines[0] + '")] or following-sibling::h2/strong[contains(text(), "' + processed_headlines[0] + '")]]')]

            # Extract paragraphs corresponding to each headline, except the last one
            for i in range(len(headlines)-1):
                body[headlines[i]] = [node.xpath('string()').get().strip() for node in response.xpath('//div/p[@class="text small" and not(descendant::strong) and (preceding-sibling::h2[contains(text(), "' + processed_headlines[i] + '")] or preceding-sibling::h2/strong[contains(text(), "' + processed_headlines[i] + '")]) and (following-sibling::h2[contains(text(), "' + processed_headlines[i+1] +'")] or following-sibling::h2/strong[contains(text(), "' + processed_headlines[i+1] +'")])]')]
           
            # Extract the paragraphs belonging to the last headline
            body[headlines[-1]] = [node.xpath('string()').get().strip() for node in response.xpath('//div/p[@class="text small" and not(descendant::strong) and preceding-sibling::h2[contains(text(), "' + processed_headlines[-1] + '")] or preceding-sibling::h2/strong[contains(text(), "' + processed_headlines[-1] + '")]]')]

        else:
            # The article has no headlines, just paragraphs
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract keywords
        keywords = response.xpath('//meta[@name="news_keywords"]/@content').get()
        item['keywords'] = keywords.split(', ') if keywords else list()
        
        # Extract first 5 recommendations towards articles from the same news outlet, if available
        
        # Extract from categories "Mehr zum Thema"
        mehr_zum_thema_h3 = response.xpath('//div[preceding-sibling::h3[contains(text(), "Mehr zum Thema")]]/div/ul/li/a/@href').getall() 
        if mehr_zum_thema_h3:
            mehr_zum_thema_h3 = ['https://www.tagesschau.de' + rec for rec in mehr_zum_thema_h3]

        mehr_zum_thema_h4 = response.xpath('//div[preceding-sibling::h4[contains(text(), "Mehr zum Thema")]]/ul/li/a/@href').getall() 
        if mehr_zum_thema_h4:
            mehr_zum_thema_h4 = ['https://www.tagesschau.de' + rec for rec in mehr_zum_thema_h4]
        
        # Extract from categories "Aus dem Archiv"
        aus_dem_archiv = response.xpath('//div[preceding-sibling::h3[contains(text(), "Aus dem Archiv")]]/div/ul/li/a/@href').getall() 
        if aus_dem_archiv:
            aus_dem_archiv = ['https://www.tagesschau.de' + rec for rec in aus_dem_archiv]

        recommendations = mehr_zum_thema_h3 + mehr_zum_thema_h4 + aus_dem_archiv
        if recommendations:
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
            item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        # Save article in html format
        save_as_html(response, 'tagesschau.de', title)

        yield item
