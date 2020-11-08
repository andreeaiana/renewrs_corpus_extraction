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


class SpiegelSpider(BaseSpider):
    """Spider for Spiegel"""
    name = 'spiegel'
    rotate_user_agent = True
    allowed_domains = ['www.spiegel.de']
    start_urls = ['https://www.spiegel.de/']
    
    # Exclude articles in English 
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'spiegel\.de\/\w.*$'),
                    deny=(r'spiegel\.de\/international\/\w.*$',
                        r'www\.spiegel\.de\/audio\/',
                        r'www\.spiegel\.de\/plus\/',
                        r'www\.spiegel\.de\/thema\/mobilitaet-videos\/',
                        r'www\.spiegel\.de\/thema\/podcasts',
                        r'www\.spiegel\.de\/thema\/audiostorys\/',
                        r'www\.spiegel\.de\/thema\/spiegel-update\/',
                        r'www\.spiegel\.de\/thema\/spiegel-tv\/',
                        r'www\.spiegel\.de\/thema\/bundesliga_experten\/',
                        r'www\.spiegel\.de\/video\/',
                        r'www\.spiegel\.de\/newsletter',
                        r'www\.spiegel\.de\/services',
                        r'sportdaten\.spiegel\.de\/',
                        r'spiele\.spiegel\.de\/',
                        r'akademie\.spiegel\.de\/',
                        r'jobs\.spiegel\.de\/',
                        r'immobilienbewertung\.spiegel\.de\/',
                        r'sportwetten\.spiegel\.de\/',
                        r'ed\.spiegel\.de\/',
                        r'www\.spiegel\.de\/lebenundlernen\/schule\/ferien-schulferien-und-feiertage-a-193925\.html',
                        r'www\.spiegel\.de\/dienste\/besser-surfen-auf-spiegel-online-so-funktioniert-rss-a-1040321\.html',
                        r'www\.spiegel\.de\/gutscheine\/',
                        r'www\.spiegel\.de\/impressum',
                        r'www\.spiegel\.de\/kontakt',
                        r'www\.spiegel\.de\/nutzungsbedingungen',
                        r'www\.spiegel\.de\/datenschutz-spiegel',
                        r'www\.spiegel-live\.de\/'
                        )
                    ),
                callback='parse_item',
                follow=True
                ),
            )

    def parse_item(self, response):
        """Scrapes information from pages into items"""
       
        # Exclude paid articls (i.e. SpiegelPlus)
        if response.xpath('//span[@class="flex-shrink-0 leading-none"]').get():
            return

        # Filter by date
        creation_date = response.xpath('//meta[@name="date"]/@content').get()
        if not creation_date:
            return
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//div[contains(@class, "RichText RichText--iconLinks")]/p')]
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
        item['author'] = authors.split(', DER SPIEGEL')[0].split(', ') if authors else list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        last_modified = response.xpath('//meta[@name="last-modified"]/@content').get()
        item['last_modified'] = datetime.fromisoformat(last_modified.split('+')[0]).strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get().split(' - DER SPIEGEL')[0]
        description = response.xpath('//meta[@property="og:description"]/@content').get()
       
        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h3'):
            # Extract headlines
            headlines = [h3.xpath('string()').get() for h3 in response.xpath('//h3')]

            # Extract the paragraphs and headlines together
            text = [node.xpath('string()').get().strip() for node in response.xpath('//div[contains(@class, "RichText RichText--iconLinks")]/p | //h3')]
          
            # Extract paragraphs between the abstract and the first headline
            body[''] = text[:text.index(headlines[0])]

            # Extract paragraphs corresponding to each headline, except the last one
            for i in range(len(headlines)-1):
                body[headlines[i]] = text[text.index(headlines[i])+1:text.index(headlines[i+1])]

            # Extract the paragraphs belonging to the last headline
            body[headlines[-1]] = text[text.index(headlines[-1])+1:]

        else:
            # The article has no headlines, just paragraphs
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract keywords, if available 
        keywords = response.xpath('//meta[@name="news_keywords"]/@content').get()
        item['keywords'] = keywords.split(', ') if keywords else list()

        # Extract first 5 recommendations towards articles from the same news outlet, if available
        recommendations = response.xpath('//ul[@class="flex flex-col" and preceding-sibling::span[contains(text(), "Mehr zum Thema")]]//a[@class="text-black block" and not(../descendant::span[@data-flag-name="sponpaid"])]/@href').getall()
        if recommendations:
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
                item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        # Save article in html format
        save_as_html(response, 'spiegel.de', title)

        yield item 
