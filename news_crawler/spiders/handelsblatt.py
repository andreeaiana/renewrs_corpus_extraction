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


class HandelsblattSpider(BaseSpider):
    """Spider for Handelsblatt"""
    name = 'handelsblatt'
    rotate_user_agent = True
    allowed_domains = ['www.handelsblatt.com']
    start_urls = ['https://www.handelsblatt.com/']

    # Exclude pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'www\.handelsblatt\.com\/\w.*\/\d+\.html$'),
                    deny=(r'www\.handelsblatt\.com\/themen\/meine\-news\?\w.*',
                        r'www\.handelsblatt\.com\/finanzen\/finanztools\/',
                        r'www\.handelsblatt\.com\/arts_und_style\/spiele\/',
                        r'www\.handelsblatt\.com\/video\/',
                        r'www\.handelsblatt\.com\/audio\/',
                        r'www\.handelsblatt\.com\/angebot\/',
                        r'www\.handelsblatt\.com\/veranstaltungen\/',
                        r'www\.handelsblatt\.com\/service-angebote\/',
                        r'www\.handelsblatt\.com\/impressum\/',
                        r'www\.handelsblatt\.com\/sitemap\/',
                        r'handelsblattgroup\.com\/agb\/',
                        r'www\.handelsblatt\.com\/\w.*\_detail\_tab\_comments.*'
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
        if 'dateCreated' not in data.keys():
            return
        creation_date = data['dateCreated']
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//p[not(ancestor::div[@class="vhb-caption"]) and not(contains(@class, "vhb-notice")) and not(contains(@class, "vhb-comment-content")) and not(descendant::strong[contains(text(), "Mehr:")])]')]
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
        
        # Get authors (can be displayed in different ways)
        authors = response.xpath('//div[contains(@class, "vhb-author--onecolumn")]/ul/li/a/text()').getall()
        if authors:
            item['author'] = [author.strip() for author in authors]
        else:
            authors = response.xpath('//div[contains(@class, "vhb-author--onecolumn")]/ul/li/text()').getall()
            if authors:
                item['author'] = [author.strip() for author in authors]
            else:
                authors = response.xpath('//div[contains(@class, "vhb-author--onecolumn")]/a/span/text()').getall()
                if authors:
                    item['author'] = [author.strip() for author in authors]
                else:
                    item['author'] = list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        # No last-modified date available
        item['last_modified'] = creation_date.strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get()
        description = response.xpath('//meta[@property="og:description"]/@content').get()
       
        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h3[not(descendant::a)]'):
            # Extract headlines
            headlines = [h3.xpath('string()').get().strip() for h3 in response.xpath('//h3[not(descendant::a)]')]

            # Extract the paragraphs and headlines together
            text = [node.xpath('string()').get().strip() for node in response.xpath('//p[not(ancestor::div[@class="vhb-caption"]) and not(contains(@class, "vhb-notice")) and not(contains(@class, "vhb-comment-content")) and not(descendant::strong[contains(text(), "Mehr:")])] | //h3[not(descendant::a)]')]
          
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
      
        # Extract keywords
        keywords = response.xpath('//meta[@name="keywords"]/@content').get()
        item['keywords'] = keywords.split(',') if keywords else list()
        
        # Extract first 5 recommendations towards articles from the same news outlet, if available
        recommendations = response.xpath('//div[@class="vhb-teaser--recommendation-row"]/div/h3/a/@href').getall()
        if len(recommendations) > 5:
            recommendations = recommendations[:5]
        item['recommendations'] = ['https://www.handelsblatt.com' + rec for rec in recommendations] if recommendations else list()

        # Save article in html format
        save_as_html(response, 'handelsblatt.com', title)

        yield item
