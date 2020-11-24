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
        """
        Checks article validity. If valid, it parses it.
        """
        
        data_json = response.xpath('//script[@type="application/ld+json"]/text()').get()
        if not data_json:
            return
        data = json.loads(data_json)        

        # Check date validity 
        if 'dateCreated' not in data.keys():
            return
        creation_date = data['dateCreated']
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if self.is_out_of_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//p[not(ancestor::div[@class="vhb-caption"]) and not(contains(@class, "vhb-notice")) and not(contains(@class, "vhb-comment-content")) and not(descendant::strong[contains(text(), "Mehr:")])]')]
        paragraphs = remove_empty_paragraphs(paragraphs)
        text = ' '.join([para for para in paragraphs])

        # Check article's length validity
        if not self.has_min_length(text):
            return

        # Check keywords validity
        if not self.has_valid_keywords(text):
            return

        # Parse the valid article
        item = NewsCrawlerItem()
        
        item['news_outlet'] = 'handelsblatt'
        item['provenance'] = response.url
        item['query_keywords'] = self.get_query_keywords()
        
        # Get creation, modification, and crawling dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        # No last-modified date available
        item['last_modified'] = creation_date.strftime('%d.%m.%Y')
        item['crawl_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get authors (can be displayed in different ways)
        author_person = response.xpath('//div[contains(@class, "vhb-author--onecolumn")]/ul/li/a/text()').getall()
        if author_person:
            item['author_person'] = [author.strip() for author in author_person]
        else:
            author_person = response.xpath('//div[contains(@class, "vhb-author--onecolumn")]/ul/li/text()').getall()
            if author_person:
                item['author_person'] = [author.strip() for author in author_person]
            else:
                author_person = response.xpath('//div[contains(@class, "vhb-author--onecolumn")]/a/span/text()').getall()
                if author_person:
                    item['author_person'] = [author.strip() for author in author_person]
                else:
                    item['author_person'] = list()
        author_organization = response.xpath('//ul[@class="vhb-author-shortcutlist"]/li[@class="vhb-author-shortcutlist--name"]/text()').getall()
        item['author_organization'] = author_organization if author_organization else list()

        # Extract keywords
        news_keywords = response.xpath('//meta[@name="keywords"]/@content').get()
        item['news_keywords'] = news_keywords.split(',') if news_keywords else list()
        
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
            body[''] = remove_empty_paragraphs(text[:text.index(headlines[0])])

            # Extract paragraphs corresponding to each headline, except the last one
            for i in range(len(headlines)-1):
                body[headlines[i]] = remove_empty_paragraphs(text[text.index(headlines[i])+1:text.index(headlines[i+1])])

            # Extract the paragraphs belonging to the last headline
            body[headlines[-1]] = remove_empty_paragraphs(text[text.index(headlines[-1])+1:])

        else:
            # The article has no headlines, just paragraphs
            body[''] = paragraphs 

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract first 5 recommendations towards articles from the same news outlet, if available
        recommendations = response.xpath('//div[@class="vhb-teaser--recommendation-row"]/div/h3/a/@href').getall()
        if len(recommendations) > 5:
            recommendations = recommendations[:5]
        item['recommendations'] = ['https://www.handelsblatt.com' + rec for rec in recommendations] if recommendations else list()

        item['response_body'] = response.body

        yield item
