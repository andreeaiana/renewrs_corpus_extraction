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


class Epochtimes(BaseSpider):
    """Spider for Epochtimes"""
    name = 'epochtimes'
    rotate_user_agent = True
    allowed_domains = ['www.epochtimes.de']
    start_urls = ['https://www.epochtimes.de/']

    # Exclude articles in English and pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'epochtimes\.de\/\w.*$'),
                    deny=(r'epochtimes\.de\/international\/\w.*$',
                        r'www\.epochtimes\.de\/audio\/',
                        r'www\.epochtimes\.de\/plus\/',
                        r'www\.epochtimes\.de\/thema\/mobilitaet-videos\/',
                        r'www\.epochtimes\.de\/thema\/podcasts',
                        r'www\.epochtimes\.de\/thema\/audiostorys\/',
                        r'www\.epochtimes\.de\/thema\/spiegel-update\/',
                        r'www\.epochtimes\.de\/thema\/spiegel-tv\/',
                        r'www\.epochtimes\.de\/thema\/bundesliga_experten\/',
                        r'www\.epochtimes\.de\/video\/',
                        r'www\.epochtimes\.de\/newsletter',
                        r'www\.epochtimes\.de\/services',
                        r'www\.epochtimes\.de\/lebenundlernen\/schule\/ferien-schulferien-und-feiertage-a-193925\.html',
                        r'www\.epochtimes\.de\/dienste\/besser-surfen-auf-spiegel-online-so-funktioniert-rss-a-1040321\.html',
                        r'www\.epochtimes\.de\/gutscheine\/',
                        r'www\.epochtimes\.de\/impressum',
                        r'www\.epochtimes\.de\/kontakt',
                        r'www\.epochtimes\.de\/nutzungsbedingungen',
                        r'www\.epochtimes\.de\/datenschutz-spiegel'
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
        # Check date validity
        creation_date = response.xpath('//meta[@name="article:published_time"]/@content').get()
        if not creation_date:
            return
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if self.is_out_of_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//div[@id="news-content"]//p')]
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

        item['news_outlet'] = 'epochtimes'
        item['provenance'] = response.url
        item['query_keywords'] = self.get_query_keywords()

        # Get creation, modification, and crawling dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        last_modified = response.xpath('//meta[@name="article:modified_time"]/@content').get()
        item['last_modified'] = datetime.fromisoformat(last_modified.split('+')[0]).strftime('%d.%m.%Y')
        item['crawl_date'] = datetime.now().strftime('%d.%m.%Y')

        # Get authors
        item['author_person'] = list()
        item['author_organization'] = list()
        data_json = response.xpath('//script[@type="application/ld+json"]/text()').get()
        data_json = json.loads(data_json)
        data_json = data_json['author']
        if data_json['@type']=="Person":
            item['author_person'].append(data_json['name'])
        else:
            item['author_organization'].append(data_json['name'])


        # Extract keywords, if available
        news_keywords = response.xpath('//meta[@name="keywords"]/@content').get()
        item['news_keywords'] = news_keywords.split(', ') if news_keywords else list()

        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get()
        description = response.xpath('//meta[@property="og:description"]/@content').get()

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()

        # The article has inconsistent headlines
        for p in paragraphs:
            if description.replace("...","") in p:
                paragraphs.remove(p)
        body[''] = paragraphs

        item['content'] = {'title': title, 'description': description, 'body':body}

        # Extract first 5 recommendations towards articles from the same news outlet, if available
        recommendations = response.xpath('//div[@class="mu-title "]//a/@href').getall()
        if recommendations:
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
                item['recommendati ons'] = recommendations
        else:
            item['recommendations'] = list()

        item['response_body'] = response.body

        yield item
