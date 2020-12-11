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


class Vice(BaseSpider):
    """Spider for Vice"""
    name = 'vice'
    rotate_user_agent = True
    allowed_domains = ['vice.com']
    start_urls = ['https://vice.com/de/']

    # Exclude articles in English and pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'www\.vice\.com\/de\/\w.*$'),
                    deny=(r'vice\.com\/de\/international\/\w.*$',
                        r'www\.vice\.com\/de\/audio\/',
                        r'www\.vice\.com\/de\/plus\/',
                        r'www\.vice\.com\/de\/thema\/mobilitaet-videos\/',
                        r'www\.vice\.com\/de\/thema\/podcasts',
                        r'www\.vice\.com\/de\/thema\/audiostorys\/',
                        r'www\.vice\.com\/de\/thema\/spiegel-update\/',
                        r'www\.vice\.com\/de\/thema\/spiegel-tv\/',
                        r'www\.vice\.com\/de\/thema\/bundesliga_experten\/',
                        r'www\.vice\.com\/de\/video\/',
                        r'www\.vice\.com\/de\/newsletter',
                        r'www\.vice\.com\/de\/services',
                        r'www\.vice\.com\/de\/lebenundlernen\/schule\/ferien-schulferien-und-feiertage-a-193925\.html',
                        r'www\.vice\.com\/de\/dienste\/besser-surfen-auf-spiegel-online-so-funktioniert-rss-a-1040321\.html',
                        r'www\.vice\.com\/de\/gutscheine\/',
                        r'www\.vice\.com\/de\/impressum',
                        r'www\.vice\.com\/de\/kontakt',
                        r'www\.vice\.com\/de\/nutzungsbedingungen',
                        r'www\.vice\.com\/de\/datenschutz-spiegel'
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
        data_json = response.xpath("//script[@type='application/ld+json']/text()").get()
        try:
            data_json = json.loads(data_json)
        except:
            return
        if '@graph' not in data_json:
            return
        data_json = data_json['@graph'][1]
        creation_date = data_json['datePublished']
        if not creation_date:
            return
        creation_date = datetime.fromisoformat(creation_date.split('T')[0])
        if self.is_out_of_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath("//div[@class='article__body-components']//p")]
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

        item['news_outlet'] = 'vice'
        item['provenance'] = response.url
        item['query_keywords'] = self.get_query_keywords()

        # Get creation, modification, and crawling dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        last_modified = data_json['dateModified']
        item['last_modified'] = last_modified
        item['crawl_date'] = datetime.now().strftime('%d.%m.%Y')

        # Get authors
        item['author_person'] = list()
        item['author_organization'] = list()
        if data_json['author']['@type']=='Person':
            item['author_person'].append(data_json['author']['name'])
        else:
            item['author_organization'].append(data_json['author']['name'])

        # Extract keywords, if available
        targeting_data = response.xpath('//div[@class="vice-ad__ad"]/@data-targeting').get()
        if targeting_data is not None:
            targeting_data = json.loads(targeting_data)
        news_keywords = targeting_data['keywords']
        item['news_keywords'] = news_keywords

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
        recommendations = set(response.xpath('//div[@class="td-related-row"]//a/@href').getall())
        if recommendations:
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
                item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        item['response_body'] = response.body

        yield item
