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


class Antispiegel(BaseSpider):
    """Spider for Antispiegel"""
    name = 'anti_spiegel'
    rotate_user_agent = True
    allowed_domains = ['www.anti-spiegel.ru']
    start_urls = ['https://www.anti-spiegel.ru/']

    # Exclude articles in English and pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'anti-spiegel\.ru\/\w.*$'),
                    deny=(r'anti-spiegel\.ru\/international\/\w.*$',
                        r'www\.anti-spiegel\.ru\/audio\/',
                        r'www\.anti-spiegel\.ru\/plus\/',
                        r'www\.anti-spiegel\.ru\/thema\/mobilitaet-videos\/',
                        r'www\.anti-spiegel\.ru\/thema\/podcasts',
                        r'www\.anti-spiegel\.ru\/thema\/audiostorys\/',
                        r'www\.anti-spiegel\.ru\/thema\/spiegel-update\/',
                        r'www\.anti-spiegel\.ru\/thema\/spiegel-tv\/',
                        r'www\.anti-spiegel\.ru\/thema\/bundesliga_experten\/',
                        r'www\.anti-spiegel\.ru\/video\/',
                        r'www\.anti-spiegel\.ru\/newsletter',
                        r'www\.anti-spiegel\.ru\/services',
                        r'www\.anti-spiegel\.ru\/lebenundlernen\/schule\/ferien-schulferien-und-feiertage-a-193925\.html',
                        r'www\.anti-spiegel\.ru\/dienste\/besser-surfen-auf-spiegel-online-so-funktioniert-rss-a-1040321\.html',
                        r'www\.anti-spiegel\.ru\/gutscheine\/',
                        r'www\.anti-spiegel\.ru\/impressum',
                        r'www\.anti-spiegel\.ru\/kontakt',
                        r'www\.anti-spiegel\.ru\/nutzungsbedingungen',
                        r'www\.anti-spiegel\.ru\/datenschutz-spiegel'
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
        creation_date = response.xpath('//meta[@property="article:published_time"]/@content').get()
        if not creation_date:
            return
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if self.is_out_of_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//div[contains(@class, "entry-content")]/p')]
        paragraphs = remove_empty_paragraphs(paragraphs)
        text = ' '.join([para for para in paragraphs])

        # # Check article's length validity
        if not self.has_min_length(text):
            return

        # Check keywords validity
        if not self.has_valid_keywords(text):
            return

        # Parse the valid article
        item = NewsCrawlerItem()

        item['news_outlet'] = 'anti_spiegel'
        item['provenance'] = response.url
        item['query_keywords'] = self.get_query_keywords()

        # Get creation, modification, and crawling dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        last_modified = response.xpath('//meta[@property="article:modified_time"]/@content').get()
        item['last_modified'] = datetime.fromisoformat(last_modified.split('+')[0]).strftime('%d.%m.%Y')
        item['crawl_date'] = datetime.now().strftime('%d.%m.%Y')

        # Get authors
        item['author_person'] = list()
        item['author_organization'] = list()
        author = response.xpath("//h2[@class='author-title']/text()").get().strip()
        if 'Anti' in author:
            item['author_organization'].append(author)
        else:
            item['author_person'].append(author)

        # Extract keywords, if available
        data_json = response.xpath("//script[@class='yoast-schema-graph']/text()").get()
        data_json = json.loads(data_json)
        news_keywords = data_json['@graph'][3]['keywords']
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
        recommendations = response.xpath('//section[@class="related-posts"]//article/a/@href').getall()
        if recommendations:
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
                item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        item['response_body'] = response.body

        yield item
