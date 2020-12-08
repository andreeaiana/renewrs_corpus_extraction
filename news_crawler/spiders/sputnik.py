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


class Sputniknews(BaseSpider):
    """Spider for Sputniknews"""
    name = 'sputniknews'
    rotate_user_agent = True
    allowed_domains = ['de.sputniknews.com']
    start_urls = ['https://de.sputniknews.com/']

    # Exclude articles in English and pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'de\.sputniknews\.com\/\w.*$'),
                    deny=(r'de\.sputniknews\.com\/international\/\w.*$',
                        r'www\.de\.sputniknews\.com\/audio\/',
                        r'www\.de\.sputniknews\.com\/plus\/',
                        r'www\.de\.sputniknews\.com\/thema\/mobilitaet-videos\/',
                        r'www\.de\.sputniknews\.com\/thema\/podcasts',
                        r'www\.de\.sputniknews\.com\/thema\/audiostorys\/',
                        r'www\.de\.sputniknews\.com\/thema\/spiegel-update\/',
                        r'www\.de\.sputniknews\.com\/thema\/spiegel-tv\/',
                        r'www\.de\.sputniknews\.com\/thema\/bundesliga_experten\/',
                        r'www\.de\.sputniknews\.com\/video\/',
                        r'www\.de\.sputniknews\.com\/newsletter',
                        r'www\.de\.sputniknews\.com\/services',
                        r'www\.de\.sputniknews\.com\/lebenundlernen\/schule\/ferien-schulferien-und-feiertage-a-193925\.html',
                        r'www\.de\.sputniknews\.com\/dienste\/besser-surfen-auf-spiegel-online-so-funktioniert-rss-a-1040321\.html',
                        r'www\.de\.sputniknews\.com\/gutscheine\/',
                        r'www\.de\.sputniknews\.com\/impressum',
                        r'www\.de\.sputniknews\.com\/kontakt',
                        r'www\.de\.sputniknews\.com\/nutzungsbedingungen',
                        r'www\.de\.sputniknews\.com\/datenschutz-spiegel'
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
        creation_date = response.xpath('//meta[@itemprop="datePublished"]/@content').get()
        if not creation_date:
            return
        creation_date = datetime.fromisoformat(creation_date.split('T')[0])
        if self.is_out_of_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//div[@itemprop="articleBody"]//p')]
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

        item['news_outlet'] = 'pi-news'
        item['provenance'] = response.url
        item['query_keywords'] = self.get_query_keywords()

        # Get creation, modification, and crawling dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        last_modified = response.xpath('//meta[@itemprop="dateModified"]/@content').get()
        item['last_modified'] = datetime.fromisoformat(last_modified.split('T')[0]).strftime('%d.%m.%Y')
        item['crawl_date'] = datetime.now().strftime('%d.%m.%Y')

        # Get authors
        item['author_person'] = list()
        item['author_organization'] = list()
        item['author_person'].append(response.xpath('//meta[@name="author"]/@content').get())

        # Extract keywords, if available
        news_keywords = response.xpath('//meta[@name="news_keywords"]/@content').get()
        item['news_keywords'] = news_keywords.split(', ') if news_keywords else list()

        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get()
        description = response.xpath('//meta[@property="og:description"]/@content').get()

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()

        # The article has inconsistent headlines
        for p in paragraphs:
            if description is not None:
                if description.replace("...","") in p:
                    paragraphs.remove(p)
        body[''] = paragraphs

        item['content'] = {'title': title, 'description': description, 'body':body}

        # Extract first 5 recommendations towards articles from the same news outlet, if available
        recommendations = set(response.xpath("//div[@class='td-related-row']//a/@href").getall())
        if recommendations:
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
                item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        item['response_body'] = response.body

        yield item
