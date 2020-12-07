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


class Jungefreiheit(BaseSpider):
    """Spider for jungefreiheit"""
    name = 'jungefreiheit'
    rotate_user_agent = True
    allowed_domains = ['jungefreiheit.de']
    start_urls = ['https://jungefreiheit.de/']

    # Exclude articles in English and pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'jungefreiheit\.de\/\w.*$'),
                    deny=(r'jungefreiheit\.de\/international\/\w.*$',
                        r'www\.jungefreiheit\.de\/audio\/',
                        r'www\.jungefreiheit\.de\/plus\/',
                        r'www\.jungefreiheit\.de\/thema\/mobilitaet-videos\/',
                        r'www\.jungefreiheit\.de\/thema\/podcasts',
                        r'www\.jungefreiheit\.de\/thema\/audiostorys\/',
                        r'www\.jungefreiheit\.de\/thema\/spiegel-update\/',
                        r'www\.jungefreiheit\.de\/thema\/spiegel-tv\/',
                        r'www\.jungefreiheit\.de\/thema\/bundesliga_experten\/',
                        r'www\.jungefreiheit\.de\/video\/',
                        r'www\.jungefreiheit\.de\/newsletter',
                        r'www\.jungefreiheit\.de\/services',
                        r'www\.jungefreiheit\.de\/lebenundlernen\/schule\/ferien-schulferien-und-feiertage-a-193925\.html',
                        r'www\.jungefreiheit\.de\/dienste\/besser-surfen-auf-spiegel-online-so-funktioniert-rss-a-1040321\.html',
                        r'www\.jungefreiheit\.de\/gutscheine\/',
                        r'www\.jungefreiheit\.de\/impressum',
                        r'www\.jungefreiheit\.de\/kontakt',
                        r'www\.jungefreiheit\.de\/nutzungsbedingungen',
                        r'www\.jungefreiheit\.de\/datenschutz-spiegel'
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
        data_json = response.xpath('//script[@type="application/ld+json"]/text()').get()
        data_json = json.loads(data_json)['@graph']
        creation_date = data_json[4]['datePublished']
        if not creation_date:
            return
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if self.is_out_of_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="elementor-widget-container"]/p')]
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

        item['news_outlet'] = 'jungefreiheit'
        item['provenance'] = response.url
        item['query_keywords'] = self.get_query_keywords()

        # Get creation, modification, and crawling dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')

        data_json = json.loads(data_json)
        last_modified = data_json[4]['dateModified']
        item['last_modified'] = datetime.fromisoformat(last_modified.split('+')[0]).strftime('%d.%m.%Y')
        item['crawl_date'] = datetime.now().strftime('%d.%m.%Y')

        # Get authors
        item['author_person'] = list()
        item['author_organization'] = list()
        author = data_json[4]['author']
        if author['@type'] == "Person":
            item['author_person'].append(author['name'])
        else:
            item['author_organization'].append(author['name'])

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
            if description.replace("...","") in p:
                paragraphs.remove(p)
        body[''] = paragraphs

        item['content'] = {'title': title, 'description': description, 'body':body}

        # Extract first 5 recommendations towards articles from the same news outlet, if available
        recommendations = response.xpath("//a[@class='ee-media ee-post__media ee-post__media--content']/@href").getall()
        if recommendations:
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
                item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        item['response_body'] = response.body

        yield item
