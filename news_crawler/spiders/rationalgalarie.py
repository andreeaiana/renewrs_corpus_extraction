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


class CompactOnline(BaseSpider):
    """Spider for Compact Online"""
    name = 'rationalgalarie'
    rotate_user_agent = True
    allowed_domains = ['rationalgalerie.de']
    start_urls = ['https://rationalgalerie.de/']

    # Exclude articles in English and pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'rationalgalerie\.de\/\w.*$'),
                    deny=(r'rationalgalerie\.de\/international\/\w.*$',
                        r'www\.rationalgalerie\.de\/audio\/',
                        r'www\.rationalgalerie\.de\/plus\/',
                        r'www\.rationalgalerie\.de\/thema\/mobilitaet-videos\/',
                        r'www\.rationalgalerie\.de\/thema\/podcasts',
                        r'www\.rationalgalerie\.de\/thema\/audiostorys\/',
                        r'www\.rationalgalerie\.de\/thema\/spiegel-update\/',
                        r'www\.rationalgalerie\.de\/thema\/spiegel-tv\/',
                        r'www\.rationalgalerie\.de\/thema\/bundesliga_experten\/',
                        r'www\.rationalgalerie\.de\/video\/',
                        r'www\.rationalgalerie\.de\/newsletter',
                        r'www\.rationalgalerie\.de\/services',
                        r'www\.rationalgalerie\.de\/lebenundlernen\/schule\/ferien-schulferien-und-feiertage-a-193925\.html',
                        r'www\.rationalgalerie\.de\/dienste\/besser-surfen-auf-spiegel-online-so-funktioniert-rss-a-1040321\.html',
                        r'www\.rationalgalerie\.de\/gutscheine\/',
                        r'www\.rationalgalerie\.de\/impressum',
                        r'www\.rationalgalerie\.de\/kontakt',
                        r'www\.rationalgalerie\.de\/nutzungsbedingungen',
                        r'www\.rationalgalerie\.de\/datenschutz-spiegel',
                        r'www\.rationalgalerie-live\.de\/'
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
        creation_date = datetime.strptime(creation_date,'%d.%m.%Y')
        if self.is_out_of_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath("//div[@class='aticle-text']/p")]
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

        item['news_outlet'] = 'rationalgalarie'
        item['provenance'] = response.url
        item['query_keywords'] = self.get_query_keywords()

        # Get creation, modification, and crawling dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        last_modified = response.xpath('//meta[@property="article:modified_time"]/@content').get()
        item['last_modified'] = datetime.strptime(creation_date,'%d.%m.%Y')
        item['crawl_date'] = datetime.now().strftime('%d.%m.%Y')

        # Get authors
        item['author_person'] = list()
        item['author_person'].append(response.xpath('//meta[@name="author"]/@content').get())
        item['author_organization'] = list()

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
        recommendations = [response.urljoin(link) for link in response.xpath("//div[@class='pos-relative']/h4/a/@href").getall()]
        if recommendations:
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
                item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        item['response_body'] = response.body

        yield item
