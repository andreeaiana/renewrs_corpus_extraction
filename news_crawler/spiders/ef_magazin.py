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


class EfMagazin(BaseSpider):
    """Spider for journalistenwatch"""
    name = 'ef_magazin'
    rotate_user_agent = True
    # allowed_domains = ['ef-magazin.de']
    start_urls = ['https://www.ef-magazin.de/']

    # Exclude articles in English and pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'ef-magazin\.de\/\w.*$'),
                    deny=(r'ef-magazin\.de\/international\/\w.*$',
                        r'www\.ef-magazin\.de\/audio\/',
                        r'www\.ef-magazin\.de\/plus\/',
                        r'www\.ef-magazin\.de\/thema\/mobilitaet-videos\/',
                        r'www\.ef-magazin\.de\/thema\/podcasts',
                        r'www\.ef-magazin\.de\/thema\/audiostorys\/',
                        r'www\.ef-magazin\.de\/thema\/spiegel-update\/',
                        r'www\.ef-magazin\.de\/thema\/spiegel-tv\/',
                        r'www\.ef-magazin\.de\/thema\/bundesliga_experten\/',
                        r'www\.ef-magazin\.de\/video\/',
                        r'www\.ef-magazin\.de\/newsletter',
                        r'www\.ef-magazin\.de\/services',
                        r'www\.ef-magazin\.de\/lebenundlernen\/schule\/ferien-schulferien-und-feiertage-a-193925\.html',
                        r'www\.ef-magazin\.de\/dienste\/besser-surfen-auf-spiegel-online-so-funktioniert-rss-a-1040321\.html',
                        r'www\.ef-magazin\.de\/gutscheine\/',
                        r'www\.ef-magazin\.de\/impressum',
                        r'www\.ef-magazin\.de\/kontakt',
                        r'www\.ef-magazin\.de\/nutzungsbedingungen',
                        r'www\.ef-magazin\.de\/datenschutz-spiegel'
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
        url = response.xpath('//meta[@property="og:url"]/@content').get()
        try:
            creation_date = url[url.index("de")+3:url.rindex('/')]
            creation_date = datetime.strptime(creation_date, "%Y/%m/%d")
        except:
            return

        if not creation_date:
            return
        if self.is_out_of_date(creation_date):
            return
        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath("//article[@class='col-md-7']/p")]
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

        item['news_outlet'] = 'ef_magazin'
        item['provenance'] = response.url
        item['query_keywords'] = self.get_query_keywords()

        # Get creation, modification, and crawling dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        #could not obtain last modified
        # last_modified = response.xpath('//meta[@property="article:modified_time"]/@content').get()
        # item['last_modified'] = datetime.fromisoformat(last_modified.split('+')[0]).strftime('%d.%m.%Y')
        item['crawl_date'] = datetime.now().strftime('%d.%m.%Y')

        # Get authors
        item['author_person'] = response.xpath("//em[@class='author']/a/text()").get()
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
        recommendations = [response.urljoin(link) for link in response.xpath("//h3[@class='ahd'][contains(text(),'Dossier: ')]/following-sibling::aside[1]/ul/li/a/@href").getall()]
        if recommendations:
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
                item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        item['response_body'] = response.body

        yield item
