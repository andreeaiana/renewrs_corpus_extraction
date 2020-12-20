# -*- coding: utf-8 -*-

import os
import sys
import json
from news_crawler.spiders import BaseSpider
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from datetime import datetime
import dateutil.parser as date_parser

sys.path.insert(0, os.path.join(os.getcwd(), "..",))
from news_crawler.items import NewsCrawlerItem
from news_crawler.utils import remove_empty_paragraphs


class Rubikon(BaseSpider):
    """Spider for Rubikon"""
    name = 'rubikon'
    rotate_user_agent = True
    # allowed_domains = ['https://www.rubikon.news']
    start_urls = ['https://www.rubikon.news/']
    months_en = {"januar":"january", "februar":"february", "märz":"march", "mai":"may", "juni":"june", "juli":"july", "oktober":"october", "dezember":"december"}

    # Exclude articles in English and pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'rubikon\.news/\w.*$'),
                    deny=(r'rubikon\.news/international\/\w.*$',
                        r'www\.rubikon\.news/audio\/',
                        r'www\.rubikon\.news/plus\/',
                        r'www\.rubikon\.news/thema\/mobilitaet-videos\/',
                        r'www\.rubikon\.news/thema\/podcasts',
                        r'www\.rubikon\.news/thema\/audiostorys\/',
                        r'www\.rubikon\.news/thema\/spiegel-update\/',
                        r'www\.rubikon\.news/thema\/spiegel-tv\/',
                        r'www\.rubikon\.news/thema\/bundesliga_experten\/',
                        r'www\.rubikon\.news/video\/',
                        r'www\.rubikon\.news/newsletter',
                        r'www\.rubikon\.news/services',
                        r'www\.rubikon\.news/lebenundlernen\/schule\/ferien-schulferien-und-feiertage-a-193925\.html',
                        r'www\.rubikon\.news/dienste\/besser-surfen-auf-spiegel-online-so-funktioniert-rss-a-1040321\.html',
                        r'www\.rubikon\.news/gutscheine\/',
                        r'www\.rubikon\.news/impressum',
                        r'www\.rubikon\.news/kontakt',
                        r'www\.rubikon\.news/nutzungsbedingungen',
                        r'www\.rubikon\.news/datenschutz-spiegel'
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
        dates = response.xpath("//div[@class='article-meta']/text()").getall()
        try:
            dates = [date.strip() for date in dates if len(date.strip())!=0]
            creation_date = dates[0]
            creation_date = creation_date.split(',')[1].strip().lower()
            for month_de in self.months_en:
                if month_de in creation_date:
                    creation_date = creation_date.replace(month_de, months_en)
            creation_date = date_parser.parse(creation_date)
        except:
            return
        if not creation_date:
            return
        if self.is_out_of_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="article-content"]//p')]
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

        item['news_outlet'] = 'rubikon'
        item['provenance'] = response.url
        item['query_keywords'] = self.get_query_keywords()

        # Get creation, modification, and crawling dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        #could not get last modified
        # last_modified = response.xpath('//meta[@property="article:modified_time"]/@content').get()
        # item['last_modified'] = datetime.fromisoformat(last_modified.split('+')[0]).strftime('%d.%m.%Y')
        item['crawl_date'] = datetime.now().strftime('%d.%m.%Y')

        # Get authors
        item['author_person'] = response.xpath("//div[@class='article-author']//strong/text()").get()
        item['author_organization'] = list()

        # Extract keywords, if available
        news_keywords = response.xpath('//meta[@name="news_keywords"]/@content').get()
        item['news_keywords'] = news_keywords.split(', ') if news_keywords else list()

        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get()
        description = response.xpath('//meta[@name="description"]/@content').get()

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()

        # The article has inconsistent headlines
        for p in paragraphs:
            if description.replace("...","") in p:
                paragraphs.remove(p)
        body[''] = paragraphs

        item['content'] = {'title': title, 'description': description, 'body':body}

        # Extract first 5 recommendations towards articles from the same news outlet, if available
        recommendations = [response.urljoin(link) for link in response.xpath("//div[@class='loop-main']/article/a[@class='article-image']/@href").getall()]
        if recommendations:
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
                item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        item['response_body'] = response.body

        yield item
