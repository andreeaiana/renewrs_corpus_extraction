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


class Achgut(BaseSpider):
    """Spider for Achgut"""
    name = 'achgut'
    rotate_user_agent = True
    # allowed_domains = ['https://www.rubikon.news']
    start_urls = ['https://www.achgut.com/']
    months_en = {"januar":"january", "februar":"february", "märz":"march", "mai":"may", "juni":"june", "juli":"july", "oktober":"october", "dezember":"december"}

    # Exclude articles in English and pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                        allow=(r'achgut\.com\/\w.*$'),
                    deny=(r'achgut\.com\/international\/\w.*$',
                        r'www\.achgut\.com\/audio\/',
                        r'www\.achgut\.com\/plus\/',
                        r'www\.achgut\.com\/thema\/mobilitaet-videos\/',
                        r'www\.achgut\.com\/thema\/podcasts',
                        r'www\.achgut\.com\/thema\/audiostorys\/',
                        r'www\.achgut\.com\/thema\/spiegel-update\/',
                        r'www\.achgut\.com\/thema\/spiegel-tv\/',
                        r'www\.achgut\.com\/thema\/bundesliga_experten\/',
                        r'www\.achgut\.com\/video\/',
                        r'www\.achgut\.com\/newsletter',
                        r'www\.achgut\.com\/services',
                        r'www\.achgut\.com\/lebenundlernen\/schule\/ferien-schulferien-und-feiertage-a-193925\.html',
                        r'www\.achgut\.com\/dienste\/besser-surfen-auf-spiegel-online-so-funktioniert-rss-a-1040321\.html',
                        r'www\.achgut\.com\/gutscheine\/',
                        r'www\.achgut\.com\/impressum',
                        r'www\.achgut\.com\/kontakt',
                        r'www\.achgut\.com\/nutzungsbedingungen',
                        r'www\.achgut\.com\/datenschutz-spiegel'
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
        dates = response.xpath("//div[@class='column full']//div[@class='teaser_text_meta']/text()").getall()
        try:
            dates = [date.strip() for date in dates if len(date.strip())!=0]
            creation_date = dates[0]
            creation_date = creation_date.replace('/','').strip()
            creation_date = date_parser.parse(creation_date)
        except:
            return
        if not creation_date:
            return
        if self.is_out_of_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//div[@id="article_maincontent"]/p')]
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

        item['news_outlet'] = 'achgut'
        item['provenance'] = response.url
        item['query_keywords'] = self.get_query_keywords()

        # Get creation, modification, and crawling dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        #could not get last modified
        # last_modified = response.xpath('//meta[@property="article:modified_time"]/@content').get()
        # item['last_modified'] = datetime.fromisoformat(last_modified.split('+')[0]).strftime('%d.%m.%Y')
        item['crawl_date'] = datetime.now().strftime('%d.%m.%Y')

        # Get authors
        item['author_person'] = response.xpath("//div[@class='column full']//div[@class='teaser_text_meta']/*[1]/text()").get().strip()
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
        recommendations = [response.urljoin(link) for link in response.xpath("//div[@class='teaser_blog_text']/h3/a/@href").getall()]
        if recommendations:
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
                item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        item['response_body'] = response.body

        yield item
