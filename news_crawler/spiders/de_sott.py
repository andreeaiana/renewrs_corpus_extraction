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


class DeSott(BaseSpider):
    """Spider for DeSott"""
    name = 'de_sott'
    rotate_user_agent = True
    # allowed_domains = ['https://www.rubikon.news']
    start_urls = ['https://de.sott.net/']
    months_en = {"mär":"march", "mai":"may", "juni":"june", "juli":"july", "okt":"october", "dez":"december"}

    # Exclude articles in English and pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                        allow=(r'de\.sott\.net\/\w.*$'),
                    deny=(r'de\.sott\.net\/users\/\w.*$',
                        r'de\.sott\.net\/user\/\w.*$'
                        r'www\.de\.sott\.net\/thema\/mobilitaet-videos\/',
                        r'www\.de\.sott\.net\/thema\/podcasts',
                        r'www\.de\.sott\.net\/thema\/audiostorys\/',
                        r'www\.de\.sott\.net\/thema\/spiegel-update\/',
                        r'www\.de\.sott\.net\/thema\/spiegel-tv\/',
                        r'www\.de\.sott\.net\/thema\/bundesliga_experten\/',
                        r'www\.de\.sott\.net\/video\/',
                        r'www\.de\.sott\.net\/newsletter',
                        r'www\.de\.sott\.net\/services',
                        r'www\.de\.sott\.net\/lebenundlernen\/schule\/ferien-schulferien-und-feiertage-a-193925\.html',
                        r'www\.de\.sott\.net\/dienste\/besser-surfen-auf-spiegel-online-so-funktioniert-rss-a-1040321\.html',
                        r'www\.de\.sott\.net\/gutscheine\/',
                        r'www\.de\.sott\.net\/impressum',
                        r'www\.de\.sott\.net\/kontakt',
                        r'www\.de\.sott\.net\/nutzungsbedingungen',
                        r'www\.de\.sott\.net\/datenschutz-spiegel'
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
        date =response.xpath("//div[@class='article-info']//div[@class='m-bar']")[0]
        date = date.xpath("string()").get().split(',')[1].strip().lower()
        date = date.replace('utc','').strip()
        for month_de in self.months_en.keys():
            if month_de in date:
                date = date.replace(month_de, self.months_en[month_de])
        creation_date = date_parser.parse(date)
        if not creation_date:
            return
        if self.is_out_of_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [text.strip() for text in response.xpath('//div[@class="article-body"]//text()').getall()]
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

        item['news_outlet'] = 'de_sott'
        item['provenance'] = response.url
        item['query_keywords'] = self.get_query_keywords()

        # Get creation, modification, and crawling dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        #could not get last modified
        # last_modified = response.xpath('//meta[@property="article:modified_time"]/@content').get()
        # item['last_modified'] = datetime.fromisoformat(last_modified.split('+')[0]).strftime('%d.%m.%Y')
        item['crawl_date'] = datetime.now().strftime('%d.%m.%Y')

        # Get authors
        item['author_person'] = list()
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
