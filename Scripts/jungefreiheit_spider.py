import scrapy
from datetime import date
import json
from urllib import parse
import dateutil.parser as date_parser
import re
import os
import bs4
import urllib.parse
from bs4 import BeautifulSoup
from article_validator import ArticleValidator
from scrapy.http import Request

class Jungefreiheit(scrapy.Spider):
    name = "Jungefreiheit"
    custom_settings = {'DOWNLOADER_MIDDLEWARES' :  { 'scrapy_middleware.MyCustomDownloaderMiddleware': 900}}
    months_en = {"januar":"january", "februar":"february", "märz":"march", "mai":"may", "juni":"june", "juli":"july", "oktober":"october", "dezember":"december"}

    def __init__(self, config=None, *args, **kwargs):
        self.config = config
        self.article_num = 0
        self.today = date.today()
        self.today = self.today.strftime("%d-%m-%y")
        super(Jungefreiheit, self).__init__(*args, **kwargs)

        keywords = [keyword[:keyword.index('*')] for keyword in self.config['keywords'].keys()
                                               if '*' in keyword]
        self.url = 'https://jungefreiheit.de/page/{}/?s={}'
        self.url_vs_query = {self.url.format('1',urllib.parse.quote(query)):query for query in keywords}
        self.start_urls = list(self.url_vs_query.keys())

    def extract_paragraphs_from_article(self, all_para_tags):
        #reconstructing paragraphs by combining them with their respective headlines
        final_paragraphs = []
        for p_tag in all_para_tags:
            text = BeautifulSoup(p_tag, features="lxml").get_text()
            text = text.strip()
            if len(text)!=0:
                final_paragraphs.append({"":text})
        return final_paragraphs

    def parse_news_page(self, response):
        body = response.text
        query = response.meta.get('query')
        print("Query used in Page:", query)
        date_published = response.css('span.elementor-post-info__item--type-date::text').get().lower()
        month_de = [month for month in self.months_en.keys() if month in date_published]
        if len(month_de)>0:
            date_published = date_published.replace(month_de[0], self.months_en[month_de[0]])
        headline = response.css('h1.elementor-heading-title.elementor-size-default::text').get()
        author = response.css('span.elementor-post-info__item--type-custom::text').get()
        provenance = response.url
        last_modified = ''
        all_para_tags  = response.css('div.elementor-widget-theme-post-content>div.elementor-widget-container>*>p::text').getall()
        if len(all_para_tags)==0:
            all_para_tags  = response.css('div.elementor-widget-theme-post-content>div.elementor-widget-container>p::text').getall()
        paragraphs_with_headlines = self.extract_paragraphs_from_article(all_para_tags)
        json_obj =  {"provenance": provenance,
                    "author":[author],
                    "creation_date": date_published,
                    "content":{"title":headline, "body": paragraphs_with_headlines},
                    "last_modified":last_modified,
                    "crawl_date": self.today,
                    "query_word":query
                    }
        whole_text = [BeautifulSoup(p_tag, features="lxml").get_text() for p_tag in all_para_tags]
        art_validator = ArticleValidator(json_obj, self.config)
        if not art_validator.is_valid_article(json_obj, query, whole_text):
            return
        self.article_num += 1
        with open(os.path.join("spiders",self.name, str(self.article_num)+'.json'), 'w', encoding = 'utf-8') as f:
            json.dump(json_obj, f)
        with open(os.path.join("spiders", self.name, str(self.article_num)+'.html'),'w',encoding = 'utf-8') as f:
            f.write(body)


    def parse(self, response):
        if self.article_num > self.config["top_k"]:
            exit(-1)
        elif response is None:
            return
        query_params = parse.parse_qs(parse.urlsplit(response.url).query)
        links_in_page =  response.css('a.elementor-post__thumbnail__link::attr(href)').getall()
        next_page = response.css('a.page-numbers.next::attr(href)').get()
        for link in links_in_page:
            yield scrapy.Request(response.urljoin(link), callback = self.parse_news_page,
            meta = {'query':query_params['s'][0]})
        if next_page is not None:
            yield scrapy.Request(next_page, callback = self.parse)
