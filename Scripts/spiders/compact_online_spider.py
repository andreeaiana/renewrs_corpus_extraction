import scrapy
from datetime import date
import json
import urllib.parse as ulib
import dateutil.parser as date_parser
import re
import os
import bs4
import urllib.parse
from bs4 import BeautifulSoup
from article_validator import ArticleValidator
from scrapy.http import Request

class CompactOnline(scrapy.Spider):
    name = "compact_online"
    custom_settings = {'DOWNLOADER_MIDDLEWARES' :  { 'scrapy_middleware.MyCustomDownloaderMiddleware': 900}}

    def __init__(self, config=None, *args, **kwargs):
        self.config = config
        self.article_num = 0
        self.today = date.today()
        self.today = self.today.strftime("%d-%m-%y")
        super(CompactOnline, self).__init__(*args, **kwargs)

        keywords = [keyword[:keyword.index('*')] for keyword in self.config['keywords'].keys()
                                               if '*' in keyword]
        urls = [
        'https://www.compact-online.de/?s={}'
        ]
        self.url_vs_query = {url.format(urllib.parse.quote(query)):query for url in urls for query in keywords}
        self.start_urls = list(self.url_vs_query.keys())

    #compact-online
    def extract_paragraphs_from_article(self, all_para_tags):
        #reconstructing paragraphs by combining them with their respective headlines
        final_paragraphs = []
        for p_tag in all_para_tags:
            text = BeautifulSoup(p_tag).get_text()
            text = text.strip()
            if len(text)!=0:
                final_paragraphs.append({"":text})
        return final_paragraphs

    #compact online
    def parse_news_page(self, response):
        body = response.text
        query = response.meta.get('query')
        print("Query used in Page:", query)
        date_published = response.css('time.value-title::attr(title)').get()
        author = response.css('span.posted-by>span.reviewer ::text').get()
        provenance = response.url
        headline = response.css('h1.post-title::text').get().strip()
        last_modified = response.css('meta[property="article:modified_time"]::attr(content)').get()
        all_para_tags  = response.css('div.post-content>*').getall()
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
        if self.article_num > self.config["top_k"]:
            exit(-1)
        with open(os.path.join("spiders",self.name, str(self.article_num)+'.json'), 'w', encoding = 'utf-8') as f:
            json.dump(json_obj, f)
        with open(os.path.join("spiders", self.name, str(self.article_num)+'.html'),'w',encoding = 'utf-8') as f:
            f.write(body)


    def parse(self, response):
        links_in_page =  response.css('article>a::attr(href)').getall()
        next_page = response.css('a.next::attr(href)').get()
        for link in links_in_page:
            if self.article_num > self.config["top_k"]:
                exit(-1)
            query  =  self.url_vs_query[response.url] if response.url in self.url_vs_query.keys() else urllib.parse.unquote(response.url[response.url.index("=")+1:])
            yield scrapy.Request(link, callback = self.parse_news_page,
            meta = {'query':query})
        if next_page is not None:
            yield scrapy.Request(next_page, callback = self.parse)
