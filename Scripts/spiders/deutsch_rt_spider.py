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

class DeutschRT(scrapy.Spider):
    name = "deutsch_rt"
    custom_settings = {'DOWNLOADER_MIDDLEWARES' :  { 'scrapy_middleware.MyCustomDownloaderMiddleware': 900}}

    def __init__(self, config=None, *args, **kwargs):
        self.config = config
        self.article_num = 0
        self.today = date.today()
        self.today = self.today.strftime("%d-%m-%y")
        super(DeutschRT, self).__init__(*args, **kwargs)

        keywords = [keyword[:keyword.index('*')] for keyword in self.config['keywords'].keys()
                                               if '*' in keyword]
        self.url = 'https://deutsch.rt.com/search?&page={}&q={}'
        self.url_vs_query = {self.url.format('1',urllib.parse.quote(query)):query for query in keywords}
        self.start_urls = list(self.url_vs_query.keys())

    #deutsch rt
    def extract_paragraphs_from_article(self, all_para_tags):
        #reconstructing paragraphs by combining them with their respective headlines
        final_paragraphs = []
        for p_tag in all_para_tags:
            text = BeautifulSoup(p_tag, features="lxml").get_text()
            text = text.strip()
            if len(text)!=0:
                final_paragraphs.append({"":text})
        return final_paragraphs

    #deutsch rt
    def parse_news_page(self, response):
        body = response.text
        query = response.meta.get('query')
        print("Query used in Page:", query)
        date_published = response.css('div.article__date::text').get().split('•')[0].strip()
        headline = response.css('h1.article__heading::text').get().strip()
        author =''
        provenance = response.url
        last_modified = ''
        all_para_tags  = response.css('div.article__text>*').getall()
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
        links_in_page =  response.css('a.link.link_hover_red::attr(href)').getall()
        query_params = parse.parse_qs(parse.urlsplit(response.url).query)
        page_num, query = query_params['page'][0], query_params['q'][0]
        next_page = self.url.format(int(page_num)+1, query)
        for link in links_in_page:
            yield scrapy.Request(response.urljoin(link), callback = self.parse_news_page,
            meta = {'query':query})
        yield scrapy.Request(next_page, callback = self.parse)
