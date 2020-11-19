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

class Epochtimes(scrapy.Spider):
    name = "epochtimes"
    custom_settings = {'DOWNLOADER_MIDDLEWARES' :  { 'scrapy_middleware.MyCustomDownloaderMiddleware': 900}}
    months_en = {"januar":"january", "februar":"february", "märz":"march", "mai":"may", "juni":"june", "juli":"july", "oktober":"october", "dezember":"december"}

    def __init__(self, config=None, *args, **kwargs):
        self.config = config
        self.article_num = 0
        self.today = date.today()
        self.today = self.today.strftime("%d-%m-%y")
        super(Epochtimes, self).__init__(*args, **kwargs)

        keywords = [keyword[:keyword.index('*')] for keyword in self.config['keywords'].keys()
                                               if '*' in keyword]
        self.url = 'https://www.epochtimes.de/suche?pagenum={}&q={}&sort=relevance'
        self.url_vs_query = {self.url.format('1',urllib.parse.quote(query)):query for query in keywords}
        self.start_urls = list(self.url_vs_query.keys())

    #epochtimes
    def extract_paragraphs_from_article(self, all_tags, headlines):
        #reconstructing paragraphs by combining them with their respective headlines
        final_paragraphs = []
        each_para = []
        each_headline = ""
        new_headline = False
        all_text = [BeautifulSoup(_tag, features="lxml").get_text() for _tag in all_tags]
        for para in all_text:
            if len(para)==0:
                continue
            if para in headlines:
                if len(each_para) > 0:
                    final_paragraphs.append({each_headline:each_para})
                each_headline = para
                new_headline = True
                each_para = []
            #para is independent, not associated to any heading
            elif len(each_para) == 0 and not new_headline:
                final_paragraphs.append({each_headline:para})
            else:
                #para is not a part of the heading
                each_para.append(para)
        if len(each_para) > 0:
            final_paragraphs.append({each_headline:each_para})
        return final_paragraphs

    #epochtimes
    def parse_news_page(self, response):
        if self.article_num > self.config["top_k"]:
            exit(-1)
        body = response.text
        query = response.meta.get('query')
        print("Query used in Page:", query)
        date_published = response.css('span.publish-date::text').get().lower()
        month_de = [month for month in self.months_en.keys() if month in date_published]
        if len(month_de)>0:
            date_published = date_published.replace(month_de[0], self.months_en[month_de[0]])
        headline = response.css('div#news-header>h1::text').get().strip()
        author = response.css('div.news-meta>span.author> ::text').get()
        author = '' if author is None else author
        provenance = response.url
        last_modified = response.css('span.last-modified::text').get().replace('Aktualisiert: ','')
        h_tags = response.css('div#news-content>*>h2').getall()
        headlines = [BeautifulSoup(h_tag, features="lxml").get_text() for h_tag in h_tags]
        all_tags = response.css('div#news-content>*>*').getall()
        paragraphs_with_headlines = self.extract_paragraphs_from_article(all_tags, headlines)
        json_obj =  {"provenance": provenance,
                    "author":[author],
                    "creation_date": date_published,
                    "content":{"title":headline, "body": paragraphs_with_headlines},
                    "last_modified":last_modified,
                    "crawl_date": self.today,
                    "query_word":query
                    }
        whole_text = [BeautifulSoup(p_tag, features="lxml").get_text() for p_tag in all_tags]
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
        links_in_page  = response.css('div.pure-u-1>a.link::attr(href)').getall()
        if len(links_in_page) == 0:
            return
        query_params = parse.parse_qs(parse.urlsplit(response.url).query)
        page_num, query = query_params['pagenum'][0], query_params['q'][0]
        next_page = self.url.format(int(page_num)+1, query)
        for link in links_in_page:
            yield scrapy.Request(link, callback = self.parse_news_page,
            meta = {'query':query})
        yield scrapy.Request(next_page, callback = self.parse)
