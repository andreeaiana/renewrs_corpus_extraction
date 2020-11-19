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
from scrapy.http import Request
from article_validator import ArticleValidator


class Rationalegalerie(scrapy.Spider):
    name = "rationalgalerie"
    custom_settings = {'DOWNLOADER_MIDDLEWARES' :  { 'scrapy_middleware.MyCustomDownloaderMiddleware': 900}}

    def __init__(self, config=None, *args, **kwargs):
        self.config = config
        self.article_num = 0
        self.today = date.today()
        self.today = self.today.strftime("%d-%m-%y")
        super(Rationalegalerie, self).__init__(*args, **kwargs)

        keywords = [keyword[:keyword.index('*')] for keyword in self.config['keywords'].keys()
                                               if '*' in keyword]
        urls = [
        'https://rationalgalerie.de/weiteres/suche?searchword={}&searchphrase=all&limit=0'
        ]
        self.url_vs_query = {url.format(urllib.parse.quote(query)):query for url in urls for query in keywords}
        self.start_urls = list(self.url_vs_query.keys())

    #rationalgalerie
    def extract_paragraphs_from_article(self, all_para_tags, headlines):
        #reconstructing paragraphs by combining them with their respective headlines
        final_paragraphs = []
        each_para = []
        each_headline = ""
        new_headline = False
        all_text = [BeautifulSoup(p_tag, features="lxml").get_text() for p_tag in all_para_tags]
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

    #rationalgalerie
    def parse_news_page(self, response):
        body = response.text
        query = response.meta.get('query')
        print("Query used in Page:", query)
        date_published = response.css('time[itemprop="datePublished"]::text').getall()[-1].strip()
        author = response.css('span[itemprop="name"]::text').get()
        provenance = response.url
        headline = response.css('div.page-header h1[itemprop="headline"]::text').get().strip()
        last_modified = response.css('meta[name="last-modified"]::attr(content)').get()
        all_para_tags, headlines  = response.css('div.aticle-text>p').getall(),response.css('div.aticle-text>p>strong::text').getall()
        if len(headlines)==0:
            headlines = response.css('div.aticle-text>p>*>strong::text').getall()
        paragraphs_with_headlines = self.extract_paragraphs_from_article(all_para_tags, headlines)
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
        links = response.css('p.readmore a::attr(href)').getall()
        query  =  self.url_vs_query[response.url] if response.url in self.url_vs_query.keys() else urllib.parse.unquote(response.url[response.url.index("=")+1:])
        for link in links:
            # if self.article_num > self.config["top_k"]:
            #     exit(-1)
            yield scrapy.Request(response.urljoin(link), callback = self.parse_news_page,
            meta = {'query':query})
