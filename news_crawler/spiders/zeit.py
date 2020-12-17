# -*- coding: utf-8 -*-

import os
import sys
import json
from news_crawler.spiders import BaseSpider
from scrapy import signals
from scrapy import Selector
from scrapy.spiders import Rule 
from scrapy.linkextractors import LinkExtractor
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

sys.path.insert(0, os.path.join(os.getcwd(), "..",))
from news_crawler.items import NewsCrawlerItem
from news_crawler.utils import remove_empty_paragraphs


class ZeitSpider(BaseSpider):
    """Spider for Zeit"""
    name = 'zeit'
    rotate_user_agent = True
    allowed_domains = ['www.zeit.de']
   # start_urls = ['https://www.zeit.de/index']
    start_urls = ['https://www.zeit.de/gesellschaft/zeitgeschehen/2020-09/heinrich-bedford-strohm-ekd-kirchenasyl-lockerungen']
    # Exclude English articles and pages without relevant articles  
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'www\.zeit\.de\/\w.*'),
                    deny=(r'www\.zeit\.de\/english\/',
                        r'www\.zeit\.de\/exklusive-zeit-artikel\/',
                        r'www\.zeit\.de\/angebote\/',
                        r'www\.zeit\.de\/zeit-magazin\/',
                        r'www\.zeit\.de\/podcasts\/',
                        r'www\.zeit\.de\/hilfe\/',
                        r'www\.zeit\.de\/video\/',
                        r'www\.zeit\.de\/spiele\/',
                        r'www\.zeit-verlagsgruppe\.de\/',
                        r'www\.zeit\.de\/administratives\/',
                        r'www\.zeit\.de\/zustimmung\?',
                        r'www\.zeit\.de\/autoren\/',
                        r'www\.zeit\.de\/impressum'
                        )
                    ),
                callback='parse_item',
                follow=True
                ),
            )
    
    def __init__(self):
        BaseSpider.__init__(self)
        # Initialize Chrome driver
        self.driver = webdriver.Chrome()
        self.driver.get('https://www.zeit.de/index')
        self.driver.implicitly_wait(30)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ZeitSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        self.driver.quit()
    
    def parse_item(self, response):
        """
        Checks article validity. If valid, it parses it.
        """

        # Get web page
        self.driver.get(response.url)
        self.driver.implicitly_wait(2)
        # Switch to the iframe containing the button to close pop-up window
        self.driver.switch_to.frame(self.driver.find_element_by_tag_name('iframe'))
               
        # Extract the xpath of the button that needs to be clicked and click it
        WebDriverWait(self.driver, 20).until(EC.element_to_be_clickable((By.XPATH, "/html/body/div/div[3]/button"))).click()
        # Pass the new response back to scrapy for further processing
        sel = Selector(text=self.driver.page_source)
        provenance = self.driver.current_url
        print('\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n')
        print(self.driver.page_source)
        print(sel.xpath('//meta[@name="date"]').get())
        print('\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n')
            

        # Exclude paid articles
        if response.xpath('//aside[@id="paywall"]').get():
            return

        # Check date validity
        creation_date = response.xpath('//meta[@name="date"]/@content').get()
        if not creation_date:
            return
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if self.is_out_of_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//p[@class="paragraph article__item"]')]
        paragraphs = remove_empty_paragraphs(paragraphs)
        text = ' '.join([para for para in paragraphs])

        # Check by article's length validity
        if not self.has_min_length(text):
            return

        # Check keywords validity
        if not self.has_valid_keywords(text):
            return

        # Parse the article
        item = NewsCrawlerItem()

        item['news_outlet'] = 'zeit'
        item['provenance'] = provenance
        item['query_keywords'] = self.get_query_keywords()

        # Get authors
        # Extract person authors
        author_person_url = response.xpath('//meta[@property="article:author"]/@content').getall()
        item['author_person'] = [author.split('/')[-2].replace('_', ' ') for author in author_person_url] if author_person_url else list()
        
        # Extract any organization listed as author
        data_json = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        if data_json:
            data = json.loads(data_json[-1])
            authors = data['author'] if type(data['author'])==list else [data['author']]         
            item['author_organization'] = [author['name'] for author in authors if not author['url'] in author_person_url]
        else:
            item['author_organization'] = list()

        # Get creation, modification, and crawling dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        last_modified = response.xpath('//meta[@name="last-modified"]/@content').get()
        item['last_modified'] = datetime.fromisoformat(last_modified[:-1]).strftime('%d.%m.%Y')
        item['crawl_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Extract keywords
        news_keywords = response.xpath('//meta[@name="keywords"]/@content').get()
        item['news_keywords'] = news_keywords.split(', ') if news_keywords else list()
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get()
        description = response.xpath('//meta[@property="og:description"]/@content').get()

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h2[@class="article__subheading article__item "]'):
            # Extract headlines
            headlines = [h2.xpath('string()').get().strip() for h2 in response.xpath('//h2[@class="article__subheading article__item "]')]
            
            # Extract paragraphs with headlines
            text = [node.xpath('string()').get().strip() for node in response.xpath('//p[@class="paragraph article__item"] | //h2[@class="article__subheading article__item "]')]

            # Extract paragraphs between the abstract and the first headline
            body[''] = remove_empty_paragraphs(text[:text.index(headlines[0])])

            # Extract paragraphs corresponding to each headline, except the last one
            for i in range(len(headlines)-1):
                body[headlines[i]] = remove_empty_paragraphs(text[text.index(headlines[i])+1:text.index(headlines[i+1])])

            # Extract the paragraphs belonging to the last headline
            body[headlines[-1]] = remove_empty_paragraphs(text[text.index(headlines[-1])+1:])

        else:
            # The article has no headlines, just paragraphs
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract first 5 recommendations towards articles from the same news outlet, if available
        recommendations = response.xpath('//article[@class="topicbox-item"]/a/@href').getall() 
        if recommendations:    
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
            item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        item['response_body'] = response.body
        
#        # Check if the article continues on the next page
 #       next_page = response.xpath('//nav[@class="article-pagination article__item"]/a[@data-ct-label="Nächste Seite"]/@href').get() 
  #      if next_page:
   #         yield response.follow(next_page, self.parse_item)

        yield item
