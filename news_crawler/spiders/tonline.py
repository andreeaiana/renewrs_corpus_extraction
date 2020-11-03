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
from news_crawler.utils import save_as_html


class TonlineSpider(BaseSpider):
    """Spider for t-online"""
    name = 'tonline'
    rotate_user_agent = True
    allowed_domains = ['www.t-online.de']
    start_urls = ['https://www.t-online.de/']
    
    # Exclude English articles and pages without relevant articles  
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'www\.t-online\.de\/\w.*'),
                    deny=(r'www\.t-online\.de\/spiele\/',
                        r'www\.t-online\.de\/wetter\/',
                        r'www\.t-online\.de\/tv\/',
                        r'www\.t-online\.de\/podcasts\/',
                        r'www\.t-online\.de\/sport\/live-ticker\/',
                        r'www\.t-online\.de\/computer\/browser\/',
                        r'www\.t-online\.de\/\w.*\/quiz\-\w.*',
                        r'www\.t-online\.de\/\w.*\-lottozahlen\-\w.*',
                        r'lotto\.t-online\.de\/',
                        r'telefonbuch\.t-online\.de\/',
                        r'tarife-und-produkte\.t-online\.de\/',
                        r'jobsuche\.t-online\.de\/',
                        r'horoskop\.t-online\.de\/'
                        )
                    ),
                callback='parse_item',
                follow=True
                ),
            )

    def parse_item(self, response):
        """Scrapes information from pages into items"""
      
        # Filter by date
        creation_date = response.xpath('//meta[@itemprop="datePublished"]/@content').get()
        if not creation_date:
            return
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//div[@itemprop="articleBody"]/p[not(preceding-sibling::h2[@itemprop="alternativeHeadline"]) and not(descendant::b) and not(descendant::span[@class="Tiflle"])]')]
        text = ' '.join([para for para in paragraphs if para != ' ' and para != ""])

        # Filter by article length
        if not self.filter_by_length(text):
            return

        # Filter by keywords
        if not self.filter_by_keywords(text):
            return

        # Parse the article
        item = NewsCrawlerItem()
        item['provenance'] = response.url
        
        # Get authors
        data_json = response.xpath('//script[@type="application/ld+json"]/text()').get()
        data = json.loads(data_json)
        data_authors = data['author']
        if data_authors:
            authors = [data_authors[i]['name'] for i in range(len(data_authors)) if data_authors[i]['@type'] == 'Person']
            item['author'] = [author for author in authors if author != ""] 
        else:
            item['author'] = list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        item['last_modified'] = creation_date.strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get()
        description = response.xpath('//meta[@property="og:description"]/@content').get()

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h3[not(@*)]'):
            # Extract headlines
            headlines = [h3.xpath('string()').get().strip() for h3 in response.xpath('//h3[not(@*)]')]
            
            # Remove surrounding quotes from headlines
            processed_headlines = [headline.strip('"') for headline in headlines]
          
            # I quote inside headline, keep substring fro quote onwards
            processed_headlines = [headline[headline.index('"')+1:len(headline)] if '"' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.index('"')+1:len(headline)] if '"' in headline else headline for headline in processed_headlines]

            # Extract paragraphs between the abstract and the first headline
            body[''] = [node.xpath('string()').get().strip() for node in response.xpath('//div[@itemprop="articleBody"]/p[not(preceding-sibling::h2[@itemprop="alternativeHeadline"]) and not(descendant::b) and not(descendant::span[@class="Tiflle"]) and following-sibling::h3[contains(text(), "' + processed_headlines[0] + '")]]')]

            # Extract paragraphs corresponding to each headline, except the last one
            for i in range(len(headlines)-1):
                body[headlines[i]] = [node.xpath('string()').get().strip() for node in response.xpath('//div[@itemprop="articleBody"]/p[not(preceding-sibling::h2[@itemprop="alternativeHeadline"]) and not(descendant::b) and not(descendant::span[@class="Tiflle"]) and preceding-sibling::h3[contains(text(), "' + processed_headlines[i] + '")] and following-sibling::h3[contains(text(), "' + processed_headlines[i+1] +'")]]')]
           
            # Extract the paragraohs belonging to the last headline
            body[headlines[-1]] = [node.xpath('string()').get().strip() for node in response.xpath('//div[@itemprop="articleBody"]/p[not(preceding-sibling::h2[@itemprop="alternativeHeadline"]) and not(descendant::b) and not(descendant::span[@class="Tiflle"]) and preceding-sibling::h3[contains(text(), "' + processed_headlines[-1] + '")]]')]

        else:
            # The article has no headlines, just paragraphs
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract keywords
        keywords = response.xpath('//meta[@name="news_keywords"]/@content').get()
        item['keywords'] = keywords.split(', ') if keywords else list()
        
        # Extract first 5 recommendations towards articles from the same news outlet, if available
        recommendations = response.xpath('//ul[preceding-sibling::p[contains(text(), "Mehr zum Thema")]]/li/a/@href').getall()
        if recommendations:    
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
            item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        # Save article in htmk format
        save_as_html(response, 'tonline.de', title)
        
        yield item
