# -*- coding: utf-8 -*-

import os
import sys
from news_crawler.spiders import BaseSpider
from scrapy.spiders import Rule 
from scrapy.linkextractors import LinkExtractor
from datetime import datetime

sys.path.insert(0, os.path.join(os.getcwd(), "..",))
from news_crawler.items import NewsCrawlerItem
from news_crawler.utils import remove_empty_paragraphs


class KlasseGegenKlasseSpider(BaseSpider):
    """Spider for Klasse Gegen Klasse"""
    name = 'klasse_gegen_klasse'
    rotate_user_agent = True
    allowed_domains = ['www.klassegegenklasse.org']
    start_urls = ['https://www.klassegegenklasse.org/']
    
    # Exclude pages without relevant articles 
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'www\.klassegegenklasse\.org\/\w.*\/'),
                    deny=(r'www\.klassegegenklasse\.org\/kategorie\/turkce\/'
                        )
                    ),
                callback='parse_item',
                follow=True
                ),
            )

    def parse_item(self, response):
        """Scrapes information from pages into items"""
     
        # Filter by date
        creation_date = response.xpath('//time/@datetime').get()
        if not creation_date:
            return
        creation_date = datetime.strptime(creation_date, '%Y-%m-%d')
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//p[not(@*) and not(ancestor::div[@class="article-content"])]')]
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
        authors = response.xpath('//div/a/p[preceding-sibling::img[@class="author-img"]]/text()').getall()
        item['author'] = authors if authors else list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        item['last_modified'] = creation_date.strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get().strip()
        description = response.xpath('//meta[@property="og:description"]/@content').get()

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h2[not(@*)]'):
            # Extract headlines
            headlines = [h2.xpath('string()').get() for h2 in response.xpath('//h2[not(@*)]')]
            
            # Remove surrounding quotes from headlines
            processed_headlines = [headline.strip('"') for headline in headlines]
            processed_headlines = [headline.strip('“') for headline in processed_headlines]
          
            # If quote inside headline, keep substring from quote onwards
            processed_headlines = [headline[headline.rindex('"')+1:len(headline)] if '"' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.index('„')+1:len(headline)] if '„' in headline else headline for headline in processed_headlines]
            processed_headlines = [headline[headline.rindex('“')+1:len(headline)] if '“' in headline else headline for headline in processed_headlines]

            # Extract paragraphs between the abstract and the first headline
            body[''] = [node.xpath('string()').get().strip() for node in response.xpath('//div[@class="article-excerpt"]/p')]
            body[''].extend([node.xpath('string()').get().strip() for node in response.xpath('//p[not(@*) and not(ancestor::div[@class="article-content"]) and not(descendant::img) and following-sibling::h2[contains(text(), "' + processed_headlines[0] + '")]] | //blockquote/p[../following-sibling::h2[contains(text(), "' + processed_headlines[0] + '")]]')])

            # Extract paragraphs corresponding to each headline, except the last one
            for i in range(len(headlines)-1):
                body[headlines[i]] = [node.xpath('string()').get().strip() for node in response.xpath('//p[not(@*) and not(ancestor::div[@class="article-content"]) and not(descendant::img) and preceding-sibling::h2[contains(text(), "' + processed_headlines[i] + '")] and following-sibling::h2[contains(text(), "' + processed_headlines[i+1] +'")]] | //blockquote/p[../preceding-sibling::h2[contains(text(), "' + processed_headlines[i] + '")] and ../following-sibling::h2[contains(text(), "' + processed_headlines[i+1] +'")]]')]
           
            # Extract the paragraphs belonging to the last headline
            body[headlines[-1]] = [node.xpath('string()').get().strip() for node in response.xpath('//p[not(@*) and not(ancestor::div[@class="article-content"]) and not(descendant::img) and preceding-sibling::h2[contains(text(), "' + processed_headlines[-1] + '")]] | //blockquote/p[../preceding-sibling::h2[contains(text(), "' + processed_headlines[-1] + '")]]')]
            last_paragraph = response.xpath('//p[not(@*) and not(ancestor::div[@class="article-content"]) and not(descendant::img) and ../preceding-sibling::h2[contains(text(), "' + processed_headlines[-1] + '")] and not(ancestor::blockquote)]')
            if last_paragraph:
                body[headlines[-1]].extend([node.xpath('string()').get().strip() for node in response.xpath('//p[not(@*) and not(ancestor::div[@class="article-content"]) and not(descendant::img) and ../preceding-sibling::h2[contains(text(), "' + processed_headlines[-1] + '")] and not(ancestor::blockquote)]')])

        else:
            # The article has no headlines, just paragraphs
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # No keywords available
        item['keywords'] = list()
        
        # No recommendations related to the article are available
        item['recommendations'] = list()

        # Save article in html format
        save_as_html(response, 'klasse_gegen_klasse.org', title)

        yield item
