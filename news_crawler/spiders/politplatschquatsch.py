# -*- coding: utf-8 -*-

import os
import sys
import dateparser
from news_crawler.spiders import BaseSpider
from scrapy.spiders import Rule 
from scrapy.linkextractors import LinkExtractor
from datetime import datetime

sys.path.insert(0, os.path.join(os.getcwd(), "..",))
from news_crawler.items import NewsCrawlerItem
from news_crawler.utils import save_as_html


class PolitplatschquatschSpider(BaseSpider):
    """Spider for politplatschquatsch"""
    name = 'politplatschquatsch'
    rotate_user_agent = True
    allowed_domains = ['www.politplatschquatsch.com']
    start_urls = ['https://www.politplatschquatsch.com/']
    
    # Exclude pages without relevant articles 
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'www\.politplatschquatsch\.com\/\d+\/\w.*\.html$')
                    ),
                callback='parse_item',
                follow=True
                ),
            )

    def parse_item(self, response):
        """Scrapes information from pages into items"""

        # Filter by date
        creation_date = response.xpath('//h2[@class="date-header"]/span/text()').get()
        if not creation_date:
            return
        creation_date = creation_date.split(', ')[-1]
        creation_date = dateparser.parse(creation_date)
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's content
        raw_paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//div[@itemprop="articleBody" and descendant::text()]')]

        if not '\n\n' in raw_paragraphs[0]:
            # Handle non-breaking spaces
            raw_paragraphs = raw_paragraphs[0].replace('.\xa0', '.\n')
            raw_paragraphs = raw_paragraphs.split('.\n')
            raw_paragraphs = [para.replace('\n', '') for para in raw_paragraphs]
            raw_paragraphs = [para.replace('\xa0', '') for para in raw_paragraphs]
            paragraphs = [para.strip() for para in raw_paragraphs]
            paragraphs = [para for para in paragraphs if para != '' and para != ' ']
            text = ' '.join([para for para in paragraphs])
 
        else:
            # Split text into paragraphs
            raw_paragraphs = raw_paragraphs[0].split('\n\n')
            raw_paragraphs = [para.strip() for para in raw_paragraphs]

            paragraphs = list()
            for para in raw_paragraphs:
                if '\n' in para:
                    paragraphs.extend(para.split('\n'))
                else:
                    paragraphs.append(para)
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
        
        # No authors listed
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
        
        # Headlines are not handled consistently
        body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # No keywords available
        item['keywords'] = list()
        
        # No recommendations related to the article are available
        item['recommendations'] = list()

        # Save article in html format
        save_as_html(response, 'politplatschquatsch.com', title)

        yield item
