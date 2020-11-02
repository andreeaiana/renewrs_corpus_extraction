# -*- coding: utf-8 -*-

import os
import sys
from news_crawler.spiders import BaseSpider
from scrapy.spiders import Rule 
from scrapy.linkextractors import LinkExtractor
from datetime import datetime

sys.path.insert(0, os.path.join(os.getcwd(), "..",))
from news_crawler.items import NewsCrawlerItem
from news_crawler.utils import save_as_html


class FazSpider(BaseSpider):
    """Spider for Frankfurter Allgemeine Zeitung"""
    name = 'faz'
    rotate_user_agent = True
    allowed_domains = ['www.faz.net']
    start_urls = ['https://www.faz.net/']
    
    # Exclude English articles 
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'www\.faz\.net\/\w+\/\w.*\.html$'),
                    deny=(r'www\.faz\.net\/english\/\w.*\.html$',
                        r'www\.faz\.net\/asv\/\w.*\.html$',
                        r'www\.faz\.net\/\w.*\/routenplaner\/\w+\-\d+\.html$'
                        )
                    ),
                callback='parse_item',
                follow=True
                ),
            )

    def parse_item(self, response):
        """Scrapes information from pages into items"""
      
        # Exclude paid articles
        if response.xpath('//div[contains(@class, "PaywallInfo")]').get():
            return

        # Filter by date
        creation_date = response.xpath('//time/@datetime').get()
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//p[@class="First atc-TextParagraph"]')]
        paragraphs.extend([node.xpath('string()').get().strip() for node in response.xpath('//p[@class="atc-TextParagraph"]')])
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
        authors = response.xpath('//div[@class="atc-Meta "]//a/text()').getall()
        item['author'] = authors if authors else list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        item['last_modified'] = creation_date.strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get()
        description = response.xpath('//meta[@property="og:description"]/@content').get()

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h3[@class="atc-SubHeadline"]'):
            # Extract headlines
            headlines = [h3.xpath('string()').get() for h3 in response.xpath('//h3[@class="atc-SubHeadline"]')]
            
            # Remove surrounding quotes from headlines
            processed_headlines = [headline.strip('“') for headline in headlines]
          
            # If quote inside headline, keep substring fro quote onwards
            processed_headlines = [headline[headline.index('„')+1:len(headline)] if '„' in headline else headline for headline in processed_headlines]

            # Extract paragraphs between the abstract and the first headline
            body[''] = [node.xpath('string()').get().strip() for node in response.xpath('//p[@class="First atc-TextParagraph" and following-sibling::h3[contains(text(), "' + processed_headlines[0] + '")]]')]
            body[''].extend([node.xpath('string()').get().strip() for node in response.xpath('//div/p[@class="atc-TextParagraph" and following-sibling::h3[contains(text(), "' + processed_headlines[0] + '")]]')])

            # Extract paragraphs corresponding to each headline, except the last on
            for i in range(len(headlines)-1):
                body[headlines[i]] = [node.xpath('string()').get().strip() for node in response.xpath('//div/p[@class="atc-TextParagraph" and preceding-sibling::h3[contains(text(), "' + processed_headlines[i] + '")] and following-sibling::h3[contains(text(), "' + processed_headlines[i+1] +'")]]')]
           
            # Extract the paragraohs belonging to the last headline
            body[headlines[-1]] = [node.xpath('string()').get().strip() for node in response.xpath('//div/p[@class="atc-TextParagraph" and preceding-sibling::h3[contains(text(), "' + processed_headlines[-1] + '")]]')]

        else:
            # The article has no headlines, just paragraphs
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract keywords
        keywords = response.xpath('//meta[@name="news_keywords"]/@content').get()
        item['keywords'] = keywords.split(', ') if keywords else list()
        
        # Extract first 5 recommendations towards articles from the same news outlet, if available
        recommendations = response.xpath('//div[@class="tsr-Base_TextWrapper  " and ancestor::article[@class="js-tsr-Base tsr-Base tsr-More tsr-Base-has-no-text-border-line  tsr-Base-has-border     "]]/div/div/a/@href').getall() 
        if recommendations:    
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
            item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        # Save article in htmk format
        save_as_html(response, 'faz.net')
        
        # Check if the article continues on the next page
        next_page = response.xpath('//li[@class="nvg-Paginator_Item nvg-Paginator_Item-to-next-page"]/a/@href').get() 
        if next_page:
            yield response.follow(next_page, self.parse_item)

        yield item
