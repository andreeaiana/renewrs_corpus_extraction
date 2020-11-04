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


class SternSpider(BaseSpider):
    """Spider for Stern"""
    name = 'stern'
    rotate_user_agent = True
    allowed_domains = ['www.stern.de']
    start_urls = ['https://www.stern.de/']

    # Exclude paid and English articles, and pages without relevant articles
    rules = (
            Rule(
                LinkExtractor(
                    allow=(r'stern\.de\/\w.*\.html$'),
                    deny=(r'stern\.de\/p\/plus\/\w.*\.html$',
                        r'stern\.de\/\w.*\/english-version-\w.*\.html$',
                        r'www\.stern\.de\/gutscheine\/'
                        )
                    ),
                callback='parse_item',
                follow=True
                ),
            )

    def parse_item(self, response):
        """Scrapes information from pages into items"""
        
        # Filter by date
        creation_date = response.xpath('//meta[@name="date"]/@content').get()
        if not creation_date:
            return
        creation_date = datetime.fromisoformat(creation_date.split('+')[0])
        if not self.filter_by_date(creation_date):
            return

        # Extract the article's paragraphs
        paragraphs = [node.xpath('string()').get().strip() for node in response.xpath('//div/p[@class="text-element u-richtext u-typo u-typo--article-text article__text-element text-element--context-article" and not(descendant::strong)]')]
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
        authors = response.xpath('//div[@class="authors__text u-typo u-typo--author"]/a/text()').getall()
        item['author'] = authors if authors else list()

        # Get creation, modification, and scraping dates
        item['creation_date'] = creation_date.strftime('%d.%m.%Y')
        last_modified = response.xpath('//meta[@name="last-modified"]/@content').get()
        item['last_modified'] = datetime.fromisoformat(last_modified.split('+')[0]).strftime('%d.%m.%Y')
        item['scraped_date'] = datetime.now().strftime('%d.%m.%Y')
        
        # Get title, description, and body of article
        title = response.xpath('//meta[@property="og:title"]/@content').get().strip()
        description = response.xpath('//meta[@property="og:description"]/@content').get().strip()

        # Body as dictionary: key = headline (if available, otherwise empty string), values = list of corresponding paragraphs
        body = dict()
        if response.xpath('//h2[contains(@class, "subheadline-element")]'):
           # Extract headlines
           headlines = [h2.xpath('string()').get().strip() for h2 in response.xpath('//h2[contains(@class, "subheadline-element")]')]
           
           # Remove surrounding quotes from headlines
           processed_headlines = [headline.strip('"') for headline in headlines]
          
           # If quote inside headline, keep substring from quote onwards
           processed_headlines = [headline[headline.rindex('"')+1:len(headline)] if '"' in headline else headline for headline in processed_headlines]

           # Extract paragraphs between the abstract and the first headline
           body[''] = [node.xpath('string()').get().strip() for node in response.xpath('//div/p[@class="text-element u-richtext u-typo u-typo--article-text article__text-element text-element--context-article" and following-sibling::h2[contains(text(), "' + processed_headlines[0] + '")] and not(descendant::strong)]')]

           # Extract paragraphs corresponding to each headline, except the last one
           for i in range(len(headlines)-1):
               body[headlines[i]] = [node.xpath('string()').get().strip() for node in response.xpath('//div/p[@class="text-element u-richtext u-typo u-typo--article-text article__text-element text-element--context-article" and preceding-sibling::h2[contains(text(), "' + processed_headlines[i] + '")] and following-sibling::h2[contains(text(), "' + processed_headlines[i+1] +'")] and not(descendant::strong)]')]
           
           # Extract the paragraphs belonging to the last headline
           body[headlines[-1]] = [node.xpath('string()').get().strip() for node in response.xpath('//div/p[@class="text-element u-richtext u-typo u-typo--article-text article__text-element text-element--context-article" and preceding-sibling::h2[contains(text(), "' + processed_headlines[-1] + '")] and not(descendant::strong)]')]

        else:
            # The article has no headlines, just paragraphs
            body[''] = [para for para in paragraphs if para != ' ' and para != ""]

        item['content'] = {'title': title, 'description': description, 'body':body}
      
        # Extract keywords, if available
        text = response.body.decode('utf-8') 
        pattern = re.compile('keywords\: \[\"\w.+\,.+\"\]')
        match = pattern.search(text)
        if match:
            keywords = text[match.start():match.end()]
            keywords = keywords.split('[')[1].rsplit(']')[0].split(',')
            item['keywords'] = [keyword.strip('"') for keyword in keywords]
        else:
            item['keywords'] = list()

        # Extract first 5 recommendations towards articles from the same news outlet, if available
        recommendations = response.xpath('//div/article/a[@class="teaser__link "]/@href').getall()
        if recommendations:
            recommendations = ['www.stern.de' + rec for rec in recs if not ('/p/plus' in rec or '/noch-fragen' in rec)]
            if len(recommendations) > 5:
                recommendations = recommendations[:5]
            item['recommendations'] = recommendations
        else:
            item['recommendations'] = list()

        # Save article in htmk format
        save_as_html(response, 'stern.de', title)

        yield item
