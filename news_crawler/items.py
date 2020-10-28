# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class NewsCrawlerItem(scrapy.Item):
    """ Model for the scraped items"""
    provenance = scrapy.Field()
    author = scrapy.Field()
    creation_date = scrapy.Field()
    last_modified = scrapy.Field()
    scraped_date = scrapy.Field() 
    content = scrapy.Field() # title, description, body
    keywords = scrapy.Field()
    recommendations = scrapy.Field()
    
