# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class NewsCrawlerItem(scrapy.Item):
    """ Model for the scraped items"""
    url = scrapy.Field()
    visited = scrapy.Field()
    published = scrapy.Field()
    last_modified = scrapy.Field()
    keywords = scrapy.Field()
    author = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()
    subheadlines = scrapy.Field()
    body = scrapy.Field()
