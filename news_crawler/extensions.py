# -*- coding: utf-8 -*-

import os
import json
from scrapy import signals
from scrapy.exceptions import NotConfigured


class PersistStatsExtension(object):
    """ 
    Persists spider core stats to json file 

    attr: stats: dict, crawler statistics
    """

    def __init__(self, stats):
        self.stats = stats
    
    @classmethod
    def from_crawler(cls, crawler):
        # Check if the extension is enabled and raise NotConfigured otherwise
        if not crawler.settings.getbool('PERSIST_STATS_ENABLED'):
            raise NotConfigured

        # Instatiate extension object
        ext =  cls(crawler.stats)

        # Connect the extension object to signals
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        # Return the extension object
        return ext

    def spider_opened(self, spider):
        # Check if directory exists for the given spider, and create it if it does not
        folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'data', spider.name)
        if not os.path.isdir(folder):
            os.makedirs(self.folder)
        self.file = open(os.path.join(folder, 'core_stats.json'), 'w')

    def spider_closed(self, spider):
        json.dump(self.stats.get_stats(), self.file, sort_keys=True, default=str)
        self.file.close()
