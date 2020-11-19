import argparse
import scrapy
from scrapy.crawler import CrawlerProcess
from spiders import *
import json


def read_config():
    with open('config.json') as json_file:
        config = json.load(json_file)

    return config

if __name__ == "__main__":
  config  = read_config()
  print(config)
  process = CrawlerProcess()
  process.crawl(epochtimes_spider.Epochtimes, config = config)
  process.start()
