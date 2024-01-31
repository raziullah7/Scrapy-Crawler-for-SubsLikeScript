# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import logging
from itemadapter import ItemAdapter


class TranscriptscraperPipeline:
    def open_spider(self, spider):
        logging.warning("Spider Opened - Pipeline")
        
    def close_spider(self, spider):
        logging.warning("Spider Closed - Pipeline")
    
    def process_item(self, item, spider):
        return item
