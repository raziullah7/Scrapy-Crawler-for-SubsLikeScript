# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# import pymongo
import sqlite3
import logging
from itemadapter import ItemAdapter


# class TranscriptscraperPipeline:
# class MongodbPipeline:
#     collection_name = 'transcripts'
    
#     def open_spider(self, spider):
#         self.client = pymongo.MongoClient('mongosh "mongodb+srv://cluster0.d7bhmvz.mongodb.net/" --apiVersion 1 --username razi')
#         self.db = self.client["My_Database"]
#         # logging.warning("Spider Opened - Pipeline")
        
#     def close_spider(self, spider):
#         self.client.close()
#         # logging.warning("Spider Closed - Pipeline")
    
#     def process_item(self, item, spider):
#         self.db[self.collection_name].insert(item)
#         return item


class SQLitePipeline:
    def open_spider(self, spider):
        self.connection = sqlite3.connect('transcripts.db')
        self.c = self.connection.cursor()
        # writing query
        try:
            self.c.execute('''
                    CREATE TABLE transcripts(
                        title TEXT,
                        plot TEXT,
                        transcript TEXT,
                        url TEXT
                    )
            ''')
            self.connection.commit()
        except sqlite3.OperationalError:
            pass


    def close_spider(self, spider):
        self.connection.close()


    def process_item(self, item, spider):
        self.c.execute('''
            INSERT INTO transcripts (title, plot, transcript, url) VALUES (?, ?, ?, ?)
        ''', (
            item.get('title'),
            item.get('plot'),
            item.get('transcript'),
            item.get('url'),
        ))
        self.connection.commit()
        return item
    