# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb


class NewsRankPipeline(object):
    def open_spider(self, spider):
        self.conn = MySQLdb.connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='960423',
            db='news_rank',
            charset="utf8"
        )
        self.cur = self.conn.cursor()
        self.cur.execute('''create table if not exists news_rank(
        url varchar(100), title varchar(100), period char(1), type int,
        pv varchar(10), reply varchar(10), date date, PRIMARY KEY (url))''')

    def close_spider(self, spider):
        self.conn.commit()
        self.conn.close()

    def process_item(self, item, spider):
        col = ','.join(item.keys())
        placeholders = ','.join(len(item) * ['%s'])
        sql = 'insert into ignore news_rank({}) values({})'
        self.cur.execute(sql.format(col, placeholders), item.values())
        return item
