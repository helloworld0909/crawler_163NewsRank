import scrapy
from bs4 import BeautifulSoup
from scrapy_project.items import NewsRankItem
import time


class NewsRankCrawler163(scrapy.Spider):
    name = 'NewsRank'
    start_urls = ['http://news.163.com/rank/']

    def parse(self, response):
        more_links = BeautifulSoup(response.body, 'lxml').select('.more')
        for rank_type in xrange(len(more_links)):
            link = more_links[rank_type].a['href']
            # The first panel is the whole news rank, deal with it separately
            if 'rank_whole' in link:
                yield scrapy.Request(link, self.parse_more, meta={'rank_whole': 1, 'type': rank_type})
            else:
                yield scrapy.Request(link, self.parse_more, meta={'rank_whole': 0, 'type': rank_type})

    def parse_more(self, response):
        soup = BeautifulSoup(response.body, 'lxml')
        news_item = NewsRankItem()
        date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        boxes = soup.select('.tabBox')
        # The first panel is special, deal it separately
        rank_whole = response.meta['rank_whole']
        if rank_whole:
            periods = [('d', 'w', 'm'), ('d', 'w', 'm')]
        else:
            periods = [('h', 'd', 'w'), ('d', 'w', 'm')]
        for i in xrange(len(boxes)):
            time.sleep(1)
            tables = boxes[i].select('.tabContents')
            for j in xrange(len(tables)):
                for newsLink in tables[j].select('a'):
                    news_item['title'] = newsLink.text
                    news_item['url'] = newsLink['href']
                    news_item['date'] = date
                    news_item['type'] = response.meta['type']
                    # Two boxes are pv rank and replies rank
                    if i == 0:
                        news_item['pv'] = newsLink.parent.next_sibling.next_sibling.text
                        news_item['reply'] = ''
                    else:
                        news_item['pv'] = ''
                        news_item['reply'] = newsLink.parent.next_sibling.next_sibling.text
                    # Three columns, in sequence, are d, w, m ranks
                    if i < 2 and j < 3:
                        news_item['period'] = periods[i][j]
                    else:
                        news_item['period'] = 'e'
                    yield news_item
                    # print newsLink.text, newsLink['href']
                    # print newsLink.parent.next_sibling.next_sibling.text
