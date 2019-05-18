import requests
import re
from lxml import etree
import collections
import sqlite3
import time

Irasuto = collections.namedtuple('Irasuto', ['illust_id', 'bookmark_count'])


class PixivCrawler(object):
    _headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/74.0.3729.157 Safari/537.36'
    }
    _raw_url = 'https://www.pixiv.net/search.php?word={}&order=date_d&p={}'

    def __init__(self, keyword, cookiefile):
        self.keyword = keyword
        self.result = set()
        self._read_cookies(cookiefile)

    def _read_cookies(self, filename):
        self._cookies = {}
        with open(filename) as f:
            text = f.read()
            for row in text.split(';'):
                key, value = row.split('=', 1)
                self._cookies[key] = value

    def run(self, max_pages, sleep_time=0):
        page = 1
        prog = re.compile(r'\{"illustId":[^\{\}]*\}|\{"illustId":[^\{\}]*"illustSeries":\{[^\{\}]*\}\}')
        self.irasuto = set()
        while True:
            url = self._raw_url.format(self.keyword, page)
            r = requests.get(url, headers=self._headers, cookies=self._cookies)
            print('Page:{:4d} Status:{:4d}'.format(page, r.status_code))
            selector = etree.HTML(r.text)
            irasuto_category = selector.xpath('//*[@id="js-mount-point-search-result-list"]/@data-items')
            dataItems = irasuto_category[0]
            if dataItems != '[]' and page <= max_pages:
                allIrasuto = prog.findall(dataItems)
                for irasuto in allIrasuto:
                    # print(irasuto)
                    id_search = re.search(r'"illustId":"\d+"', irasuto)
                    if id_search is not None:
                        id = id_search[0].split(':')[1].strip('"')
                    bookmark_search = re.search(r'"bookmarkCount":\d+', irasuto)
                    if bookmark_search is not None:
                        bookmark = bookmark_search[0].split(':')[1]
                    self.irasuto.add(Irasuto(id, bookmark))
            else:
                break
            time.sleep(sleep_time)
            page += 1

    def save(self, database, tablename):
        try:
            conn = sqlite3.connect(database)
            cursor = conn.cursor()
            cursor.execute('SELECT count(*) FROM sqlite_master '
                           'WHERE type=\'{}\' AND name=\'{}\';'.format('table', tablename))
            if cursor.fetchall()[0][0] == 0:
                cursor.execute('CREATE TABLE {} '
                               '('
                               ' id INTEGER PRIMARY KEY AUTOINCREMENT,'
                               ' pixiv_id VARCHAR(10) NOT NULL,'
                               ' bookmark_count INTEGER NOT NULL'
                               ');'.format(tablename))
            for i in self.irasuto:
                insert = 'INSERT INTO {} (pixiv_id, bookmark_count) ' \
                         'VALUES (\'{}\', \'{}\')'.format(tablename, i.illust_id, int(i.bookmark_count))
                cursor.execute(insert)
            cursor.close()
            conn.commit()
            conn.close()
        except RuntimeError as e:
            print('error: ', e)
