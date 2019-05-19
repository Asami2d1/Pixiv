import requests
import re
from lxml import etree
import collections
import sqlite3
import time
import pymysql


Illust = collections.namedtuple('Illust', ['illustId', 'userId', 'bookmarkCount', 'width', 'height'])


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

    @staticmethod
    def _read_illust(illust: str) -> Illust:
        attr2search = [('illustId', 'str'), ('userId', 'str'),
                       ('bookmarkCount', 'int'), ('width', 'int'), ('height', 'int')]
        result = dict()
        for attr in attr2search:
            if attr[1] == 'str':
                attrsearch = re.search(r'"{}":"\d+"'.format(attr[0]), illust)
                if attrsearch is not None:
                    result[attr[0]] = attrsearch[0].split(':')[1].strip('"')
            elif attr[1] == 'int':
                attrsearch = re.search(r'"{}":\d+'.format(attr[0]), illust)
                if attrsearch is not None:
                    result[attr[0]] = attrsearch[0].split(':')[1]
        return Illust(result['illustId'], result['userId'], result['bookmarkCount'], result['width'], result['height'])

    def run(self, max_pages: int, sleep_time=0):
        page = 1
        prog = re.compile(r'\{"illustId":[^\{\}]*\}|\{"illustId":[^\{\}]*"illustSeries":\{[^\{\}]*\}\}')
        self.illust = set()
        while True:
            url = self._raw_url.format(self.keyword, page)
            r = requests.get(url, headers=self._headers, cookies=self._cookies)
            if r.status_code == 200:
                print('[INFO] Page:{:6d} Status:{:4d}'.format(page, r.status_code))
            else:
                print('[ERROR]Page:{:6d} Status:{:4d}'.format(page, r.status_code))
                break
            selector = etree.HTML(r.text)
            illust_category = selector.xpath('//*[@id="js-mount-point-search-result-list"]/@data-items')
            dataItems = illust_category[0]
            if dataItems != '[]' and page <= max_pages:
                allIllust = prog.findall(dataItems)
                for illust in allIllust:
                    self.illust.add(self._read_illust(illust))
            else:
                break
            time.sleep(sleep_time)
            page += 1

    def saveall_sqlite(self, database, tablename):
        # 储存到本地Sqlite数据库
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
            for i in self.illust:
                insert = 'INSERT INTO {} (pixiv_id, bookmark_count) ' \
                         'VALUES (\'{}\', \'{}\')'.format(tablename, i.illust_id, int(i.bookmark_count))
                cursor.execute(insert)
            cursor.close()
            conn.commit()
            conn.close()
        except RuntimeError as e:
            print('error: ', e)

    def saveall_mysql(self, database: dict, tablename: str):
        # 储存到服务器端数据库
        try:
            conn = pymysql.connect(host=database['host'],
                                   port=database['port'],
                                   user=database['user'],
                                   password=database['password'],
                                   db=database['db'],
                                   charset='utf8')
            cursor = conn.cursor()
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS `{}`
            (
                `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
                `pixiv_id` VARCHAR(15) NOT NULL,
                `user_id` VARCHAR(15),
                `bookmark_count` INTEGER NOT NULL,
                `width` FLOAT,
                `height` FLOAT
            )
            ENGINE=InnoDB DEFAULT CHARSET=utf8;
            '''.format(tablename))
            conn.commit()
            cursor.close()

            for i in self.illust:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM {} WHERE pixiv_id='{}';".format(tablename, i.illustId))
                if cursor.fetchall()[0][0] > 0:
                    cursor.execute('''
                    UPDATE {}
                    SET bookmark_count={},
                        width={},
                        height={}
                    WHERE pixiv_id='{}';
                    '''.format(tablename, int(i.bookmarkCount), int(i.width), int(i.height), i.illustId))
                    conn.commit()
                    cursor.close()
                else:
                    cursor.execute('''
                    INSERT INTO {} (
                        pixiv_id,
                        user_id,
                        bookmark_count,
                        width,
                        height
                    )
                    VALUES (
                        '{}',
                        '{}',
                        {},
                        {},
                        {}
                    );
                    '''.format(tablename, i.illustId, i.userId, int(i.bookmarkCount), int(i.width), int(i.height)))
                    cursor.close()
                    conn.commit()
        except RuntimeError as e:
            print('error: ', e)
        finally:
            conn.close()
