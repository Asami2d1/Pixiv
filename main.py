from PixivCrawler import *


if __name__ == '__main__':
    crawler = PixivCrawler(u'津島善子', 'cookie.txt')
    crawler.run(1000)
    database = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'Yy2.718281828',
        'db': 'Crawl',
    }
    crawler.saveall_mysql(database, 'yoshiko_tsushima')
