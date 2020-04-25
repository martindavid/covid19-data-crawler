from datetime import datetime
from models.base import engine, Base, Session
from models import CrawlerTimestamp
from crawler.datahub_crawler import DatahubCrawler
from crawler.john_hopkins_data_crawler import JohnHopkinsDataCrawler
from crawler.owd_crawler import OWDCrawler



def init_db():
    Base.metadata.create_all(engine)


def crawl_dataset():

    dh_crawler = DatahubCrawler()
    dh_crawler.crawl_data()

    owd_crawler = OWDCrawler()
    owd_crawler.crawl_data()

    jh_crawler = JohnHopkinsDataCrawler()
    jh_crawler.crawl_data()

    update_crawler_timestamp()


def update_crawler_timestamp():
    session = Session()
    session.add(CrawlerTimestamp(crawled_at=datetime.now()))
    session.commit()



if __name__ == '__main__':
    init_db()
    crawl_dataset()
