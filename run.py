from datetime import datetime
from models.base import engine, Base
from crawler import datahub_crawler as dhcrl, john_hopkins_data_crawler as jhcrl


def init_db():
    Base.metadata.create_all(engine)


def crawl_dataset():

    dhcrl.crawl_datahub()
    jhcrl.crawl_data()


if __name__ == '__main__':
    init_db()
    crawl_dataset()
