import csv
from http import HTTPStatus
import requests
from datetime import datetime
from models.base import Session
from models import RawDataCrawlerTimestamp, JohnHopkinsData


def crawl_data():
    csv_base_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports"
    session = Session()
    # The date where it has fix structure until now
    start_date = datetime(2020, 3, 22)
    end_date = datetime.now()
    for single_date in daterange(start_date, end_date):
        date_to_crawl = single_date.strftime("%m-%d-%Y")
        has_been_crawled = session.query(RawDataCrawlerTimestamp).filter(
            RawDataCrawlerTimestamp.data_date_crawled == single_date).first()
        if not has_been_crawled:
            print(f"[START]Crawl data for {date_to_crawl}")
            csv_file = f"{csv_base_url}/{date_to_crawl}.csv"

            data_to_store = []
            with requests.get(csv_file, stream=True) as f:
                if f.status_code != HTTPStatus.NOT_FOUND:
                    lines = (line.decode('utf-8') for line in f.iter_lines())
                    idx = 0
                    for row in csv.reader(lines):
                        if idx > 0:
                            data_to_store.append(
                                JohnHopkinsData(
                                    fips=(row[0] if row[0] != '' else None),
                                    admin2=row[1],
                                    province_state=row[2],
                                    country_region=row[3],
                                    last_update=row[4],
                                    lat=(row[5] if row[5] != '' else 0.0),
                                    long=(row[6] if row[6] != '' else 0.0),
                                    confirmed=(row[7] if row[7] != '' else 0),
                                    death=(row[8] if row[8] != '' else 0),
                                    recovered=(row[9] if row[9] != '' else 0),
                                    combined_key=row[10]))
                        idx += 1
                    if date_to_crawl != end_date.strftime("%m-%d-%Y"):
                        session.add(
                            RawDataCrawlerTimestamp(
                                data_date_crawled=single_date,
                                crawled_at=datetime.now()))
                    session.add_all(data_to_store)
                    session.flush()
                    session.commit()
                    print(f"[END]Success crawl {idx} data for {date_to_crawl}")
    print("Success crawl raw data from JohnHopkins Repo")


from datetime import timedelta, date


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)
