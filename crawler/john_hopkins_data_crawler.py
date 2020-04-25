import csv
from datetime import timedelta, date
from http import HTTPStatus
import requests
from datetime import datetime
from models.base import Session
from models import RawDataCrawlerTimestamp, JohnHopkinsData
from sqlalchemy import Date, cast


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


class JohnHopkinsDataCrawler():
    def __init__(self):
        self.session = Session()
        # The date where it has fix structure until now
        self.start_date = date(2020, 3, 22)
        self.end_date = date.today()

    def crawl_data(self):
        for single_date in daterange(self.start_date, self.end_date):
            self.crawl_individual_csv(single_date)
        print("Success crawl raw data from JohnHopkins Repo")

    def crawl_individual_csv(self, date_to_crawl: date):
        csv_base_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports"

        date_str = date_to_crawl.strftime("%m-%d-%Y")
        csv_file = f"{csv_base_url}/{date_str}.csv"
        print(f"[START]Crawl data for {date_str}")

        try:
            self.session.query(JohnHopkinsData).delete(synchronize_session=False)

            data_to_store = []
            with requests.get(csv_file, stream=True) as f:
                if f.status_code != HTTPStatus.NOT_FOUND:
                    lines = (line.decode('utf-8') for line in f.iter_lines())
                    idx = 0

                    for row in csv.reader(lines):
                        if idx > 0:
                            try:
                                data_to_store.append(
                                    JohnHopkinsData(
                                        fips=(row[0]
                                              if row[0] != '' else None),
                                        date=date_to_crawl,
                                        admin2=row[1],
                                        province_state=row[2],
                                        country_region=row[3],
                                        last_update=row[4],
                                        lat=(row[5] if row[5] != '' else 0.0),
                                        long=(row[6] if row[6] != '' else 0.0),
                                        confirmed=(row[7]
                                                   if row[7] != '' else 0),
                                        death=(row[8] if row[8] != '' else 0),
                                        recovered=(row[9]
                                                   if row[9] != '' else 0),
                                        combined_key=row[10]))
                            except Exception as e:
                                print(e)
                        idx += 1

                    self.session.add_all(data_to_store)
                    self.session.commit()
                    print(f"[END]Success crawl {idx} data for {date_to_crawl}")
                else:
                    print(f"Can't find data for {date_str}")
        except Exception as e:
            print(e)
