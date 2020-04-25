import csv
import requests
from http import HTTPStatus
from models.base import Session
from models import OWDData
from datetime import datetime

class OWDCrawler():
    def __init__(self):
        self.session = Session()

    def crawl_data(self):
        print(f"[START]Crawl data from OWD Dataset")
        file_url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
        # clean up existing table first
        self.session.query(OWDData).delete(synchronize_session=False)
        with requests.get(file_url, stream=True) as f:
            if f.status_code != HTTPStatus.NOT_FOUND:
                lines = (line.decode('utf-8') for line in f.iter_lines())
                idx = 0
                data_to_store = []
                for row in csv.reader(lines):
                    if idx > 0:
                        data_to_store.append(OWDData(
                            iso_code=row[0],
                            location=row[1],
                            date=row[2],
                            total_cases=row[3],
                            new_cases=row[4],
                            total_deaths=row[5],
                            new_deaths=row[6],
                            total_cases_per_million=parseToFloat(row[7],0.0),
                            new_cases_per_million=parseToFloat(row[8],0.0),
                            total_deaths_per_million=parseToFloat(row[9], 0.0),
                            new_deaths_per_million=parseToFloat(row[10], 0.0),
                            total_tests=parseToFloat(row[11],0.0),
                            new_tests=parseToFloat(row[12], 0.0),
                            total_tests_per_thousand=parseToFloat(row[13], 0.0),
                            new_tests_per_thousand=parseToFloat(row[14], 0.0),
                            tests_unit=row[15],
                            last_updated=datetime.now()
                        ))
                    idx += 1
                self.session.add_all(data_to_store)
                self.session.commit()
                print(f"[END]Success crawl {idx} data from OWD Dataset")


def parseToFloat(data, default):
    return default if data == "" else float(data)
