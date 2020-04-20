import csv
import datapackage
import requests
from models.base import Session, engine, Base
from models import TimeSeriesData, CountryAggregated, WorldwideAggregated, RawDataCrawlerTimestamp, JohnHopkinsData


class DatahubCrawler():
    def __init__(self):
        data_url = 'https://datahub.io/core/covid-19/datapackage.json'

        # to load Data Package into storage
        package = datapackage.Package(data_url)

        # to load only tabular data
        resources = package.resources
        self.time_series_csv = ""
        self.country_aggregate_csv = ""
        self.world_aggregate_csv = ""
        self.session = Session()
        print("Fetching dataset from datahub")
        for resource in resources:
            if resource.tabular:
                if resource.descriptor.get(
                        "name") == "time-series-19-covid-combined":
                    self.time_series_csv = resource.descriptor['path']
                if resource.descriptor.get("name") == "countries-aggregated":
                    self.country_aggregate_csv = resource.descriptor['path']
                if resource.descriptor.get("name") == "worldwide-aggregated":
                    self.world_aggregate_csv = resource.descriptor['path']

    def crawl_data(self):
        self.crawl_time_series_data(self.time_series_csv)
        self.crawl_country_aggregated_data(self.country_aggregate_csv)
        self.crawl_world_aggregated_data(self.world_aggregate_csv)

    def crawl_time_series_data(self, file_url: str):
        idx = 0
        tsc_data = []
        print("[START]Insert time series data")
        print(f"Crawl data using {self.time_series_csv}")
        with requests.get(file_url, stream=True) as tsc:
            lines = (line.decode('utf-8') for line in tsc.iter_lines())
            self.session.query(TimeSeriesData).delete()
            for row in csv.reader(lines):
                if idx > 0 and len(row) > 0:
                    confirmed = (row[5] if row[5] != '' else '0')
                    recovered = (row[6] if row[6] != '' else '0')
                    death = (row[7] if row[7] != '' else '0')
                    tsc_data.append(
                        TimeSeriesData(date=row[0],
                                       country=row[1],
                                       state=row[2],
                                       lat=row[3],
                                       long=row[4],
                                       confirmed=confirmed,
                                       recovered=recovered,
                                       death=death))
                idx += 1
            self.session.add_all(tsc_data)
            self.session.commit()
        print(f"[END]Insert time series data. Success inserting {idx} records")

    def crawl_country_aggregated_data(self, file_url: str):
        ca_data = []
        idx = 0
        print("[START]Insert country aggregated data")
        print(f"Crawl data using {self.country_aggregate_csv}")
        with requests.get(file_url, stream=True) as ca:
            lines = (line.decode('utf-8') for line in ca.iter_lines())
            self.session.query(CountryAggregated).delete()
            for row in csv.reader(lines):
                if idx > 0 and len(row) > 0:
                    confirmed = (row[2] if row[2] != '' else '0')
                    recovered = (row[3] if row[3] != '' else '0')
                    death = (row[4] if row[4] != '' else '0')
                    ca_data.append(
                        CountryAggregated(date=row[0],
                                          country=row[1],
                                          confirmed=confirmed,
                                          recovered=recovered,
                                          death=death))
                idx += 1
            self.session.add_all(ca_data)
            self.session.commit()
        print(
            f"[END]Insert country aggregated data. Success inserting {idx} records"
        )

    def crawl_world_aggregated_data(self, file_url: str):
        wwa_data = []
        idx = 0
        print("[START]Insert world aggregated data")
        print(f"Crawl data using {self.world_aggregate_csv}")
        with requests.get(file_url, stream=True) as wwa:
            lines = (line.decode('utf-8') for line in wwa.iter_lines())
            self.session.query(WorldwideAggregated).delete()
            for row in csv.reader(lines):
                if idx > 0 and len(row) > 0:
                    confirmed = (row[1] if row[1] != '' else '0')
                    recovered = (row[2] if row[2] != '' else '0')
                    death = (row[3] if row[3] != '' else '0')
                    wwa_data.append(
                        WorldwideAggregated(date=row[0],
                                            confirmed=confirmed,
                                            recovered=recovered,
                                            death=death))
                idx += 1
            self.session.add_all(wwa_data)
            self.session.commit()
        print(
            f"[END]Insert world aggregated data. Success inserting {idx} records"
        )
        print("Finish run DataHub crawler")
