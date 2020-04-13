import csv
import requests
import datapackage
import pandas as pd
from datetime import datetime
from models.base import Session, engine, Base
from models import TimeSeriesData, CountryAggregated, WorldwideAggregated, CrawlerTimestamp, RawDataCrawlerTimestamp, JohnHopkinsData


def init_db():
    Base.metadata.create_all(engine)


def crawl_dataset():
    data_url = 'https://datahub.io/core/covid-19/datapackage.json'

    # to load Data Package into storage
    package = datapackage.Package(data_url)

    # to load only tabular data
    resources = package.resources
    time_series_csv = ""
    country_aggregate_csv = ""
    world_aggregate_csv = ""
    for resource in resources:
        if resource.tabular:
            if resource.descriptor.get(
                    "name") == "time-series-19-covid-combined":
                time_series_csv = resource.descriptor['path']
            if resource.descriptor.get("name") == "countries-aggregated":
                country_aggregate_csv = resource.descriptor['path']
            if resource.descriptor.get("name") == "worldwide-aggregated":
                world_aggregate_csv = resource.descriptor['path']

    print(time_series_csv)
    print(country_aggregate_csv)
    print(world_aggregate_csv)

    session = Session()
    idx = 0
    tsc_data = []
    print("[START]Insert time series data")
    with requests.get(time_series_csv, stream=True) as tsc:
        lines = (line.decode('utf-8') for line in tsc.iter_lines())
        session.query(TimeSeriesData).delete()
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
        session.add_all(tsc_data)
        session.flush()
        session.commit()
    print(f"[END]Insert time series data. Success inserting {idx} records")

    ca_data = []
    idx = 0
    print("[START]Insert country aggregated data")
    with requests.get(country_aggregate_csv, stream=True) as ca:
        lines = (line.decode('utf-8') for line in ca.iter_lines())
        session.query(CountryAggregated).delete()
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
        session.add_all(ca_data)
        session.flush()
        session.commit()
    print(
        f"[END]Insert country aggregated data. Success inserting {idx} records"
    )

    wwa_data = []
    idx = 0
    print("[START]Insert world aggregated data")
    with requests.get(world_aggregate_csv, stream=True) as wwa:
        lines = (line.decode('utf-8') for line in wwa.iter_lines())
        session.query(WorldwideAggregated).delete()
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
        session.add_all(wwa_data)
        session.flush()
        session.commit()
    print(
        f"[END]Insert world aggregated data. Success inserting {idx} records")
    print("Finish run DataHub crawler")


if __name__ == '__main__':
    init_db()
    crawl_dataset()
