from .base import Base
from sqlalchemy import Column, String, Integer, DateTime, Date, Float, Text


class TimeSeriesData(Base):
    __tablename__ = "time_series_data"

    _id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    country = Column(String(255))
    state = Column(String(255))
    lat = Column(Float)
    long = Column(Float)
    confirmed = Column(Integer)
    recovered = Column(Integer)
    death = Column(Integer)


class CountryAggregated(Base):
    __tablename__ = "country_aggregated"

    _id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    country = Column(String(255))
    confirmed = Column(Integer)
    recovered = Column(Integer)
    death = Column(Integer)


class WorldwideAggregated(Base):
    __tablename__ = "worldwide_aggregated"

    _id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    confirmed = Column(Integer)
    recovered = Column(Integer)
    death = Column(Integer)
    increased_rate = Column(Float)


class RawDataCrawlerTimestamp(Base):
    __tablename__ = "raw_data_crawler_timestamp"

    _id = Column(Integer, primary_key=True, autoincrement=True)
    data_date_crawled = Column(Date)
    crawled_at = Column(Date)

    def has_been_crawled(self, date):
        self.data_date_crawled.strftime("%m-%d-%Y") == date


class OWDData(Base):

    __tablename__ = "owd_data"

    _id = Column(Integer, primary_key=True, autoincrement=True)
    iso_code = Column(Text)
    location = Column(Text)
    date = Column(Date)
    total_cases = Column(Integer)
    new_cases = Column(Integer)
    total_deaths = Column(Integer)
    new_deaths = Column(Integer)
    total_cases_per_million = Column(Float, nullable=True)
    new_cases_per_million = Column(Float, nullable=True)
    total_deaths_per_million = Column(Float, nullable=True)
    new_deaths_per_million = Column(Float, nullable=True)
    total_tests = Column(Float, nullable=True)
    new_tests = Column(Float, nullable=True)
    total_tests_per_thousand = Column(Float, nullable=True)
    new_tests_per_thousand = Column(Float, nullable=True)
    tests_unit = Column(Text, nullable=True)
    last_updated = Column(DateTime)



class JohnHopkinsData(Base):

    __tablename__ = "john_hopkins_data"

    _id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    fips = Column(Text, nullable=True)
    admin2 = Column(Text, nullable=True)
    province_state = Column(Text, nullable=True)
    country_region = Column(Text, nullable=True)
    last_update = Column(DateTime, nullable=True)
    lat = Column(Float, default=0.0)
    long = Column(Float, default=0.0)
    confirmed = Column(Integer, default=0)
    death = Column(Integer, default=0)
    recovered = Column(Integer, default=0)
    combined_key = Column(Text, nullable=True)
