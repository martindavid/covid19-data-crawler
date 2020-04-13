from .base import Base
from sqlalchemy import Column, String, Integer, Date, Float, Text


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


class JohnHopkinsData(Base):

    __tablename__ = "john_hopkins_data"

    _id = Column(Integer, primary_key=True, autoincrement=True)
    fips = Column(Integer, nullable=True)
    admin2 = Column(Text)
    province_state = Column(Text)
    country_region = Column(Text)
    last_update = Column(Date)
    lat = Column(Float)
    long = Column(Float)
    confirmed = Column(Integer, default=0)
    death = Column(Integer, default=0)
    recovered = Column(Integer, default=0)
    combined_key = Column(Text)
