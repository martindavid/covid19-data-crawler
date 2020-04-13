import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine(os.getenv('DB_CONN_STRING'))
Session = sessionmaker(bind=engine)

Base = declarative_base()
