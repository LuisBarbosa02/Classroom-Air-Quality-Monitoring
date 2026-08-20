# Import libraries
from .config import DATABASE_URL

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .table import Base

# Create engine
engine = create_engine(DATABASE_URL)

# Create session
SessionLocal = sessionmaker(bind=engine)

# Access or create table
def access_table():
    Base.metadata.create_all(bind=engine)