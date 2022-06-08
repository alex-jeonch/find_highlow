from sqlalchemy import Column, String, INT, FLOAT
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Highlow(Base):
    __tablename__ = 'highlow_point_tb'
    id = Column(INT,nullable=False, autoincrement=True, primary_key=True)
    date = Column(String, nullable=False)
    price = Column(FLOAT, nullable=False)
    point_type = Column(String, nullable=False)
    coin_type = Column(String, nullable=False)
    etc = Column(String, nullable=False, unique=True)