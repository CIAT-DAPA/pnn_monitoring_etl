from sqlalchemy import Column, Integer, String
from .declarative_base import Base
from sqlalchemy.orm import relationship

class Period(Base):
	__tablename__ = 'Periods'
	id =  Column(Integer, primary_key=True)
	name = Column(String)
	
	# Relación con la tabla Input
	inputs = relationship("Input", back_populates="period")
