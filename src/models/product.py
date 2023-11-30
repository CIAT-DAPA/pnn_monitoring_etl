from sqlalchemy import Column, Integer, String
from .declarative_base import Base
from sqlalchemy.orm import relationship

class Product(Base):
	__tablename__ = 'Products'
	id =  Column(Integer, primary_key=True)
	name = Column(String)
	observation = Column(String)
	
	# Relación con la tabla Input
	details = relationship("Detail", back_populates="product")
