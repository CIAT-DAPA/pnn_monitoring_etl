from sqlalchemy import Column, Integer, String
from .declarative_base import Base
from sqlalchemy.orm import relationship

class Objective(Base):
	__tablename__ = 'Objectives'

	id = Column(Integer, primary_key=True)
	name = Column(String)
	description = Column(String)

	# Relación con la tabla Guideline
	guidelines = relationship("Guideline", back_populates="objective")
