from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from .declarative_base import Base

class Annuity(Base):
  __tablename__ = 'Annuities'

  id = Column(Integer, primary_key=True)
  name = Column(String)
  description = Column(String)

  # Relación con la tabla Input
  inputs = relationship("Input", back_populates="annuity")
