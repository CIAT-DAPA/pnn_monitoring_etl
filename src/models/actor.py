from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import relationship
from .declarative_base import Base

class Actor(Base):
    __tablename__= 'Actors'

    id = Column(Integer, primary_key=True)
    institution_id = Column(Integer, ForeignKey("Institutions.id"))
    input_id = Column(Integer, ForeignKey("Inputs.id"))

    # Relación con la tabla Actor
    institution = relationship("Institution", back_populates="actors")
    # Relación con la tabla Responsible
    input = relationship("Input", back_populates="actors")