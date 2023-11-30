from sqlalchemy import Column, Integer, String, ForeignKey
from .declarative_base import Base
from sqlalchemy.orm import relationship

class Guideline(Base):
    __tablename__ = 'Guidelines'

    id = Column(Integer, primary_key=True, autoincrement=True)
    description = Column(String)
    name = Column(String)
    objective_id = Column(Integer, ForeignKey('Objectives.id'))
    sirap_id = Column(Integer, ForeignKey('Siraps.id'))

    # Relación con la tabla Objective
    objective = relationship("Objective", back_populates="guidelines")
    # Relación con la tabla Sirap
    sirap = relationship("Sirap", back_populates="guidelines")
    # Relación con la tabla Action
    actions = relationship("Action", back_populates="guideline")
