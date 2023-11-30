from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import relationship
from .declarative_base import Base

class Actor(Base):
    __tablename__= 'Actors'


    id = Column(Integer, primary_key=True)
    institution_id = Column(Integer, ForeignKey("Institutions.id"))
    detail_id = Column(Integer, ForeignKey("Details.id"))

    # Relación con la tabla Actor
    institution = relationship("Institution", back_populates="actors")
    # Relación con la tabla Responsible
    detail = relationship("Detail", back_populates="actors")