from sqlalchemy import Column, Integer, String, ForeignKey
from .declarative_base import Base
from sqlalchemy.orm import relationship

class Sirap(Base):
    __tablename__ = 'Siraps'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)
    region = Column(String)
    prot_areas = Column(String)

    # Relación con la tabla Guideline
    guidelines = relationship("Guideline", back_populates="sirap")

