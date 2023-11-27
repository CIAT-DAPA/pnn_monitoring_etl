from sqlalchemy import Column, Integer, String, ForeignKey, Double, Date
from .declarative_base import Base
from sqlalchemy.orm import relationship

class Input(Base):
    __tablename__ = 'Inputs'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    amount = Column(Double)
    quantity = Column(Integer)
    date = Column(Date)
    goat = Column(Integer)
    period_id = Column(Integer, ForeignKey("Periods.id"))
    annuity_id = Column(Integer, ForeignKey("Annuities.id"))
    product_id = Column(Integer, ForeignKey("Products.id"))
    implemented_value = Column(Double)
    milestone_id = Column(Integer, ForeignKey("Milestones.id"))

    # Relación con la tabla Period
    period = relationship("Period", back_populates="inputs")
    # Relación con la tabla Annuity
    annuity = relationship("Annuity", back_populates="inputs")
    # Relación con la tabla Product
    product = relationship("Product", back_populates="inputs")
    # Relación con la tabla Milestone
    milestone = relationship("Milestone", back_populates="inputs")

    # Relación con la tabla Actor
    actors = relationship("Actor", back_populates="input")
    # Relación con la tabla Responsible
    responsibles = relationship("Responsible", back_populates="input")


