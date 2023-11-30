from sqlalchemy import Column, Integer, String, ForeignKey
from .declarative_base import Base
from sqlalchemy.orm import relationship

class Milestone(Base):
	__tablename__ = 'Milestones'
	
	id = Column(Integer, primary_key=True)
	name = Column(String)
	description = Column(String)
	producto_indicator = Column(String)
	action_id = Column(Integer, ForeignKey('Actions.id'))

	# Relación con la tabla Action
	action = relationship("Action", back_populates="milestones")
	# Relación con la tabla Input
	details = relationship("Detail", back_populates="milestone")

