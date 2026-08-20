# Import libraries
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Float, Integer, JSON, String, DateTime

# Define table
Base = declarative_base()
class ClassroomAirQuality(Base):
    __tablename__ = 'classroom_air_quality'

    event_timestamp = Column(DateTime, primary_key=True)
    school_period = Column(String(50), nullable=False)
    student_count_estimated = Column(Integer, nullable=False)
    co2_ppm = Column(Float, nullable=False)
    pm25_ugm3 = Column(Float, nullable=False)
    temperature_c = Column(Float, nullable=False)
    humidity_pct = Column(Float, nullable=False)
    robot_x_pos = Column(Float, nullable=False)
    robot_y_pos = Column(Float, nullable=False)
    ventilation_decision = Column(String(50), nullable=False)
    air_quality_label = Column(String(50), nullable=False)
    raw_data = Column(JSON, nullable=False)