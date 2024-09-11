from sqlalchemy import inspect, Column, Integer, DateTime
from sqlalchemy.sql import func
from sqlalchemy.orm import DeclarativeBase, Mapped


class Base(DeclarativeBase):

    def to_dict(self):
        """Функция для представления атрибутов модели в виде словаря."""

        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}


class WeatherRecord(Base):
    """Представление записи о погоде в базе данных."""

    __tablename__ = "weather_record"

    id = Column(Integer, primary_key=True)
    temperature: Mapped[int]
    wind_speed: Mapped[float]
    wind_direction: Mapped[str]
    precipitation: Mapped[int]
    weather: Mapped[str]
    pressure: Mapped[float]
    created_at = Column(DateTime(timezone=True), server_default=func.now())
