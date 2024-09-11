from pydantic import BaseModel, Field, field_serializer
from datetime import datetime, timedelta

from constants import WEATHER_CODE_MAP


class WeatherMinutelySchemaIn(BaseModel):
    """Валидация информации о погоде, разделенной на интервалы по 15 минут."""

    time: list[str]
    temperature_2m: list[float] = Field(..., serialization_alias="temperature")
    precipitation: list[int]
    wind_speed_10m: list[float] = Field(..., serialization_alias="wind_speed")
    wind_direction_10m: list[float] = Field(..., serialization_alias="wind_direction")
    weather_code: list[int] = Field(..., serialization_alias="weather")

    @field_serializer("time")
    def serialize_time(self, time: list[str], _info) -> list[datetime]:
        return [datetime.fromisoformat(i) for i in time]

    @field_serializer("weather_code")
    def serialize_weather_code(self, codes: list[int], _info) -> list[str]:
        return [WEATHER_CODE_MAP.get(i) for i in codes]

    @field_serializer("wind_direction_10m")
    def serialize_wind_direction(self, directions: list[float], _info) -> list[str]:
        result = []
        for direction in directions:
            # Не стал добавлять ЮВ, СЗ и т.д. в рамках тестового задания
            if 315 < direction <= 360 or 0 <= direction <= 45:
                result.append("Север")
            elif 45 < direction <= 135:
                result.append("Восток")
            elif 135 < direction <= 225:
                result.append("Юг")
            elif 225 < direction <= 315:
                result.append("Запад")
        return result


class WeatherHourlySchemaIn(BaseModel):
    """Валидация информации о погоде, разделенной на интервалы по 1 часу"""

    time: list[str]
    surface_pressure: list[float] = Field(..., serialization_alias="pressure")

    @field_serializer("time")
    def serialize_time(self, time: list[str], _info) -> list[datetime]:
        return [datetime.fromisoformat(i) for i in time]

    @field_serializer("surface_pressure")
    def serialize_surface_pressure(self, pressures: list[float], _info) -> list:
        """Переводим в mm рт. ст."""
        result = []
        for pressure in pressures:
            result.append(pressure // 1.333)
        return result


class WeatherRecordCreateSchema(BaseModel):
    """Валидация информации о погоде для создания записи в базе данных."""

    temperature: float
    wind_speed: float
    wind_direction: str
    precipitation: float
    pressure: float
    weather: str


class WeatherRecordSchemaOut(BaseModel):
    """Валидатор для записей о погоде для файла Excel."""

    temperature: float = Field(..., serialization_alias="Температура, °С")
    wind_speed: float = Field(..., serialization_alias="Скорость ветра, м/с")
    wind_direction: str = Field(..., serialization_alias="Напрваление ветра")
    precipitation: float = Field(..., serialization_alias="Осадки, мм")
    pressure: float = Field(..., serialization_alias="Давление, мм. рт. ст.")
    weather: str = Field(..., serialization_alias="Погода")
    created_at: datetime = Field(..., serialization_alias="Дата и время")

    @field_serializer("created_at")
    def serialize_created_at(self, created_at: datetime, _info) -> datetime:
        # Убираем timezone и добавляем три часа, чтобы было по Москве
        # Эксель не поддерживает timezone
        created_at = created_at.replace(tzinfo=None)
        created_at = created_at + timedelta(hours=3)
        return created_at
