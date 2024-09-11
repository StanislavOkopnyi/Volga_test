import asyncio
from concurrent.futures import Executor
import functools
import sys
from aiohttp import ClientSession
from pydantic import BaseModel
from sqlalchemy import Engine, select, text
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from pandas import DataFrame

from models import WeatherRecord
from schema import (
    WeatherHourlySchemaIn,
    WeatherMinutelySchemaIn,
    WeatherRecordCreateSchema,
    WeatherRecordSchemaOut,
)
from settings import settings


DEFAULT_WEATHER_URL = r"https://api.open-meteo.com/v1/forecast?latitude=55.6878&longitude=37.3684&minutely_15=temperature_2m,precipitation,weather_code,wind_speed_10m,wind_direction_10m&hourly=surface_pressure&timezone=Europe%2FMoscow&past_minutely_15=1&forecast_days=1&forecast_minutely_15=4"


class AiohttpWeatherService:
    """Сервис для запроса информации о погоде"""

    def __init__(self, url: str | None = None) -> None:
        self.url = url or DEFAULT_WEATHER_URL

    async def __call__(self, session: ClientSession):
        async with session.get(self.url) as response:
            return await response.json()


class ValidationService:
    """Сервис для валидации словарей при помощи моделей Pydantic."""

    def __init__(self, pydantic_class: type[BaseModel]):
        self.pydantic_class = pydantic_class

    def __call__(self, *args, **kwargs) -> dict:
        return self.pydantic_class(**kwargs, strict=True).model_dump(by_alias=True)


class DatabaseRepository:
    """Репозиторий для взаимодействия с базой данных."""

    def __init__(self, db_table: type[DeclarativeBase]):
        self.db_table = db_table

    async def create(self, engine: Engine, *args, **kwargs) -> None:
        async with AsyncSession(engine) as session:
            model_to_create = self.db_table(**kwargs)
            session.add(model_to_create)
            await session.commit()

    async def get_all(self, engine: Engine, *args, **kwargs):
        async with AsyncSession(engine) as session:
            order_by = kwargs.get("order_by")
            limit = kwargs.get("limit")
            models = await session.scalars(
                select(self.db_table)
                .order_by(text(order_by if order_by else "id"))
                .limit(limit)
            )
        return models.all()


class CreateRecordService:
    """Сервис по созданию записей."""

    def __init__(
        self, validation_service: ValidationService, repository: DatabaseRepository
    ):
        self.validation_service = validation_service
        self.repository = repository

    async def __call__(self, engine: Engine, *args, **kwargs):
        validated_data = self.validation_service(**kwargs)
        await self.repository.create(engine=engine, **validated_data)


class GetAllRecordService:
    """Сервис по получению всех записей."""

    def __init__(
        self, validation_service: ValidationService, repository: DatabaseRepository
    ):
        self.validation_service = validation_service
        self.repository = repository

    async def __call__(
        self,
        engine: Engine,
        order_by: str | None = None,
        limit: str | None = None,
        *args,
        **kwargs
    ) -> list[dict]:
        data = await self.repository.get_all(
            engine=engine, order_by=order_by, limit=limit
        )
        return [self.validation_service(**i.to_dict()) for i in data]


class DownloadCreateWeatherRecordService:
    """Сервис для загрузки данных о погоде из open-meteo в базу данных."""

    def __init__(
        self,
        weather_hourly_validation_service,
        weather_minutely_validation_service,
        weather_record_create_service,
        weather_aiohttp_service,
    ) -> None:
        self.weather_hourly_validation_service = weather_hourly_validation_service
        self.weather_minutely_validation_service = weather_minutely_validation_service
        self.weather_record_create_service = weather_record_create_service
        self.weather_aiohttp_service = weather_aiohttp_service

    async def __call__(self, session: ClientSession, engine: Engine):
        while True:
            response = await self.weather_aiohttp_service(session=session)

            hourly_data = self.weather_hourly_validation_service(**response["hourly"])
            hourly_data = self._get_current_hour_data(hourly_data)
            minutely_data = self.weather_minutely_validation_service(
                **response["minutely_15"]
            )
            minutely_data = self._get_current_minute_data(minutely_data)

            create_data = minutely_data | hourly_data

            await weather_record_create_service(engine=engine, **create_data)
            await asyncio.sleep(settings.TIME_TO_REQUEST)

    def _get_current_hour_data(self, data: dict) -> dict[str, float]:
        """Получаем информацию, актуальную для данного часа."""
        for time, pressure in zip(data["time"], data["pressure"]):
            if time.hour == datetime.now().hour:
                return {"pressure": pressure}

    def _get_current_minute_data(self, data: dict) -> dict:
        """Получаем информацию, актуальную для данной минуты."""
        # Находим наиболее близкое время
        index_min_tuple = (0, float("inf"))
        for index, time in enumerate(data["time"]):
            delta_minutes = abs(time.minute - datetime.now().minute)
            if delta_minutes < index_min_tuple[1]:
                index_min_tuple = (index, delta_minutes)

        current_time_index = index_min_tuple[0]

        # Отдаем результат с этим временем
        result = {}
        for key, value in data.items():
            result[key] = value[current_time_index]

        return result


class ExportWeatherRecordsToExcel:
    """Сервис для экспорта записей о погоде в Excel."""

    def __init__(self, weather_get_all_service) -> None:
        self.weather_get_all_service = weather_get_all_service

    async def __call__(self, executor: Executor, engine: Engine):
        while True:
            input_data = await self.ainput()
            if input_data.strip().lower() not in ["да", "yes", "y", "д"]:
                continue

            weather_records = await self.weather_get_all_service(
                engine=engine, order_by="id desc", limit=10
            )
            data_frame = DataFrame(weather_records)

            # Запускаю в отдельном процессе выгрузку данных
            loop = asyncio.get_running_loop()
            loop.run_in_executor(
                executor=executor,
                func=functools.partial(
                    data_frame.to_excel,
                    "test.xlsx",
                    sheet_name="weather_records",
                    index=False,
                ),
            )

    async def ainput(self) -> str:
        """Асинхронный input."""
        print("Скопировать последние десять записей в Excel файл? (Д/н)")
        return await asyncio.to_thread(sys.stdin.readline)


weather_hourly_validation_service = ValidationService(WeatherHourlySchemaIn)
weather_minutely_validation_service = ValidationService(WeatherMinutelySchemaIn)
weather_record_create_validation_service = ValidationService(WeatherRecordCreateSchema)
weather_out_validation_service = ValidationService(WeatherRecordSchemaOut)

weather_database_repository = DatabaseRepository(WeatherRecord)

weather_record_create_service = CreateRecordService(
    weather_record_create_validation_service, weather_database_repository
)
weather_record_get_all_service = GetAllRecordService(
    weather_out_validation_service, weather_database_repository
)

weather_aiohttp_service = AiohttpWeatherService()

download_create_weather_record_service = DownloadCreateWeatherRecordService(
    weather_hourly_validation_service,
    weather_minutely_validation_service,
    weather_record_create_service,
    weather_aiohttp_service,
)
export_weather_records_to_excel = ExportWeatherRecordsToExcel(
    weather_record_get_all_service
)
