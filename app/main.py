import asyncio
import concurrent

from aiohttp import ClientSession
from models import Base
from settings import settings
from sqlalchemy.ext.asyncio import create_async_engine

from services import (
    download_create_weather_record_service,
    export_weather_records_to_excel,
)


async def main():
    engine = create_async_engine(
        settings.DB_URL,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    session = ClientSession()

    # Создаем пул процессов для асинхронного создания файла Excel
    pool = concurrent.futures.ProcessPoolExecutor(max_workers=1)

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                download_create_weather_record_service(engine=engine, session=session)
            )
            tg.create_task(
                export_weather_records_to_excel(executor=pool, engine=engine)
            )
    except Exception:
        pass
    finally:
        await session.close()
        await engine.dispose()
        pool.shutdown()


asyncio.run(main())
