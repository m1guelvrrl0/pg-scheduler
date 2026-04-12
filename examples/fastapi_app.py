from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI
from pg_scheduler import Scheduler, periodic
import asyncpg
import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@periodic(every=timedelta(minutes=5))
async def cleanup_stale_sessions():
    logger.info("Cleaning up stale sessions")
    await asyncio.sleep(0.5)
    logger.info("Stale sessions cleaned")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = await asyncpg.create_pool(
        host="db",
        port=5432,
        user="scheduler",
        password="scheduler123",
        database="scheduler_db",
    )
    app.state.scheduler = Scheduler(
        app.state.db_pool,
        max_concurrent_jobs=20,
        misfire_grace_time=120,
    )
    await app.state.scheduler.start()
    yield
    await app.state.scheduler.shutdown()
    await app.state.db_pool.close()


app = FastAPI(lifespan=lifespan)


async def process_order(order_id: str, delay: float = 1.0):
    logger.info(f"Processing order {order_id}")
    await asyncio.sleep(delay)
    logger.info(f"Order {order_id} processed")


@app.post("/orders/{order_id}/process")
async def schedule_order(order_id: str):
    await app.state.scheduler.schedule(
        process_order,
        execution_time=datetime.now() + timedelta(seconds=10),
        args=(order_id,),
    )
    return {"status": "scheduled"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
