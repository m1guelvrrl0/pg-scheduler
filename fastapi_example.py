from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI
from pg_scheduler import Scheduler
import asyncpg
import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = await asyncpg.create_pool(
        host="db",
        port=5432,
        user="scheduler",
        password="scheduler123",
        database="scheduler_db"
    )
    app.state.scheduler = Scheduler(
        app.state.db_pool,
        max_concurrent_jobs=20,
        misfire_grace_time=120
    )
    await app.state.scheduler.start()
    yield
    await app.state.scheduler.shutdown()
    await app.state.db_pool.close()


app = FastAPI(lifespan=lifespan)

async def something_job(message: str, delay: float = 1.0):
    print(f"Starting job: {message}")
    await asyncio.sleep(delay)
    print(f"Job completed: {message}")
    return f"Result: {message}"

@app.get("/something")
async def something(my_input: str):
    await app.state.scheduler.schedule(
        something_job,
        execution_time=datetime.now() + timedelta(seconds=10),
        args=(my_input,),
    )
    return {"result": "Job scheduled"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)