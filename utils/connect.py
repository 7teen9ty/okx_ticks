import asyncpg
import asyncio
import logging
import aioredis

import os
import dotenv

dotenv.load_dotenv()

REDIS_DSN       = os.getenv("REDIS_DSN", "redis://redis:6379/0")
PG_DSN          = os.getenv("PG_DSN", "postgresql://user:pass@postgres:5432/dbname")
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", 5))

async def connect_redis():
    while True:
        try:
            r = await aioredis.from_url(REDIS_DSN, encoding="utf-8", decode_responses=True)
            await r.ping()
            logging.info("Connected to Redis")
            return r
        except Exception as e:
            logging.warning(f"Redis connect error: {e}, retry in {RECONNECT_DELAY}s")
            await asyncio.sleep(RECONNECT_DELAY)

async def connect_pg():
    while True:
        try:
            pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=5)
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1;")
            logging.info("Connected to Postgres")
            return pool
        except Exception as e:
            logging.warning(f"Postgres connect error: {e}, retry in {RECONNECT_DELAY}s")
            await asyncio.sleep(RECONNECT_DELAY)
