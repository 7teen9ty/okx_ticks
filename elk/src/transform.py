import asyncio
import json
import logging
import os
import time
import dotenv

import aioredis
import asyncpg

from datetime import datetime

dotenv.load_dotenv()

REDIS_DSN       = os.getenv("REDIS_DSN", "redis://redis:6379/0")
REDIS_KEY       = os.getenv("REDIS_KEY", "ticks")
PG_DSN          = os.getenv("PG_DSN", "postgresql://user:pass@postgres:5432/dbname")
PG_TABLE        = os.getenv("PG_TABLE", "ticks")
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", 5))
BGSAVE_POLL_SEC = 1  # интервал проверки статуса BGSAVE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


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


async def wait_for_snapshot(redis):
    """
    Запускаем BGSAVE и ждём, пока Redis завершит фоновой дамп.
    """
    try:
        # инициировать фоновый save
        await redis.execute_command("BGSAVE")
        logging.info("Triggered Redis BGSAVE")
    except Exception as e:
        logging.error(f"Failed to trigger BGSAVE: {e}")
        return

    # опрашиваем статус
    while True:
        info = await redis.info(section="persistence")
        # rdb_bgsave_in_progress == 0 когда готов
        if not info.get("rdb_bgsave_in_progress", 0):
            logging.info("Redis BGSAVE completed")
            break
        await asyncio.sleep(BGSAVE_POLL_SEC)


async def flush_loop(redis, pg_pool):
    """
    Каждую минуту батчево пишем в Postgres.
    Очищаем Redis только после завершения BGSAVE.
    """
    while True:
        await asyncio.sleep(60)
        try:
            items = await redis.lrange(REDIS_KEY, 0, -1)
            if not items:
                continue

            # готовим записи для БД
            records = []
            for raw in items:
                d = json.loads(raw)
                ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(d["recv_ts"]))
                ts = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                records.append((ts, d.get("instId"), d.get("px"), d.get("sz"), d.get("side")))

            # вставляем в Postgres
            async with pg_pool.acquire() as conn:
                await conn.executemany(
                    f"INSERT INTO {PG_TABLE} (recv_ts, inst_id, px, sz, side) VALUES ($1,$2,$3,$4,$5)",
                    records
                )
            logging.info(f"Flushed {len(records)} ticks to Postgres")

            # ждём, пока Redis создаст RDB-снапшот
            await wait_for_snapshot(redis)

            # только после подтверждения снимка удаляем ключ
            await redis.delete(REDIS_KEY)
            logging.info("Cleared Redis ticks list after snapshot")

        except Exception as e:
            logging.error(f"Flush error: {e}")


async def ensure_table(pool):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {PG_TABLE} (
        recv_ts  TIMESTAMP NOT NULL,
        inst_id  TEXT,
        px       NUMERIC,
        sz       NUMERIC,
        side     TEXT
    );
    """
    async with pool.acquire() as conn:
        await conn.execute(ddl)
    logging.info(f"Ensured table `{PG_TABLE}` exists")


async def main():
    redis = await connect_redis()
    pg_pool = await connect_pg()

    await ensure_table(pg_pool)

    await flush_loop(redis, pg_pool)

if __name__ == "__main__":
    asyncio.run(main())
