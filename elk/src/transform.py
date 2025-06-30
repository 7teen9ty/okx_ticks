import asyncio
import json
import logging
import os
import time
import dotenv
import psycopg2

import aioredis
import asyncpg

os.listdir()

from utils.connect import connect_pg, connect_redis

from datetime import datetime, timedelta

dotenv.load_dotenv()

PG_DSN                  = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")   
PG_TICKS_TABLE          = os.getenv("PG_TICKS_TABLE", "ticks")
PG_CANDLES_TABLE        = os.getenv("PG_CANDLES_TABLE", "candles")
PG_CANDLES_TABLE_4H     = os.getenv("PG_CANDLES_TABLE_4H", "candles_4h")
PG_RENKO_TABLE_4H       = os.getenv("PG_RENKO_TABLE_4H", "renko_4h")
REDIS_KEY               = os.getenv("REDIS_KEY", "ticks")
BGSAVE_POLL_SEC         = 1

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


async def compute_and_store_candles(pg_pool):
    """
    Берём все тики за прошлую минуту, считаем OHLC и кладём в таблицу candles.
    """
    # границы предыдущей минуты
    now = datetime.utcnow().replace(second=0, microsecond=0)
    start = now - timedelta(minutes=1)
    end = now

    query = f"""
        SELECT
          date_trunc('minute', recv_ts) AS minute,
          (array_agg(px ORDER BY recv_ts))[1]       AS open,
          MAX(px)                                   AS high,
          MIN(px)                                   AS low,
          (array_agg(px ORDER BY recv_ts DESC))[1]  AS close
        FROM {PG_TICKS_TABLE}
        WHERE recv_ts >= $1 AND recv_ts < $2
        GROUP BY minute;
    """

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(query, start, end)
        if not rows:
            logging.info("No ticks for candle at %s", start)
            return

        # UPSERT в candles
        upsert = f"""
            INSERT INTO {PG_CANDLES_TABLE}(minute, open, high, low, close)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (minute) DO UPDATE
              SET open  = EXCLUDED.open,
                  high  = EXCLUDED.high,
                  low   = EXCLUDED.low,
                  close = EXCLUDED.close;
        """
        for row in rows:
            await conn.execute(upsert,
                row["minute"], row["open"], row["high"], row["low"], row["close"]
            )

    logging.info("Stored %d candle(s) for minute %s", len(rows), start)


async def trigger_bgsave_and_wait(redis):
    """Инициируем BGSAVE и ждём, пока rdb_bgsave_in_progress не станет 0."""
    try:
        await redis.execute_command("BGSAVE")
        logging.info("Triggered Redis BGSAVE")
    except Exception as e:
        logging.error(f"Failed to trigger BGSAVE: {e}")
        return

    while True:
        info = await redis.info(section="persistence")
        if not info.get("rdb_bgsave_in_progress", 0):
            logging.info("Redis BGSAVE completed")
            break
        await asyncio.sleep(BGSAVE_POLL_SEC)



async def ensure_tables(pool):
    """Создаём таблицы ticks и candles, если их нет."""
    async with pool.acquire() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {PG_TICKS_TABLE} (
                recv_ts TIMESTAMP NOT NULL,
                inst_id TEXT,
                px NUMERIC,
                sz NUMERIC,
                side TEXT
            );
        """)

        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {PG_CANDLES_TABLE} (
                minute TIMESTAMP PRIMARY KEY,
                open   NUMERIC,
                high   NUMERIC,
                low    NUMERIC,
                close  NUMERIC
            );
        """)
        
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {PG_CANDLES_TABLE_4H} (
                timestamp TIMESTAMP PRIMARY KEY,
                open   NUMERIC,
                high   NUMERIC,
                low    NUMERIC,
                close  NUMERIC
            );
        """)
        
                
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {PG_RENKO_TABLE_4H} (
                timestamp   TIMESTAMP PRIMARY KEY,
                price       NUMERIC,
                direction   TEXT
            );
        """)
        
    logging.info("Ensured tables exist: %s, %s, %s, %s", PG_TICKS_TABLE, PG_CANDLES_TABLE, PG_CANDLES_TABLE_4H, PG_RENKO_TABLE_4H)


def generate_4h_candles(pool: asyncpg.Pool):
    """создает 4-х часовые свечки"""
    logging.info("[INFO] create new candel for 4h")
    
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cursor:
        # Получаем минимальное и максимальное время
            cursor.execute(f"SELECT MIN(recv_ts), MAX(recv_ts) FROM {PG_TICKS_TABLE}")
            min_ts, max_ts = cursor.fetchone()
            if not min_ts or not max_ts:
                logging.warning('[WARNING] no max and min date')
                return

        # Округление времени вниз до начала 4ч интервала
            current = min_ts.replace(minute=0, second=0, microsecond=0)
            current = current - timedelta(hours=current.hour % 4)

            while current < max_ts:
                next_ts = current + timedelta(hours=4)
            
                cursor.execute("""
                    SELECT px as price FROM raw_data.ticks
                    WHERE "timestamp" >= %s AND "timestamp" < %s
                    ORDER BY "timestamp"
                """, (current, next_ts))
                rows = cursor.fetchall()

                if not rows:
                    current = next_ts
                    continue

                prices = [r[0] for r in rows]
                open_ = prices[0]
                high = max(prices)
                low = min(prices)
                close = prices[-1]

                cursor.execute(f"""
                INSERT INTO {PG_CANDLES_TABLE_4H} ("timestamp", open, high, low, close)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT ("timestamp") DO NOTHING
                """, (current, open_, high, low, close))    

                current = next_ts
            
            conn.commit()
            logging.info("[INFO] 4h candles generated successfully")


def generate_renko_from_4h(pool: asyncpg.Pool, block_size=350):
    """создает 4-х часовые ренко"""
    with pool.acquire() as conn:
        rows = conn.fetch(f"""
            SELECT "timestamp", close FROM {PG_CANDLES_TABLE_4H}
            ORDER BY "timestamp"
        """)
        if not rows:
            return

        bricks = []
        prev_price = rows[0]['close']
        prev_direction = None

        for row in rows[1:]:
            price = row['close']
            diff = price - prev_price
            blocks = int(abs(diff) // block_size)

            if blocks == 0:
                continue

            direction = 'up' if diff > 0 else 'down'
            for i in range(blocks):
                prev_price += block_size if direction == 'up' else -block_size
                bricks.append((row['"timestamp"'], prev_price, direction))

            prev_direction = direction

        # Сохраняем в БД
        conn.executemany(f"""
            INSERT INTO {PG_RENKO_TABLE_4H} ("timestamp", price, direction)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
        """, bricks)


async def flush_loop(redis, pg_pool):
    """Основной цикл: каждые 60 с — батч из Redis → Postgres → BGSAVE → чистка → свечи."""
    while True:
        if datetime.utcnow().hour % 4 == 0 and datetime.utcnow().minute == 0:
            generate_4h_candles(pg_pool)
            generate_renko_from_4h(pg_pool)

        await asyncio.sleep(60 - datetime.utcnow().second)
        try:
            items = await redis.lrange(REDIS_KEY, 0, -1)
            if not items:
                continue

            # подготовка батча тиков
            records = []
            for raw in items:
                d = json.loads(raw)
                ts = datetime.fromtimestamp(d["recv_ts"])
                records.append((ts, d.get("instId"), d.get("px"), d.get("sz"), d.get("side")))

            # вставляем в ticks
            async with pg_pool.acquire() as conn:
                await conn.executemany(
                    f"INSERT INTO {PG_TICKS_TABLE} (recv_ts, inst_id, px, sz, side) VALUES ($1,$2,$3,$4,$5)",
                    records
                )
            logging.info("Flushed %d ticks to Postgres", len(records))

            # ждём завершения RDB-дампа
            await trigger_bgsave_and_wait(redis)

            # очищаем Redis
            await redis.delete(REDIS_KEY)
            logging.info("Cleared Redis ticks list after snapshot")

            # считаем и сохраняем свечи за прошлую минуту
            await compute_and_store_candles(pg_pool)

        except Exception as e:
            logging.error("Flush loop error: %s", e)


async def main():
    redis = await connect_redis()
    pg_pool = await connect_pg()

    await ensure_tables(pg_pool)
    await flush_loop(redis, pg_pool)

if __name__ == "__main__":
    asyncio.run(main())
