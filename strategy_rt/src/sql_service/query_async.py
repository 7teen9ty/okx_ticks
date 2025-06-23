import os
import dotenv
import asyncpg
import logging
from datetime import datetime, timedelta

dotenv.load_dotenv()

PG_DSN         = os.getenv("PG_DSN", "postgresql://user:pass@postgres:5432/dbname")
OKX_INST_IDS   = os.getenv("OKX_INST_IDS", "BTC-USDT").split(",")
PG_TICKS_TABLE = os.getenv("PG_TICKS_TABLE", "raw_data.ticks")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

async def load_4h_closes():
    """
    Загружает цену закрытия последнего завершённого 4-часового бара для каждого инструмента.
    Возвращает словарь {inst_id: close_price}.
    """
    pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=5)
    now = datetime.utcnow()
    # начало текущего 4h-блока
    block_start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=(now.hour % 4))
    closes = {}
    async with pool.acquire() as conn:
        for inst in OKX_INST_IDS:
            row = await conn.fetchrow(
                f"""
                SELECT px AS close
                FROM {PG_TICKS_TABLE}
                WHERE inst_id = $1
                AND recv_ts < $2
                ORDER BY recv_ts DESC
                LIMIT 1
                """,
                inst, block_start
            )
            closes[inst] = float(row["close"]) if row and row["close"] is not None else None
            logging.info(f"[Strategy] 4h close for {inst}: {closes[inst]}")
    await pool.close()
    return closes

async def load_2_last_closes(symbol: str):
    pool = await asyncpg.create_pool(dsn=PG_DSN)
    now = datetime.utcnow()
    base_time = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=(now.hour % 4))
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT px AS close FROM {PG_TICKS_TABLE}
            WHERE inst_id = $1 AND recv_ts < $2
            ORDER BY recv_ts DESC LIMIT 2
            """, symbol, base_time)
    await pool.close()
    if len(rows) == 2:
        return float(rows[1]['close']), float(rows[0]['close'])
    return None, None