import asyncio
import json
import logging
import os
import time

import aioredis
import websockets

from utils.connect import connect_redis

OKX_WS_URL      = os.getenv("OKX_WS_URL", "wss://ws.okx.com:8443/ws/v5/public")
CHANNEL         = os.getenv("OKX_CHANNEL", "trades")
OKX_INST_IDS    = os.getenv("OKX_INST_IDS", "BTC-USDT,ETH-USDT").split(",")
REDIS_DSN       = os.getenv("REDIS_DSN", "redis://redis:6379/0")
REDIS_KEY       = os.getenv("REDIS_KEY", "ticks")
REDIS_STREAM_CH = os.getenv("REDIS_STREAM_CH", "ticks_stream")
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", 5))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

async def consume_okx(redis):
    """
    Слушаем WebSocket OKX, сохраняем тики в Redis и публикуем их в Pub/Sub канал.
    """
    while True:
        try:
            async with websockets.connect(
                OKX_WS_URL, ping_interval=20, ping_timeout=10
            ) as ws:
                # Подписываемся на несколько инструментов
                args = [
                    {"channel": CHANNEL, "instId": inst}
                    for inst in OKX_INST_IDS
                ]
                sub_msg = {"op": "subscribe", "args": args}
                await ws.send(json.dumps(sub_msg))
                logging.info(f"Subscribed to {CHANNEL} for {OKX_INST_IDS}")

                # Читаем сообщения
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if "data" in msg:
                            for tick in msg["data"]:
                                payload = {"recv_ts": time.time(), **tick}
                                # Сохраняем в список
                                await redis.lpush(REDIS_KEY, json.dumps(payload))
                                # Публикуем в канал для real-time обработки
                                await redis.publish(
                                    REDIS_STREAM_CH, json.dumps(payload)
                                )
                    except Exception as inner:
                        logging.error(f"Parse/publish error: {inner}")
        except Exception as e:
            logging.warning(
                f"OKX WS connection lost: {e}, reconnect in {RECONNECT_DELAY}s"
            )
            await asyncio.sleep(RECONNECT_DELAY)


async def main():
    redis = await connect_redis()
    await consume_okx(redis)


if __name__ == "__main__":
    asyncio.run(main())
