import asyncio
import json
import logging
import os
import time

import aioredis
import websockets

from utils.connect import connect_redis

OKX_WS_URL  = os.getenv("OKX_WS_URL", "wss://ws.okx.com:8443/ws/v5/public")
CHANNEL     = os.getenv("OKX_CHANNEL", "trades")
INST_ID     = os.getenv("OKX_INST_ID", "BTC-USDT")
REDIS_DSN   = os.getenv("REDIS_DSN", "redis://redis:6379/0")
REDIS_KEY   = os.getenv("REDIS_KEY", "ticks")
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", 5))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


async def consume_okx(redis):
    while True:
        try:
            async with websockets.connect(OKX_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                sub = {"op":"subscribe", "args":[{"channel":CHANNEL, "instId":INST_ID}]}
                await ws.send(json.dumps(sub))
                logging.info(f"Subscribed to {CHANNEL}/{INST_ID}")
                async for msg in ws:
                    data = json.loads(msg)
                    if "data" in data:
                        for tick in data["data"]:
                            payload = {"recv_ts": time.time(), **tick}
                            await redis.lpush(REDIS_KEY, json.dumps(payload))
        except Exception as e:
            logging.warning(f"OKX WS disconnected: {e}, reconnect in {RECONNECT_DELAY}s")
            await asyncio.sleep(RECONNECT_DELAY)

async def main():
    redis = await connect_redis()
    await consume_okx(redis)

if __name__=="__main__":
    asyncio.run(main())
