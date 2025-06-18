import os
import json
import logging
import asyncio
import dotenv
import asyncpg
import aioredis

from datetime import datetime, timedelta
from okx.Trade import TradeAPI
from okx.Account import AccountAPI


dotenv.load_dotenv()

# Настройки из окружения
REDIS_DSN      = os.getenv("REDIS_DSN", "redis://redis:6379/0")
REDIS_STREAM   = os.getenv("REDIS_STREAM_CH", "ticks_stream")
PG_DSN         = os.getenv("PG_DSN", "postgresql://user:pass@postgres:5432/dbname")
OKX_INST_IDS   = os.getenv("OKX_INST_IDS", "BTC-USDT").split(",")
THRESHOLD      = float(os.getenv("THRESHOLD_PIPS", 350))
SIZE_POSITION  = float(os.getenv("SIZE_POSITION", 0.03))
STOP_LOSS_PIPS = float(os.getenv("STOP_LOSS_PIPS", 100))
PG_TICKS_TABLE = os.getenv("PG_TICKS_TABLE", "raw_data.ticks")
LEVERAGE      = int(os.getenv("LEVERAGE", 3))
API_KEY        = os.getenv("OKX_API_KEY", "")
if not API_KEY:
    raise ValueError("OKX_API_KEY must be set in environment variables")
API_SECRET     = os.getenv("OKX_API_SECRET", "")
if not API_SECRET:
    raise ValueError("OKX_API_SECRET must be set in environment variables")
API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE", "")
if not API_PASSPHRASE:
    raise ValueError("OKX_API_PASSPHRASE must be set in environment variables")
API_FLAG       = os.getenv("OKX_API_FLAG", "1")  # 1 - test, 0 - live

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


async def tick_stream(redis):
    """
    Асинхронный генератор тиков из Redis Pub/Sub.
    """
    pubsub = redis.pubsub()
    await pubsub.subscribe(REDIS_STREAM)
    try:
        while True:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg and msg['type'] == 'message':
                data = json.loads(msg['data'])
                yield data['instId'], float(data['px']), data
            else:
                await asyncio.sleep(0.001)
    finally:
        await pubsub.unsubscribe(REDIS_STREAM)
        await pubsub.close()

class Trade:
    """
    Управление одной позицией: вход и SL-мониторинг.
    """
    def __init__(self, symbol: str, entry_price: float, side: str, last_close: float):
        self.symbol = f"{symbol}-SWAP"
        if not self._valid_name_symbol:
            raise ValueError(f'Ошибка в имени символа торговли {self.symbol} {symbol}')
        self.entry_price = entry_price
        self.side = side   # 'long' или 'short'
        self.size = SIZE_POSITION
        # первоначальный стоп-лосс
        initial_sl = self._calc_stop_loss()
        self._set_leverage(LEVERAGE)
        # корректируем SL на основании last_close
        if side == 'long':
            self.stop_loss = (last_close - STOP_LOSS_PIPS) if last_close < initial_sl else initial_sl
        else:
            self.stop_loss = (last_close + STOP_LOSS_PIPS) if last_close > initial_sl else initial_sl

        self.okx = TradeAPI(API_KEY, API_SECRET, API_PASSPHRASE, flag=API_FLAG)
        logging.info(f"[Trade] init {self.symbol=} {self.side=} entry={self.entry_price} sl={self.stop_loss}")

    def _valid_name_symbol(self):
        split_symbol = self.symbol.split('-')
        if len(split_symbol) == 3:
            return all(split_symbol[0] in ('BTC', 'ETH'), split_symbol[1] == 'USDT', split_symbol[2] == 'SWAP')
        return False
    def _calc_stop_loss(self) -> float:
        return self.entry_price - STOP_LOSS_PIPS if self.side == 'long' else self.entry_price + STOP_LOSS_PIPS

    async def open_position(self):
        """Исполнить рыночный ордер"""
        logging.info(f"[Trade] opening {self.side} for {self.symbol} @ {self.entry_price}")
        req = self.okx.place_order(
            instId=self.symbol,
            tdMode="isolated",
            side=['sell', 'buy'][self.side == 'long'],
            ordType='market',
            sz=self.size,
            posSide=self.side,
            tag="teststrategy"
                    )
        logging.info(req)

    def _set_leverage(self, leverage: int):
        account_api = AccountAPI(API_KEY, API_SECRET, API_PASSPHRASE, flag=API_FLAG)
        account_api.set_leverage(        
        instId=self.symbol,
        lever=str(leverage),
        mgnMode='isolated',
        posSide=self.side
        )
        logging.info(f"[Trade] leverage set to {leverage} for {self.symbol} on {self.side} side")

    async def monitor(self, redis, on_close):
        """Следим за тиками и вызываем колбек при SL срабатывании"""
        async for symbol, px, _ in tick_stream(redis):
            if symbol != self.symbol:
                continue
            
            if self.side == 'long' and px <= self.stop_loss:
                logging.info(f"[Trade] SL hit LONG {self.symbol} @ {px}")
                self.okx.place_order(
                            instId=self.symbol,
                            tdMode="isolated",
                            side='sell',
                            ordType='market',
                            sz=self.size,
                            posSide=self.side,
                            tag="teststrategy"
                                    )
                break
            if self.side == 'short' and px >= self.stop_loss:
                logging.info(f"[Trade] SL hit SHORT {self.symbol} @ {px}")
                self.okx.place_order(
                            instId=self.symbol,
                            tdMode="isolated",
                            side='buy',
                            ordType='market',
                            sz=self.size,
                            posSide=self.side,
                            tag="teststrategy"
                                    )
                break
        # вызываем колбек об окончании позиции
        on_close(self.symbol)

async def strategy():
    # инициализация Redis
    redis = await aioredis.from_url(REDIS_DSN, encoding='utf-8', decode_responses=True)
    # загрузка последних 4h close
    closes = await load_4h_closes()
    # активные позиции по символам
    active_positions = set()
    # min/max за сессию
    mins = {s: float('inf') for s in OKX_INST_IDS}
    maxs = {s: float('-inf') for s in OKX_INST_IDS}

    def on_trade_close(symbol):
        """Колбек для удаления позиции из активных"""
        logging.info(f"[Strategy] position closed for {symbol}")
        active_positions.discard(symbol)

    # слушаем тики
    async for symbol, px, _ in tick_stream(redis):
        if symbol not in OKX_INST_IDS:
            continue
        if symbol in active_positions:
            continue  # только одна позиция одновременно
        last_close = closes.get(symbol)
        if last_close is None:
            continue
        mins[symbol] = min(mins[symbol], px)
        maxs[symbol] = max(maxs[symbol], px)
        logging.info(f"[Pre trade] {mins[symbol]=} {maxs[symbol]=} {last_close=} {THRESHOLD}")
        # проверка порога
        if maxs[symbol] - last_close >= THRESHOLD:
            side = 'long'
            entry = maxs[symbol]
        elif last_close - mins[symbol] >= THRESHOLD:
            side = 'short'
            entry = mins[symbol]
        else:
            continue
        # запускаем позицию
        trade = Trade(symbol, entry, side, last_close)
        await trade.open_position()
        active_positions.add(symbol)
        # мониторим SL с колбеком
        asyncio.create_task(trade.monitor(redis, on_trade_close))
        # сбрасываем min/max
        mins[symbol], maxs[symbol] = float('inf'), float('-inf')

if __name__ == '__main__':
    asyncio.run(strategy())
