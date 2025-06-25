import os
import time
import json
import logging
import asyncio
import dotenv
import websockets
from datetime import datetime, timedelta
from okx.Trade import TradeAPI
from okx.Account import AccountAPI

from sql_service.query_async import load_4h_closes, load_2_last_closes

dotenv.load_dotenv()

# Настройки из окружения
OKX_INST_IDS   = os.getenv("OKX_INST_IDS", "BTC-USDT").split(",")
THRESHOLD      = float(os.getenv("THRESHOLD_PIPS", 350))
SIZE_POSITION  = float(os.getenv("SIZE_POSITION", 0.03))
STOP_LOSS_PIPS = float(os.getenv("STOP_LOSS_PIPS", 100))
PG_TICKS_TABLE = os.getenv("PG_TICKS_TABLE", "raw_data.ticks")
LEVERAGE       = int(os.getenv("LEVERAGE", 3))
LIST_BUY_SIZE  = list(map(float, os.getenv("LIST_BUY", "0.03, 0.03, 0.06, 0.06").split(',')))

API_KEY       = os.getenv("OKX_API_KEY", "")
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


async def tick_stream(max_retries=100, retry_delay=5):
    url = "wss://wspap.okx.com:8443/ws/v5/public" if API_FLAG == "1" else "wss://ws.okx.com:8443/ws/v5/public"
    retry_count = 0
    while retry_count < max_retries:
        logging.info('Connect websocket')
        try:
            logging.info('Connect True')
            async with websockets.connect(url, ping_interval=None) as ws:
                await ws.send(json.dumps({
                    "op": "subscribe",
                    "args": [{"channel": "tickers", "instId": f"{symbol}"} for symbol in OKX_INST_IDS]
                }))
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if "data" in data:
                        for item in data["data"]:
                            yield item["instId"], float(item["last"]), item
                    await asyncio.sleep(0.001)

        except Exception as e:
            logging.warning(f"[OKX WS] Connection error: {e}, retrying in {retry_delay}s")
            retry_count += 1
            await asyncio.sleep(retry_delay)


class Trade:
    def __init__(self, symbol: str, entry_price: float, side: str, last_close: float, on_close):
        self.symbol = f"{symbol}-SWAP"
        if not self._valid_name_symbol:
            raise ValueError(f'Ошибка в имени символа торговли {self.symbol} {symbol}')
        self.entry_price = entry_price
        self.side = side
        self.size = SIZE_POSITION
        self.count = 1
        initial_sl = self._calc_stop_loss()
        self._set_leverage(LEVERAGE)
        if side == 'long':
            self.stop_loss = (last_close - STOP_LOSS_PIPS) if last_close < initial_sl else initial_sl
        else:
            self.stop_loss = (last_close + STOP_LOSS_PIPS) if last_close > initial_sl else initial_sl

        self.okx = TradeAPI(API_KEY, API_SECRET, API_PASSPHRASE, flag=API_FLAG)
        self.on_close = on_close
        logging.info(f"[Trade] init {self.symbol=} {self.side=} entry={self.entry_price} sl={self.stop_loss}")

    def _valid_name_symbol(self):
        split_symbol = self.symbol.split('-')
        if len(split_symbol) == 3:
            return all(split_symbol[0] in ('BTC', 'ETH'), split_symbol[1] == 'USDT', split_symbol[2] == 'SWAP')
        return False

    def _calc_stop_loss(self) -> float:
        return self.entry_price - STOP_LOSS_PIPS if self.side == 'long' else self.entry_price + STOP_LOSS_PIPS

    async def open_position(self, size_dep=None):
        logging.info(f"[Trade] opening {self.side} for {self.symbol} @ {self.entry_price}")
        req = self.okx.place_order(
            instId=self.symbol,
            tdMode="isolated",
            side=['sell', 'buy'][self.side == 'long'],
            ordType='market',
            sz=self.size,
            posSide=self.side if not size_dep else size_dep,
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

    async def monitor(self):
        time_start = time.time()
        closes = await load_4h_closes()
        async for symbol, px, _ in tick_stream():
            logging.info(f"{symbol=} {px=} || {self.stop_loss}")

            if 'SWAP' not in symbol.split('-'):
                symbol = f'{symbol}-SWAP'
            if symbol != self.symbol:
                continue

            utc_hour, utc_minute = datetime.utcnow().hour, datetime.utcnow().minute
            if utc_minute == 0 and utc_hour % 4 == 0 and time.time() - time_start > 60:
                logging.info('[INFO] Update close for 4 hours')
                await asyncio.sleep(1)
                time_start = time.time()
                closes = await load_4h_closes()

            closes_symbol = closes.get(self.symbol.replace('-SWAP', ''))
            stp_loss_now = [self.stop_loss < closes_symbol - STOP_LOSS_PIPS, self.stop_loss > closes_symbol + STOP_LOSS_PIPS][self.side == 'short']
            if stp_loss_now:
                self.stop_loss = [closes_symbol - STOP_LOSS_PIPS, closes_symbol + STOP_LOSS_PIPS][self.side == 'short']
            if self.side == 'long' and px >= self.entry_price + (self.count * THRESHOLD) and self.count < 4:
                await self.open_position(size_dep=LIST_BUY_SIZE[self.count])
                self.count += 1
            if self.side == 'long' and px <= self.stop_loss:
                logging.info(f"[Trade] SL hit LONG {self.symbol} @ {px}")

                self.okx.place_order(
                    instId=self.symbol,
                    tdMode="isolated",
                    side='sell',
                    ordType='market',
                    sz=LIST_BUY_SIZE[self.count],
                    posSide=self.side,
                    tag="teststrategy"
                )
                self.count -= 1
                if self.count <= 0:
                    break

            if self.side == 'short' and px >= self.stop_loss:
                logging.info(f"[Trade] SL hit SHORT {self.symbol} @ {px}")
                self.okx.place_order(
                    instId=self.symbol,
                    tdMode="isolated",
                    side='buy',
                    ordType='market',
                    sz=LIST_BUY_SIZE[self.count],
                    posSide=self.side,
                    tag="teststrategy"
                )
                self.count -= 1
                if self.count <= 0:
                    break

        await self.on_close(self.symbol)

async def strategy():
    closes = await load_4h_closes()
    active_positions = set()
    mins = {s: float('inf') for s in OKX_INST_IDS}
    maxs = {s: float('-inf') for s in OKX_INST_IDS}

    async def on_trade_close(symbol):
        logging.info(f"[Strategy] position closed for {symbol}")
        active_positions.discard(symbol)

    logging.info('Tick starter')
    async for symbol, px, _ in tick_stream():
        if symbol not in OKX_INST_IDS:
            continue
        if symbol in active_positions:
            continue
        last_close = closes.get(symbol)
        if last_close is None:
            continue

        mins[symbol] = min(mins[symbol], px)
        maxs[symbol] = max(maxs[symbol], px)
        logging.info(f"[Pre trade] {mins[symbol]=} {maxs[symbol]=} {last_close=} {THRESHOLD}")

        if maxs[symbol] - last_close >= THRESHOLD:
            side = 'long'
            entry = maxs[symbol]
        elif last_close - mins[symbol] >= THRESHOLD:
            side = 'short'
            entry = mins[symbol]
        else:
            continue

        trade = Trade(symbol, entry, side, last_close, on_trade_close)
        await trade.open_position()
        active_positions.add(symbol)
        logging.info(active_positions)

        asyncio.create_task(trade.monitor())
        mins[symbol], maxs[symbol] = float('inf'), float('-inf')

if __name__ == '__main__':
    asyncio.run(strategy())
