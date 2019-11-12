#!/usr/bin/env python
import asyncio
import logging
import time
from typing import Optional, List, Dict, AsyncIterable, Any

import aiohttp
import pandas as pd
import json
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.logger import HummingbotLogger
from hummingbot.market.dranite.dranite_order_book import DraniteOrderBook

EXCHANGE_NAME = "Dranite"

DRANITE_REST_URL = "http://localhost/api/v1"
DRANITE_EXCHANGE_INFO_PATH = "/markets.json"
DRANITE_TICKER_PATH = "/ticker.json"
DRANITE_ORDERBOOK_URL = "https://st4g1ng.dranite.com/api/v1/orderbook/"
DRANITE_WS_FEED = "wss://k36bf8buki.execute-api.ap-southeast-1.amazonaws.com/v1"

MAX_RETRIES = 20
MESSAGE_TIMEOUT = 30.0
NaN = float("nan")


class DraniteAPIOrderBookDataSource(OrderBookTrackerDataSource):
    PING_TIMEOUT = 10.0

    _draniteaobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._draniteaobds_logger is None:
            cls._draniteaobds_logger = logging.getLogger(__name__)
        return cls._draniteaobds_logger

    def __init__(self, symbols: Optional[List[str]] = None):
        super().__init__()
        self._symbols: Optional[List[str]] = symbols

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        Returned data frame should have symbol as index and include USDVolume, baseAsset and quoteAsset
        """
        market_path_url = f"{DRANITE_REST_URL}{DRANITE_EXCHANGE_INFO_PATH}"
        ticker_path_url = f"{DRANITE_REST_URL}{DRANITE_TICKER_PATH}"

        async with aiohttp.ClientSession() as client:

            market_response, ticker_response = await safe_gather(
                client.get(market_path_url), client.get(ticker_path_url)
            )

            market_response: aiohttp.ClientResponse = market_response
            ticker_response: aiohttp.ClientResponse = ticker_response

            if market_response.status != 200:
                raise IOError(
                    f"Error fetching active Dranite markets information. " f"HTTP status is {market_response.status}."
                )
            if ticker_response.status != 200:
                raise IOError(
                    f"Error fetching active Dranite market tickers. " f"HTTP status is {ticker_response.status}."
                )

            market_data, ticker_data = await safe_gather(
                market_response.json(), ticker_response.json()
            )

            ticker_data: Dict[str, Any] = {item["symbol"]: item for item in ticker_data['data']}

            market_data: List[Dict[str, Any]] = [
                {**item, **ticker_data[item["symbol"]]}
                for item in market_data['data']
                if item["symbol"] in ticker_data
            ]

            all_markets: pd.DataFrame = pd.DataFrame.from_records(data=market_data, index="symbol")
            all_markets.rename(
                {"baseCurrencySymbol": "baseAsset", "quoteCurrencySymbol": "quoteAsset"}, axis="columns", inplace=True
            )

            # btc_usd_price: float = float(all_markets.loc["BTC-USD"].last_price)
            # eth_usd_price: float = float(all_markets.loc["ETH-USD"].last_price)
            btc_usd_price: float = float(1000)
            eth_usd_price: float = float(1000)

            usd_volume: List[float] = [
                (
                    volume * quote_price if symbol.endswith(("USD", "USDT")) else
                    volume * quote_price * btc_usd_price if symbol.endswith("BTC") else
                    volume * quote_price * eth_usd_price if symbol.endswith("ETH") else
                    volume
                )
                for symbol, volume, quote_price in zip(all_markets.index,
                                                       all_markets.volume.astype("float"),
                                                       all_markets.last_price.astype("float"))
            ]
            old_symbols: List[str] = [
                (
                    f"{quoteAsset}-{baseAsset}"
                )
                for baseAsset, quoteAsset in zip(all_markets.baseAsset, all_markets.quoteAsset)
            ]

            all_markets.loc[:, "USDVolume"] = usd_volume
            all_markets.loc[:, "old_symbol"] = old_symbols
            await client.close()
            return all_markets.sort_values("USDVolume", ascending=False)

    async def get_trading_pairs(self) -> List[str]:
        if not self._symbols:
            try:
                active_markets: pd.DataFrame = await self.get_active_exchange_markets()
                self._symbols = active_markets.index.tolist()
            except Exception:
                self._symbols = []
                self.logger().network(
                    f"Error getting active exchange information.",
                    exc_info=True,
                    app_warning_msg=f"Error getting active exchange information. Check network connection.",
                )
        return self._symbols

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, Any]:
        global DRANITE_ORDERBOOK_URL
        DRANITE_ORDERBOOK_URL = DRANITE_ORDERBOOK_URL + trading_pair
        async with client.get(DRANITE_ORDERBOOK_URL) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching Dranite market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            api_data = await response.read()
            data: Dict[str, Any] = json.loads(api_data)
            return data

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        # Get the currently active markets
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            retval: Dict[str, OrderBookTrackerEntry] = {}

            number_of_pairs: int = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                    snapshot_msg: OrderBookMessage = DraniteOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        metadata={"symbol": trading_pair}
                    )
                    order_book: OrderBook = self.order_book_create_function()
                    order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
                    retval[trading_pair] = OrderBookTrackerEntry(trading_pair, snapshot_msg.timestamp, order_book)
                    self.logger().info(f"Initialized order book for {trading_pair}. "
                                       f"{index + 1}/{number_of_pairs} completed.")
                    # Dranite rate limit is 100 https requests per 10 seconds
                    await asyncio.sleep(0.4)
                except Exception:
                    self.logger().error(f"Error getting snapshot for {trading_pair}. ", exc_info=True)
                    await asyncio.sleep(5)
            return retval

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with websockets.connect(DRANITE_WS_FEED) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for trading_pair in trading_pairs:
                        subscribe_payload: Dict[str, Any] = {
                            "action": "subscribe",
                            "data": "",
                            "channel": "trade",
                            "topic": trading_pair
                        }
                        await ws.send(json.dumps(subscribe_payload))

                    async for raw_msg in self._inner_messages(ws):
                        # Dranite Incoming Messages
                        # TODO: use different action on different channel messages recieved by Dranite Exchange
                        msg: Dict[str, Any] = json.loads(raw_msg)
                        if "order-book" in msg:
                            pass
                        elif "trades" in msg:
                            pass
                        elif "ticker" in msg:
                            pass
                        else:
                            self.logger().debug(f"Unrecognized message received from Dranite websocket: {msg}")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with websockets.connect(DRANITE_WS_FEED) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for trading_pair in trading_pairs:
                        subscribe_payload: Dict[str, Any] = {
                            "action": "subscribe",
                            "data": "",
                            "channel": "order-book",
                            "topic": trading_pair
                        }
                        await ws.send(json.dumps(subscribe_payload))

                    async for raw_msg in self._inner_messages(ws):
                        # Dranite Incoming Messages
                        # TODO: use different action on different channel messages recieved by Dranite Exchange
                        msg: Dict[str, Any] = json.loads(raw_msg)
                        if "order-book" in msg:
                            pass
                        elif "trades" in msg:
                            pass
                        elif "ticker" in msg:
                            pass
                        else:
                            self.logger().debug(f"Unrecognized message received from Dranite websocket: {msg}")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with aiohttp.ClientSession() as client:
                    for trading_pair in trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            snapshot_message: OrderBookMessage = DraniteOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                metadata={"symbol": trading_pair}
                            )
                            output.put_nowait(snapshot_message)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                            await asyncio.sleep(5.0)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.logger().error("Unexpected error.", exc_info=True)
                            await asyncio.sleep(5.0)
                    this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                    next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                    delta: float = next_hour.timestamp() - time.time()
                    await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)
