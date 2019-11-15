#!/usr/bin/env python

import asyncio
import logging
from typing import (
    AsyncIterable,
    Dict,
    Optional,
    List,
)
import ujson
import websockets
from websockets.exceptions import ConnectionClosed
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.market.dranite.dranite_auth import DraniteAuth
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.market.dranite.dranite_order_book import DraniteOrderBook

DRANITE_REST_URL = "http://localhost/api/v1"
DRANITE_WS_FEED = "wss://k36bf8buki.execute-api.ap-southeast-1.amazonaws.com/v1"
MAX_RETRIES = 20
NaN = float("nan")


class DraniteAPIUserStreamDataSource(UserStreamTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _draniteausds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._draniteausds_logger is None:
            cls._draniteausds_logger = logging.getLogger(__name__)
        return cls._draniteausds_logger

    def __init__(self, dranite_auth: DraniteAuth, symbols: Optional[List[str]] = []):
        self._dranite_auth: DraniteAuth = dranite_auth
        self._symbols = symbols
        self._current_listen_key = None
        self._listen_for_user_stream_task = None
        super().__init__()

    @property
    def order_book_class(self):
        """
        *required
        Get relevant order book class to access class specific methods
        :returns: OrderBook class
        """
        return DraniteOrderBook

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        *required
        Subscribe to user stream via web socket, and keep the connection open for incoming messages
        :param ev_loop: ev_loop to execute this function in
        :param output: an async queue where the incoming messages are stored
        # TODO Dranite Does not have currently this Channel. Can be applied Later
        """
        while True:
            try:
                async with websockets.connect(DRANITE_WS_FEED) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    subscribe_payload: Dict[str, any] = {
                        "action": "subscribe",
                        "data": "",
                        "channel": "user"
                    }
                    auth_dict: Dict[str] = self._dranite_auth.generate_auth_dict("/api/v1/auth")
                    subscribe_payload.update(auth_dict)
                    await ws.send(ujson.dumps(subscribe_payload))
                    async for raw_msg in self._inner_messages(ws):
                        msg = ujson.loads(raw_msg)
                        msg_type: str = msg.get("type", None)
                        if msg_type is None:
                            raise ValueError(f"Dranite Websocket message does not contain a type - {msg}")
                        elif msg_type == "error":
                            raise ValueError(f"Dranite Websocket received error message - {msg['message']}")
                        elif msg_type in ["authenticated"]:
                            order_book_message: OrderBookMessage = self.order_book_class.diff_message_from_exchange(msg)
                            output.put_nowait(order_book_message)
                        elif msg_type in ["received", "activate", "subscriptions"]:
                            # these messages are not needed to track the order book
                            pass
                        else:
                            raise ValueError(f"Unrecognized Dranite Websocket message received - {msg}")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with Dranite WebSocket connection. "
                                    "Retrying after 30 seconds...", exc_info=True)
                await asyncio.sleep(30.0)

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        """
        Generator function that returns messages from the web socket stream
        :param ws: current web socket connection
        :returns: message in AsyncIterable format
        """
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
