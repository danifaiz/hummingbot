#!/usr/bin/env python

import asyncio
import logging
import time
import bisect
from typing import (
    List,
    Optional,
    Deque
)
from collections import (
    deque
)

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage,
    OrderBookMessageType,
    DraniteOrderBookMessage
)
from hummingbot.core.data_type.order_book_tracker import (
    OrderBookTracker,
    OrderBookTrackerDataSourceType
)
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.market.dranite.dranite_order_book import DraniteOrderBook
from hummingbot.market.dranite.dranite_api_order_book_data_source import DraniteAPIOrderBookDataSource
from hummingbot.market.dranite.dranite_active_order_tracker import DraniteActiveOrderTracker


class DraniteOrderBookTracker(OrderBookTracker):
    _hobt_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._hobt_logger is None:
            cls._hobt_logger = logging.getLogger(__name__)
        return cls._hobt_logger

    def __init__(self,
                 data_source_type: OrderBookTrackerDataSourceType = OrderBookTrackerDataSourceType.EXCHANGE_API,
                 symbols: Optional[List[str]] = None):
        super().__init__(data_source_type=data_source_type)
        self._order_book_diff_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()
        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        self._data_source: Optional[OrderBookTrackerDataSource] = None
        self._symbols: Optional[List[str]] = symbols

    @property
    def exchange_name(self) -> str:
        return "dranite"

    @property
    def data_source(self) -> OrderBookTrackerDataSource:
        if not self._data_source:
            if self._data_source_type is OrderBookTrackerDataSourceType.EXCHANGE_API:
                self._data_source = DraniteAPIOrderBookDataSource(symbols=self._symbols)
            else:
                raise ValueError(f"data_source_type {self._data_source_type} is not supported.")
        return self._data_source

    @data_source.setter
    def data_source(self, data_source):
        self._data_source = data_source

    async def start(self):
        await super().start()
        self._order_book_trade_listener_task = safe_ensure_future(
            self.data_source.listen_for_trades(self._ev_loop, self._order_book_trade_stream)
        )
        self._order_book_diff_listener_task = safe_ensure_future(
            self.data_source.listen_for_order_book_diffs(self._ev_loop, self._order_book_diff_stream)
        )
        self._order_book_snapshot_listener_task = safe_ensure_future(
            self.data_source.listen_for_order_book_snapshots(self._ev_loop, self._order_book_snapshot_stream)
        )
        self._refresh_tracking_task = safe_ensure_future(
            self._refresh_tracking_loop()
        )
        self._order_book_diff_router_task = safe_ensure_future(
            self._order_book_diff_router()
        )
        self._order_book_snapshot_router_task = safe_ensure_future(
            self._order_book_snapshot_router()
        )

    async def _order_book_diff_router(self):
        """
        Route the real-time order book diff messages to the correct order book.
        """
        last_message_timestamp: float = time.time()
        messages_queued: int = 0
        messages_accepted: int = 0
        messages_rejected: int = 0

        while True:
            try:
                ob_message: OrderBookMessage = await self._order_book_diff_stream.get()
                symbol: str = ob_message.symbol

                if symbol not in self._tracking_message_queues:
                    continue
                message_queue: asyncio.Queue = self._tracking_message_queues[symbol]
                # Check the order book's initial update ID. If it's larger, don't bother.
                order_book: OrderBook = self._order_books[symbol]

                if order_book.snapshot_uid > ob_message.update_id:
                    messages_rejected += 1
                    continue
                await message_queue.put(ob_message)
                messages_accepted += 1

                # Log some statistics.
                now: float = time.time()
                if int(now / 60.0) > int(last_message_timestamp / 60.0):
                    self.logger().debug("Diff messages processed: %d, rejected: %d, queued: %d",
                                        messages_accepted,
                                        messages_rejected,
                                        messages_queued)
                    messages_accepted = 0
                    messages_rejected = 0
                    messages_queued = 0

                last_message_timestamp = now
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error routing order book messages.",
                    exc_info=True,
                    app_warning_msg=f"Unexpected error routing order book messages. Retrying after 5 seconds."
                )
                await asyncio.sleep(5.0)

    async def _track_single_book(self, symbol: str):
        """
        Update an order book with changes from the latest batch of received messages
        """
        past_diffs_window: Deque[DraniteOrderBookMessage] = deque()
        self._past_diffs_windows[symbol] = past_diffs_window

        message_queue: asyncio.Queue = self._tracking_message_queues[symbol]
        order_book: DraniteOrderBook = self._order_books[symbol]
        active_order_tracker: DraniteActiveOrderTracker = self._active_order_trackers[symbol]

        last_message_timestamp: float = time.time()
        diff_messages_accepted: int = 0

        while True:
            try:
                message: DraniteOrderBookMessage = None
                saved_messages: Deque[DraniteOrderBookMessage] = self._saved_message_queues[symbol]
                # Process saved messages first if there are any
                if len(saved_messages) > 0:
                    message = saved_messages.popleft()
                else:
                    message = await message_queue.get()

                if message.type is OrderBookMessageType.DIFF:
                    bids, asks = active_order_tracker.convert_diff_message_to_order_book_row(message)
                    order_book.apply_diffs(bids, asks, message.update_id)
                    past_diffs_window.append(message)
                    while len(past_diffs_window) > self.PAST_DIFF_WINDOW_SIZE:
                        past_diffs_window.popleft()
                    diff_messages_accepted += 1

                    # Output some statistics periodically.
                    now: float = time.time()
                    if int(now / 60.0) > int(last_message_timestamp / 60.0):
                        self.logger().debug("Processed %d order book diffs for %s.", diff_messages_accepted, symbol)
                        diff_messages_accepted = 0
                    last_message_timestamp = now
                elif message.type is OrderBookMessageType.SNAPSHOT:
                    past_diffs: List[DraniteOrderBookMessage] = list(past_diffs_window)
                    # only replay diffs later than snapshot, first update active order with snapshot then replay diffs
                    replay_position = bisect.bisect_right(past_diffs, message)
                    replay_diffs = past_diffs[replay_position:]
                    s_bids, s_asks = active_order_tracker.convert_snapshot_message_to_order_book_row(message)
                    order_book.apply_snapshot(s_bids, s_asks, message.update_id)
                    for diff_message in replay_diffs:
                        d_bids, d_asks = active_order_tracker.convert_diff_message_to_order_book_row(diff_message)
                        order_book.apply_diffs(d_bids, d_asks, diff_message.update_id)

                    self.logger().debug("Processed order book snapshot for %s.", symbol)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error processing order book messages for {symbol}.",
                    exc_info=True,
                    app_warning_msg=f"Unexpected error processing order book messages. Retrying after 5 seconds."
                )
                await asyncio.sleep(5.0)
