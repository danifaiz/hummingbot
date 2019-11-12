#!/usr/bin/env python

import asyncio
import logging
from typing import (
    Optional,
    List,
)
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.user_stream_tracker import (
    UserStreamTrackerDataSourceType,
    UserStreamTracker
)
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.market.dranite.dranite_api_user_stream_data_source import DraniteAPIUserStreamDataSource
from hummingbot.market.dranite.dranite_auth import DraniteAuth


class DraniteUserStreamTracker(UserStreamTracker):
    _dust_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._dust_logger is None:
            cls._dust_logger = logging.getLogger(__name__)
        return cls._dust_logger

    def __init__(self,
                 data_source_type: UserStreamTrackerDataSourceType = UserStreamTrackerDataSourceType.EXCHANGE_API,
                 dranite_auth: Optional[DraniteAuth] = None,
                 symbols: Optional[List[str]] = []):
        super().__init__(data_source_type=data_source_type)
        self._dranite_auth: DraniteAuth = dranite_auth
        self._symbols: List[str] = symbols
        self._ev_loop: asyncio.events.AbstractEventLoop = asyncio.get_event_loop()
        self._data_source: Optional[UserStreamTrackerDataSource] = None
        self._user_stream_tracking_task: Optional[asyncio.Task] = None

    @property
    def data_source(self) -> UserStreamTrackerDataSource:
        """
        *required
        Initializes a user stream data source (user specific order diffs from live socket stream)
        :return: OrderBookTrackerDataSource
        """
        if not self._data_source:
            if self._data_source_type is UserStreamTrackerDataSourceType.EXCHANGE_API:
                self._data_source = DraniteAPIUserStreamDataSource(dranite_auth=self._dranite_auth,
                                                                   symbols=self._symbols)
            else:
                raise ValueError(f"data_source_type {self._data_source_type} is not supported.")
        return self._data_source

    @property
    def exchange_name(self) -> str:
        """
        *required
        Name of the current exchange
        """
        return "dranite"

    async def start(self):
        """
        *required
        Start all listeners and tasks
        """
        self._user_stream_tracking_task = safe_ensure_future(
            self.data_source.listen_for_user_stream(self._ev_loop, self._user_stream)
        )
        await safe_gather(self._user_stream_tracking_task)
