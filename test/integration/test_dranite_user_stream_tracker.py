#!/usr/bin/env python

from os.path import join, realpath
import sys; sys.path.insert(0, realpath(join(__file__, "../../../")))

import asyncio
import logging
import unittest
import conf
from typing import (
    Optional
)
from hummingbot.market.dranite.dranite_market import DraniteAuth
from hummingbot.market.dranite.dranite_user_stream_tracker import DraniteUserStreamTracker
from hummingbot.core.utils.async_utils import safe_ensure_future


class DraniteStreamTrackerUnitTest(unittest.TestCase):
    user_stream_tracker: Optional[DraniteUserStreamTracker] = None

    @classmethod
    def setUpClass(cls):
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.dranite_auth = DraniteAuth(conf.dranite_api_key, conf.dranite_secret_key)
        cls.symbols = ["LTC-BTC"]
        cls.user_stream_tracker: DraniteUserStreamTracker = DraniteUserStreamTracker(
            dranite_auth=cls.dranite_auth, symbols=cls.symbols)
        cls.user_stream_tracker_task: asyncio.Task = safe_ensure_future(cls.user_stream_tracker.start())

    def test_user_stream(self):
        # Wait process some msgs.
        self.ev_loop.run_until_complete(asyncio.sleep(20.0))
        print(self.user_stream_tracker.user_stream)


def main():
    logging.basicConfig(level=logging.INFO)
    unittest.main()


if __name__ == "__main__":
    main()
