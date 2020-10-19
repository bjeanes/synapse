# -*- coding: utf-8 -*-
# Copyright 2019 The Matrix.org Foundation C.I.C.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging

from synapse.logging import RemoteHandler

from tests.server import connect_client
from tests.unittest import DEBUG, HomeserverTestCase


class FakeBeginner:
    def beginLoggingTo(self, observers, **kwargs):
        self.observers = observers


class StructuredLoggingTestBase:
    """
    Test base that registers a cleanup handler to reset the stdlib log handler
    to 'unset'.
    """

    def prepare(self, reactor, clock, hs):
        def _cleanup():
            logging.getLogger("synapse").setLevel(logging.NOTSET)

        self.addCleanup(_cleanup)


class RemoteHandlerTestCase(StructuredLoggingTestBase, HomeserverTestCase):
    @DEBUG
    def test_log_output(self):
        """
        The Terse JSON outputter delivers simplified structured logs over TCP.
        """
        handler = RemoteHandler("127.0.0.1", 9000, reactor=self.reactor)
        logger = logging.getLogger()
        logger.addHandler(handler)

        logger.info("Hello there, %s!", "wally")

        # Trigger the connection
        self.pump()

        _, server = connect_client(self.reactor, 0)

        # Trigger data being sent
        self.pump()

        # One log message, with a single trailing newline
        logs = server.data.decode("utf8").splitlines()
        self.assertEqual(len(logs), 1)
        self.assertEqual(server.data.count(b"\n"), 1)

        # Ensure the data passed through properly.
        self.assertEqual(logs[0], "Hello there, wally!")

    @DEBUG
    def test_log_backpressure_debug(self):
        """
        When backpressure is hit, DEBUG logs will be shed.
        """
        handler = RemoteHandler(
            "127.0.0.1", 9000, maximum_buffer=10, reactor=self.reactor
        )
        logger = logging.getLogger()
        logger.addHandler(handler)

        # Send some debug messages
        for i in range(0, 3):
            logger.debug("debug %s" % (i,))

        # Send a bunch of useful messages
        for i in range(0, 7):
            logger.info("info %s" % (i,))

        # The last debug message pushes it past the maximum buffer
        logger.debug("too much debug")

        # Allow the reconnection
        _, server = connect_client(self.reactor, 0)
        self.pump()

        # Only the 7 infos made it through, the debugs were elided
        logs = server.data.splitlines()
        self.assertEqual(len(logs), 7)
        self.assertNotIn(b"debug", server.data)

    @DEBUG
    def test_log_backpressure_info(self):
        """
        When backpressure is hit, DEBUG and INFO logs will be shed.
        """
        handler = RemoteHandler(
            "127.0.0.1", 9000, maximum_buffer=10, reactor=self.reactor
        )
        logger = logging.getLogger()
        logger.addHandler(handler)

        # Send some debug messages
        for i in range(0, 3):
            logger.debug("debug %s" % (i,))

        # Send a bunch of useful messages
        for i in range(0, 10):
            logger.warning("warn %s" % (i,))

        # Send a bunch of info messages
        for i in range(0, 3):
            logger.info("info %s" % (i,))

        # The last debug message pushes it past the maximum buffer
        logger.debug("too much debug")

        # Allow the reconnection
        _, server = connect_client(self.reactor, 0)
        self.pump()

        # The 10 warnings made it through, the debugs and infos were elided
        logs = server.data.splitlines()
        self.assertEqual(len(logs), 10)
        self.assertNotIn(b"debug", server.data)
        self.assertNotIn(b"info", server.data)

    @DEBUG
    def test_log_backpressure_cut_middle(self):
        """
        When backpressure is hit, and no more DEBUG and INFOs cannot be culled,
        it will cut the middle messages out.
        """
        handler = RemoteHandler(
            "127.0.0.1", 9000, maximum_buffer=10, reactor=self.reactor
        )
        logger = logging.getLogger()
        logger.addHandler(handler)

        # Send a bunch of useful messages
        for i in range(0, 20):
            logger.warning("warn %s" % (i,))

        # Allow the reconnection
        _, server = connect_client(self.reactor, 0)
        self.pump()

        # The first five and last five warnings made it through, the debugs and
        # infos were elided
        logs = server.data.decode("utf8").splitlines()
        self.assertEqual(
            ["warn %s" % (i,) for i in range(5)]
            + ["warn %s" % (i,) for i in range(15, 20)],
            logs,
        )
