# -*- coding: utf-8 -*-
# Copyright 2020 The Matrix.org Foundation C.I.C.
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
import sys
import traceback
from collections import deque
from ipaddress import IPv4Address, IPv6Address, ip_address
from math import floor
from typing import Optional

import attr
from typing_extensions import Deque
from zope.interface import implementer

from twisted.application.internet import ClientService
from twisted.internet.defer import Deferred
from twisted.internet.endpoints import (
    HostnameEndpoint,
    TCP4ClientEndpoint,
    TCP6ClientEndpoint,
)
from twisted.internet.interfaces import IPushProducer, ITransport
from twisted.internet.protocol import Factory, Protocol
from twisted.logger import Logger


@attr.s
@implementer(IPushProducer)
class LogProducer:
    """
    An IPushProducer that writes logs from its buffer to its transport when it
    is resumed.

    Args:
        transport: Transport to write to.
        handler: The RemoteHandler instance to operate over.
    """

    transport = attr.ib(type=ITransport)
    _handler = attr.ib(type="Optional[RemoteHandler]")
    _paused = attr.ib(default=True, type=bool, init=False)

    def pauseProducing(self):
        self._paused = True

    def stopProducing(self):
        self._paused = True
        self._handler = None

    def resumeProducing(self):
        # If we're already producing, nothing to do.
        self._paused = False

        # _handler should always be set while this is resumed.
        assert self._handler

        # Loop until paused.
        while self._paused is False and (
            self._handler._buffer and self.transport.connected
        ):
            try:
                # Request the next record and format it.
                record = self._handler._buffer.popleft()
                msg = self._handler.format(record)

                # Send it as a new line over the transport.
                self.transport.write(msg.encode("utf8"))
                self.transport.write(b"\n")
            except Exception:
                # Something has gone wrong writing to the transport -- log it
                # and break out of the while.
                traceback.print_exc(file=sys.__stderr__)
                break


class RemoteHandler(logging.Handler):
    """
    An logging handler that writes logs to a TCP target.

    Args:
        host: The host of the logging target.
        port: The logging target's port.
        maximum_buffer: The maximum buffer size.
    """

    def __init__(
        self,
        host: str,
        port: int,
        maximum_buffer: int = 1000,
        level=logging.NOTSET,
        _reactor=None,
    ):
        super().__init__(level=level)
        self.host = host
        self.port = port
        self.maximum_buffer = maximum_buffer

        self._buffer = deque()  # type: Deque[logging.LogRecord]
        self._connection_waiter = None  # type: Optional[Deferred]
        self._logger = Logger()
        self._producer = None  # type: Optional[LogProducer]

        # Connect without DNS lookups if it's a direct IP.
        if _reactor is None:
            from twisted.internet import reactor

            _reactor = reactor

        try:
            ip = ip_address(self.host)
            if isinstance(ip, IPv4Address):
                endpoint = TCP4ClientEndpoint(_reactor, self.host, self.port)
            elif isinstance(ip, IPv6Address):
                endpoint = TCP6ClientEndpoint(_reactor, self.host, self.port)
            else:
                raise ValueError("Unknown IP address provided: %s" % (self.host,))
        except ValueError:
            endpoint = HostnameEndpoint(reactor, self.host, self.port)

        factory = Factory.forProtocol(Protocol)
        self._service = ClientService(endpoint, factory, clock=_reactor)
        self._service.startService()
        self._connect()

    def close(self):
        self._service.stopService()

    def _connect(self) -> None:
        """
        Triggers an attempt to connect then write to the remote if not already writing.
        """
        # Do not attempt to open multiple connections.
        if self._connection_waiter:
            return

        self._connection_waiter = self._service.whenConnected(failAfterFailures=1)

        @self._connection_waiter.addErrback
        def fail(r):
            r.printTraceback(file=sys.__stderr__)
            self._connection_waiter = None
            self._connect()

        @self._connection_waiter.addCallback
        def writer(r):
            # We have a connection. If we already have a producer, and its
            # transport is the same, just trigger a resumeProducing.
            if self._producer and r.transport is self._producer.transport:
                self._producer.resumeProducing()
                self._connection_waiter = None
                return

            # If the producer is still producing, stop it.
            if self._producer:
                self._producer.stopProducing()

            # Make a new producer and start it.
            self._producer = LogProducer(handler=self, transport=r.transport)
            r.transport.registerProducer(self._producer, True)
            self._producer.resumeProducing()
            self._connection_waiter = None

    def _handle_pressure(self) -> None:
        """
        Handle backpressure by shedding records.

        The buffer will, in this order, until the buffer is below the maximum:
            - Shed DEBUG records.
            - Shed INFO records.
            - Shed the middle 50% of the records.
        """
        if len(self._buffer) <= self.maximum_buffer:
            return

        # Strip out DEBUGs
        self._buffer = deque(
            filter(lambda record: record.levelno > logging.DEBUG, self._buffer)
        )

        if len(self._buffer) <= self.maximum_buffer:
            return

        # Strip out INFOs
        self._buffer = deque(
            filter(lambda record: record.levelno > logging.INFO, self._buffer)
        )

        if len(self._buffer) <= self.maximum_buffer:
            return

        # Cut the middle entries out
        buffer_split = floor(self.maximum_buffer / 2)

        old_buffer = self._buffer
        self._buffer = deque()

        for i in range(buffer_split):
            self._buffer.append(old_buffer.popleft())

        end_buffer = []
        for i in range(buffer_split):
            end_buffer.append(old_buffer.pop())

        self._buffer.extend(reversed(end_buffer))

    def emit(self, record: logging.LogRecord) -> None:
        self._buffer.append(record)

        # Handle backpressure, if it exists.
        try:
            self._handle_pressure()
        except Exception:
            # If handling backpressure fails, clear the buffer and log the
            # exception.
            self._buffer.clear()
            self._logger.failure("Failed clearing backpressure")

        # Try and write immediately.
        self._connect()
