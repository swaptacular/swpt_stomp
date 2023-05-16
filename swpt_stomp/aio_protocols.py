from __future__ import annotations
from typing import Optional
import asyncio
from swpt_stomp.stomp_parser import StompParser, StompFrame

DEFAULT_QUEUE_SIZE = 1000
HEARTBEAT_MIN_MILLIS = 5_000  # 5 seconds
HEARTBEAT_OPTIMAL_MILLIS = 30_000  # 30 seconds


class StompClient(asyncio.Protocol):
    _queue: asyncio.Queue
    _host: Optional[str]
    _heartbeat_min: int
    _heartbeat_optimal: int
    _transport: Optional[asyncio.Transport]
    _parser: StompParser

    def __init__(
            self,
            *,
            queue_size: int = DEFAULT_QUEUE_SIZE,
            host: Optional[str] = None,
            heartbeat_min: int = HEARTBEAT_MIN_MILLIS,
            heartbeat_optimal: int = HEARTBEAT_OPTIMAL_MILLIS,
    ):
        self._queue = asyncio.Queue(queue_size)
        self._host = host
        self._heartbeat_min = heartbeat_min
        self._heartbeat_optimal = heartbeat_optimal
        self._parser = StompParser()
        self._transport = None
        self._loop = asyncio.get_event_loop()

    def connection_made(self, transport) -> None:
        self._transport = transport

        host = self._host
        if host is None:
            host, *others = transport.get_extra_info('peername')

        connect_frame = StompFrame(
            command='STOMP',
            headers={
                'accept-version': '1.2',
                'host': host,
                'heart-beat': f'{self._heartbeat_min},{self._heartbeat_optimal}',
            },
        )
        transport.write(bytes(connect_frame))

    def data_received(self, data: bytes):
        # self.transport.write(...)
        # self.transport.close()
        pass

    def connection_lost(self, exc):
        # TODO
        pass

    def pause_writing(self):
        # TODO
        pass

    def resume_writing(self):
        # TODO
        pass
