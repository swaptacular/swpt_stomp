from __future__ import annotations
from typing import Optional
import asyncio
import logging
from dataclasses import dataclass
from swpt_stomp.stomp_parser import StompParser, StompFrame, ProtocolError

DEFAULT_QUEUE_SIZE = 1000
DEFAULT_HB_SEND_MIN = 5_000  # 5 seconds
DEFAULT_HB_RECV_DESIRED = 30_000  # 30 seconds

_logger = logging.getLogger(__name__)


def _calc_heartbeat(send_min: int, recv_desired: int) -> int:
    """Implement the heartbeat logic described in the STOMP specification.
    """
    if send_min == 0 or recv_desired == 0:
        return 0  # no heartbeats

    return max(send_min, recv_desired)


@dataclass
class Message:
    id: str
    content_type: str
    body: bytearray


class StompClient(asyncio.Protocol):
    """STOMP client that sends messages to STOMP server."""

    queue: asyncio.Queue[Message]

    def __init__(
            self,
            *,
            host: Optional[str] = None,
            queue_size: int = DEFAULT_QUEUE_SIZE,
            hb_send_min: int = DEFAULT_HB_SEND_MIN,
            hb_recv_desired: int = DEFAULT_HB_RECV_DESIRED,
    ):
        self.queue: asyncio.Queue[Message] = asyncio.Queue(queue_size)
        self._transport: Optional[asyncio.WriteTransport] = None
        self._loop = asyncio.get_event_loop()
        self._host = host
        self._connected = False
        self._closed = False
        self._paused = False  # TODO: This probably should be a condition!
        self._hb_send_min = hb_send_min
        self._hb_recv_desired = hb_recv_desired
        self._hb_send = 0
        self._hb_recv = 0
        self._parser = StompParser()
        self._writer_task: Optional[asyncio.Task] = None

    def connection_made(self, transport: asyncio.WriteTransport) -> None:  # type: ignore[override]
        self._transport = transport

        host = self._host
        if host is None:
            # TODO: This is probably not reliable!
            host, *others = transport.get_extra_info('peername')

        connect_frame = StompFrame(
            command='CONNECT',
            headers={
                'accept-version': '1.2',
                'host': host,
                'heart-beat': f'{self._hb_send_min},{self._hb_recv_desired}',
            },
        )
        transport.write(bytes(connect_frame))

    def data_received(self, data: bytes) -> None:
        if self._closed:
            return

        parser = self._parser
        try:
            parser.add_bytes(data)
        except ProtocolError as e:
            self._close_with_error(e.message)
            return

        for frame in parser:
            command = frame.command
            if command == 'CONNECTED':
                self._received_connected_command(frame)
            elif command == 'RECEIPT':
                self._received_receipt_command(frame)
            elif command == 'ERROR':
                self._received_error_command(frame)
            else:
                self._close_with_error(f'Received unexpected command "{command}".')

            if self._closed:
                break

    def connection_lost(self, exc: Optional[Exception]) -> None:
        # TODO
        pass

    def pause_writing(self) -> None:
        self._paused = True

    def resume_writing(self) -> None:
        self._paused = False

    def send_message(self, message: Message) -> None:
        if self._closed:
            return

        transport = self._transport
        if not (transport and self._connected):
            raise RuntimeError('An attempt has been made to send a message over a '
                               'connection that is not ready to receive messages.')

        message_frame = StompFrame(
            command='SEND',
            headers={
                'destination': 'smp',
                'content-type': message.content_type,
                'receipt': message.id,
            },
            body=message.body,
        )
        transport.write(bytes(message_frame))

    def _received_connected_command(self, frame: StompFrame):
        if self._connected:
            self._close_with_error("Received CONNECTED command more than once.")
        elif frame.headers.get('version') != '1.2':
            self._close_with_error("Received wrong protocol version.")
        else:
            heartbeat = frame.headers.get('heart-beat', '0,0')
            try:
                hb_send_min, hb_recv_desired = [int(n) for n in heartbeat.split(',', 2)]
            except ValueError:
                self._close_with_error("Received invalid heart-beat value.")
            else:
                self._hb_send = _calc_heartbeat(self._hb_send_min, hb_recv_desired)
                self._hb_recv = _calc_heartbeat(hb_send_min, self._hb_recv_desired)
                self._connected = True
                self._writer_task = self._loop.create_task(self._process_messages(), name='Reader')

    def _received_receipt_command(self, frame: StompFrame) -> None:
        if self._connected:
            try:
                receipt_id = frame.headers['receipt-id']
            except KeyError:
                self._close_with_error('Received a RECEIPT command without a receipt ID.')
            else:
                self._received_ack(receipt_id)
        else:
            self._close_with_error("Received other command, without receiving CONNECTED.")

    def _received_error_command(self, frame: StompFrame) -> None:
        if self._connected:
            error_message = frame.headers.get('message', '')
            self._close_with_error(f'Received server error "{error_message}".')
        else:
            self._close_with_error("Received other command, without receiving CONNECTED.")

    def _received_ack(self, receipt_id: str) -> None:
        # TODO:
        pass

    def _close_with_error(self, message: str) -> None:
        assert self._transport
        _logger.warning('Protocol error: %s', message)
        self._transport.close()
        self._closed = True

    async def _process_messages(self):
        while True:
            message = await self.queue.get()
            self.send_message(message)


class StompServer(asyncio.Protocol):
    _transport: Optional[asyncio.Transport]

    def _send_error(self, headers: dict[str, str]):
        transport = self._transport
        assert transport
        error_frame = StompFrame('ERROR', headers)
        transport.write(bytes(error_frame))
        transport.close()
