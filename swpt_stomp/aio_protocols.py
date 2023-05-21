from __future__ import annotations
from typing import Optional, Union, Generic, TypeVar
import asyncio
import logging
from swpt_stomp.common import Message, WatermarkQueue
from swpt_stomp.stomp_parser import StompParser, StompFrame, ProtocolError

DEFAULT_HB_SEND_MIN = 5_000  # 5 seconds
DEFAULT_HB_RECV_DESIRED = 30_000  # 30 seconds
DEFAULT_MAX_NETWORK_DELAY = 10_000  # 10 seconds

# TODO: Configure an async logging handler, as explained here:
#       https://stackoverflow.com/questions/45842926/python-asynchronous-logging
_logger = logging.getLogger(__name__)


def _calc_heartbeat(send_min: int, recv_desired: int) -> int:
    """Implement the heartbeat logic described in the STOMP specification.
    """
    if send_min == 0 or recv_desired == 0:
        return 0  # no heartbeats

    return max(send_min, recv_desired)


_U = TypeVar('_U')
_V = TypeVar('_V')


class _BaseStompProtocol(asyncio.Protocol, Generic[_U, _V]):
    """Implements functionality common for STOMP clients and servers.
    """
    input_queue: asyncio.Queue[Union[_U, None]]
    output_queue: WatermarkQueue[Union[_V, None]]

    def __init__(
            self,
            input_queue: asyncio.Queue[Union[_U, None]],
            output_queue: WatermarkQueue[Union[_V, None]],
            *,
            hb_send_min: int = DEFAULT_HB_SEND_MIN,
            hb_recv_desired: int = DEFAULT_HB_RECV_DESIRED,
            max_network_delay: int = DEFAULT_MAX_NETWORK_DELAY,
    ):
        assert hb_send_min >= 0
        assert hb_recv_desired >= 0

        self.input_queue = input_queue
        self.output_queue = output_queue
        self._hb_send_min = hb_send_min
        self._hb_recv_desired = hb_recv_desired
        self._max_network_delay = max_network_delay
        self._connected = False
        self._done = False
        self._hb_send = 0
        self._hb_recv = 0
        self._connection_made_at = 0.0
        self._data_receved_at = 0.0
        self._loop = asyncio.get_event_loop()
        self._parser = StompParser()
        self._transport: Optional[asyncio.Transport] = None
        self._writer_task: Optional[asyncio.Task] = None
        self._watchdog_task: Optional[asyncio.Task] = None
        self._start_sending = asyncio.Event()
        self._start_sending.set()

    def connection_made(
            self,
            transport: asyncio.Transport,  # type: ignore[override]
    ) -> None:
        loop = self._loop
        self._connection_made_at = loop.time()
        self._transport = transport
        self.output_queue.add_high_watermark_callback(transport.pause_reading)
        self.output_queue.add_low_watermark_callback(transport.resume_reading)
        loop.call_later(self._max_network_delay / 1000,
                        self._detect_connected_timeout)

    def data_received(self, data: bytes) -> None:
        self._data_receved_at = self._loop.time()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self._done = True

        if self._writer_task:
            self._writer_task.cancel('connection lost')
            self._writer_task = None

        if self._watchdog_task:
            self._watchdog_task.cancel('connection lost')
            self._watchdog_task = None

        t = self._transport
        assert t
        self.output_queue.remove_high_watermark_callback(t.pause_reading)
        self.output_queue.remove_low_watermark_callback(t.resume_reading)
        self.output_queue.put_nowait(None)  # Marks the end.

    def pause_writing(self) -> None:
        self._start_sending.clear()

    def resume_writing(self) -> None:
        self._start_sending.set()

    def _connect(self, peer_heartbeat: str) -> None:
        try:
            hb_send_min, hb_recv_desired = [
                int(n) for n in peer_heartbeat.split(',', 2)
            ]
            if hb_send_min < 0 or hb_recv_desired < 0:
                raise ValueError()
        except ValueError:
            self._close_with_error('Received invalid heart-beat value.')
            return

        self._connected = True
        self._hb_send = _calc_heartbeat(self._hb_send_min, hb_recv_desired)
        self._hb_recv = _calc_heartbeat(hb_send_min, self._hb_recv_desired)
        loop = self._loop

        if self._hb_send != 0:
            loop.call_at(self._connection_made_at + self._hb_send / 1000,
                         self._send_heartbeat)

        if self._hb_recv != 0:
            self._watchdog_task = loop.create_task(self._check_aliveness())

        self._writer_task = loop.create_task(self._process_input_queue())

    def _detect_connected_timeout(self) -> None:
        if not self._connected:
            _logger.warning('Protocol error: No response from peer.')
            assert self._transport
            self._transport.close()
            self._done = True

    def _send_heartbeat(self) -> None:
        if not self._done:
            assert self._transport
            self._transport.write(b'\n')
            self._loop.call_later(self._hb_send / 1000, self._send_heartbeat)

    async def _check_aliveness(self) -> None:
        loop = self._loop
        hb_recv_with_tolerance = self._hb_recv + self._max_network_delay
        sleep_seconds = DEFAULT_HB_SEND_MIN / 1000
        watchdog_seconds = sleep_seconds + hb_recv_with_tolerance / 1000
        while not self._done:
            await asyncio.sleep(sleep_seconds)
            if loop.time() - self._data_receved_at > watchdog_seconds:
                self._close_with_error(
                    f'No data received for {watchdog_seconds:.3f} seconds.')
                return

    def _close(self) -> None:
        assert self._transport
        self._transport.close()
        self._done = True

    def _close_gracefully(self) -> None:
        raise NotImplementedError()  # pragma: nocover

    def _close_with_error(self, message: str) -> None:
        raise NotImplementedError()  # pragma: nocover

    def _send_frame(self, content: _U) -> None:
        raise NotImplementedError()  # pragma: nocover

    async def _process_input_queue(self) -> None:
        queue = self.input_queue
        while await self._start_sending.wait():
            obj = await queue.get()
            if obj is None:
                self._close_gracefully()
                return

            self._send_frame(obj)
            queue.task_done()


class StompClient(_BaseStompProtocol[Message, str]):
    """STOMP client that sends messages to STOMP server.

    Putting `None` in the input message queue will close the connection.
    Also, when the connection is closed, a `None` will be added to the
    output queue.
    """
    def __init__(
            self,
            input_queue: asyncio.Queue[Union[Message, None]],
            output_queue: WatermarkQueue[Union[str, None]],
            *,
            hb_send_min: int = DEFAULT_HB_SEND_MIN,
            hb_recv_desired: int = DEFAULT_HB_RECV_DESIRED,
            max_network_delay: int = DEFAULT_MAX_NETWORK_DELAY,
            host: Optional[str] = None,
            send_destination: str = 'smp',
    ):
        super().__init__(
            input_queue,
            output_queue,
            hb_send_min=hb_send_min,
            hb_recv_desired=hb_recv_desired,
            max_network_delay=max_network_delay,
        )
        self._host = host
        self._send_destination = send_destination
        self._last_message_id: Union[str, None] = None
        self._last_receipt_id: Union[str, None] = None
        self._sent_disconnect = False

    def connection_made(
            self,
            transport: asyncio.Transport,  # type: ignore[override]
    ) -> None:
        super().connection_made(transport)

        host = self._host
        if host is None:
            peername = transport.get_extra_info('peername')
            host = 'default' if (peername is None) else peername[0]

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
        super().data_received(data)

        if self._done:
            return

        parser = self._parser
        try:
            parser.add_bytes(data)
        except ProtocolError as e:
            self._close_with_error(e.message)
            return

        for frame in parser:
            cmd = frame.command
            if cmd == 'CONNECTED':
                self._received_connected_command(frame)
            elif cmd == 'RECEIPT':
                self._received_receipt_command(frame)
            elif cmd == 'ERROR':
                self._received_error_command(frame)
            else:
                self._close_with_error(f'Received unexpected command "{cmd}".')

            if self._done:
                break

    def _received_connected_command(self, frame: StompFrame) -> None:
        if self._connected:
            self._close_with_error('Received CONNECTED command more than once.')
            return

        if frame.headers.get('version') != '1.2':
            self._close_with_error('Received wrong protocol version.')
            return

        self._connect(frame.headers.get('heart-beat', '0,0'))

    def _received_receipt_command(self, frame: StompFrame) -> None:
        if not self._connected:
            self._close_with_error('Received another command before CONNECTED.')
            return

        try:
            receipt_id = frame.headers['receipt-id']
        except KeyError:
            self._close_with_error('RECEIPT command without a receipt ID.')
            return

        self.output_queue.put_nowait(receipt_id)
        self._last_receipt_id = receipt_id

        if self._sent_disconnect and receipt_id == self._last_message_id:
            self._close()

    def _received_error_command(self, frame: StompFrame) -> None:
        error_message = frame.headers.get('message', '')
        self._close_with_error(f'Received server error "{error_message}".')

    def _send_frame(self, message: Message) -> None:
        if self._done:
            return

        message_frame = StompFrame(
            command='SEND',
            headers={
                'destination': self._send_destination,
                'content-type': message.content_type,
                'receipt': message.id,
            },
            body=message.body,
        )
        assert self._transport
        self._transport.write(bytes(message_frame))
        self._last_message_id = message.id

    def _close_gracefully(self) -> None:
        if self._last_message_id is None:
            self._last_message_id = 'disconnect'

        disconnect_frame = StompFrame(
            'DISCONNECT', {'receipt': self._last_message_id})
        assert self._transport
        self._transport.write(bytes(disconnect_frame))
        self._sent_disconnect = True

    def _close_with_error(self, message: str) -> None:
        _logger.warning('Protocol error: %s', message)
        self._close()


class StompServer(_BaseStompProtocol[str, Message]):
    """STOMP server that receives messages from STOMP client.

    Putting `None` in the input message queue will close the connection.
    Also, when the connection is closed, a `None` will be added to the
    output queue.
    """
    def __init__(
            self,
            input_queue: asyncio.Queue[Union[str, None]],
            output_queue: WatermarkQueue[Union[Message, None]],
            *,
            hb_send_min: int = DEFAULT_HB_SEND_MIN,
            hb_recv_desired: int = DEFAULT_HB_RECV_DESIRED,
            max_network_delay: int = DEFAULT_MAX_NETWORK_DELAY,
            recv_destination: str = 'smp'
    ):
        super().__init__(
            input_queue,
            output_queue,
            hb_send_min=hb_send_min,
            hb_recv_desired=hb_recv_desired,
            max_network_delay=max_network_delay,
        )
        self._recv_destination = recv_destination
        self._last_message_id: Union[str, None] = None
        self._last_receipt_id: Union[str, None] = None
        self._disconnect_receipt_id: Union[str, None] = None

    def data_received(self, data: bytes) -> None:
        super().data_received(data)

        if self._done:
            return

        parser = self._parser
        try:
            parser.add_bytes(data)
        except ProtocolError as e:
            self._close_with_error(e.message)
            return

        for frame in parser:
            cmd = frame.command
            if self._disconnect_receipt_id is not None:
                self._close_with_error('Received command after DISCONNECT.')
            elif cmd == 'CONNECT' or cmd == 'STOMP':
                self._received_connect_command(frame)
            elif cmd == 'SEND':
                self._received_send_command(frame)
            elif cmd == 'DISCONNECT':
                self._received_disconnect_command(frame)
            else:
                self._close_with_error(f'Received unexpected command "{cmd}".')

            if self._done:
                break

    def _received_connect_command(self, frame: StompFrame) -> None:
        if self._connected:
            self._close_with_error('Received CONNECT command more than once.')
            return

        accept_version = frame.headers.get('accept-version', '')
        versions = set(v for v in accept_version.split(',', 20))
        if '1.2' not in versions:
            self._close_with_error('STOMP 1.2 is not supported by the client.')
            return

        self._connect(frame.headers.get('heart-beat', '0,0'))

    def _received_send_command(self, frame: StompFrame) -> None:
        if not self._connected:
            self._close_with_error('Received another command before CONNECT.')
            return

        headers = frame.headers
        try:
            message_id = headers['receipt']
        except KeyError:
            self._close_with_error('SEND command without a receipt header.')
            return

        if headers.get('destination') != self._recv_destination:
            self._close_with_error('Invalid SEND destination.', message_id)
            return

        content_type = headers.get('content-type', 'application/octet-stream')
        message = Message(
            id=message_id,
            content_type=content_type,
            body=frame.body,
        )
        self.output_queue.put_nowait(message)
        self._last_message_id = message_id

    def _received_disconnect_command(self, frame: StompFrame) -> None:
        if not self._connected:
            self._close_with_error('Received DISCONNECT before CONNECT.')
            return

        try:
            self._disconnect_receipt_id = frame.headers['receipt']
        except KeyError:
            self._close_with_error('DISCONNECT command without a receipt ID.')
            return

        if (self._last_message_id is None
                or self._last_message_id == self._last_receipt_id):
            self._send_receipt_command(self._disconnect_receipt_id)
            self._close()

    def _send_receipt_command(self, receipt_id: str) -> None:
        receipt_frame = StompFrame('RECEIPT', {'receipt-id': receipt_id})
        assert self._transport
        self._transport.write(bytes(receipt_frame))

    def _send_frame(self, receipt_id: str) -> None:
        if self._done:
            return

        if (self._disconnect_receipt_id is not None
                and self._last_message_id == receipt_id):
            self._send_receipt_command(self._disconnect_receipt_id)
            self._close()
            return

        self._send_receipt_command(receipt_id)
        self._last_receipt_id = receipt_id

    def _close_gracefully(self) -> None:
        self._close_with_error('The connection has been closed by the server.')

    def _close_with_error(
            self,
            message: str,
            receipt_id: Optional[str] = None,
    ) -> None:
        transport = self._transport
        assert transport
        if not transport.is_closing():
            headers = {'message': message}
            if receipt_id is not None:
                headers['receipt-id'] = receipt_id

            error_frame = StompFrame('ERROR', headers)
            transport.write(bytes(error_frame))

        _logger.warning('Protocol error: %s', message)
        self._close()
