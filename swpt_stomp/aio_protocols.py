from typing import Optional, Union, Generic, TypeVar
import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from swpt_stomp.common import Message, WatermarkQueue, DEFAULT_MAX_NETWORK_DELAY
from swpt_stomp.stomp_parser import StompParser, StompFrame, ProtocolError

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
class ServerError:
    """Indicates that the server connection should be closed.

    Instances of this class are intended to be added to `StompServer`'s send
    queue, indicating that an error has occurred, and the connection must be
    closed.
    """
    error_message: str
    receipt_id: Optional[str] = None
    context: Optional[bytearray] = None
    context_type: Optional[str] = None
    context_content_type: Optional[str] = None


_U = TypeVar('_U')
_V = TypeVar('_V')


class _BaseStompProtocol(asyncio.Protocol, ABC, Generic[_U, _V]):
    """Implements functionality common for STOMP clients and servers.
    """
    send_queue: asyncio.Queue[Union[_U, None, ServerError]]
    recv_queue: WatermarkQueue[Union[_V, None]]

    def __init__(
            self,
            send_queue: asyncio.Queue[Union[_U, None, ServerError]],
            recv_queue: WatermarkQueue[Union[_V, None]],
            *,
            hb_send_min: int = DEFAULT_HB_SEND_MIN,
            hb_recv_desired: int = DEFAULT_HB_RECV_DESIRED,
            max_network_delay: int = DEFAULT_MAX_NETWORK_DELAY,
    ):
        assert hb_send_min >= 0
        assert hb_recv_desired >= 0

        self.send_queue = send_queue
        self.recv_queue = recv_queue
        self._hb_send_min = hb_send_min
        self._hb_recv_desired = hb_recv_desired
        self._max_network_delay = max_network_delay
        self._connected = False
        self._done = False
        self._hb_send = 0
        self._hb_recv = 0
        self._connection_started_at = 0.0
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
        self._connection_started_at = loop.time()
        self._transport = transport
        self.recv_queue.add_high_watermark_callback(transport.pause_reading)
        self.recv_queue.add_low_watermark_callback(transport.resume_reading)
        loop.call_later(self._max_network_delay / 1000,
                        self._detect_connected_timeout)

    def data_received(self, data: bytes) -> None:
        self._data_receved_at = self._loop.time()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self._done = True

        if self._writer_task:
            self._writer_task.cancel('connection lost')

        if self._watchdog_task:
            self._watchdog_task.cancel('connection lost')

        t = self._transport
        assert t
        self.recv_queue.remove_high_watermark_callback(t.pause_reading)
        self.recv_queue.remove_low_watermark_callback(t.resume_reading)
        self.recv_queue.put_nowait(None)  # Marks the end.

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
                raise ValueError
        except ValueError:
            self._close_with_error('Received invalid heart-beat value.')
            return

        self._hb_send = _calc_heartbeat(self._hb_send_min, hb_recv_desired)
        self._hb_recv = _calc_heartbeat(hb_send_min, self._hb_recv_desired)
        loop = self._loop

        if self._hb_send != 0:
            loop.call_at(self._connection_started_at + self._hb_send / 1000,
                         self._send_heartbeat)

        if self._hb_recv != 0:
            self._watchdog_task = loop.create_task(self._check_aliveness())

        self._writer_task = loop.create_task(self._process_send_queue())
        self._connected = True

    def _detect_connected_timeout(self) -> None:
        if not self._connected:
            _logger.warning('Protocol error: No response from peer.')
            self._close()

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
                self._close_with_warning(
                    f'No data received for {watchdog_seconds:.3f} seconds.')
                return

    async def _process_send_queue(self) -> None:
        queue = self.send_queue
        while await self._start_sending.wait():
            obj = await queue.get()
            if obj is None or isinstance(obj, ServerError):
                self._close_gracefully(obj)
                queue.task_done()
                break

            self._send_frame(obj)
            queue.task_done()

    def _close(self) -> None:
        if not self._done:
            assert self._transport
            self._transport.close()
            self._done = True

    @abstractmethod
    def _close_gracefully(self, error: Optional[ServerError]) -> None:
        raise NotImplementedError  # pragma: nocover

    @abstractmethod
    def _close_with_error(self, message: str) -> None:
        raise NotImplementedError  # pragma: nocover

    @abstractmethod
    def _close_with_warning(self, message: str) -> None:
        raise NotImplementedError  # pragma: nocover

    @abstractmethod
    def _send_frame(self, content: _U) -> None:
        raise NotImplementedError  # pragma: nocover


class StompClient(_BaseStompProtocol[Message, str]):
    """STOMP 1.2 client that sends messages to a STOMP 1.2 server.

    The send queue must contain an ordered sequence of messages, which will
    be sent to the server. Putting `None` in the send message queue will
    close the connection.

    The receive queue will contain an ordered sequence of confirmed message
    IDs. When a given message is confirmed, this also implicitly confirms
    all preceding messages. In fact, it is guaranteed that another
    confirmation will not be received for the given message, or the
    preceding messages. Also, when the connection is closed, a `None` will
    automatically be added to the receive queue.

    STOMP subscriptions and transactions are not supported. Also, this
    implementation sets a "persistent:true" header to all "SEND" frames, so
    as to instruct the server to send a receipt confirmation only after the
    message has been durably saved.
    """
    def __init__(
            self,
            send_queue: asyncio.Queue[Union[Message, None, ServerError]],
            recv_queue: WatermarkQueue[Union[str, None]],
            *,
            hb_send_min: int = DEFAULT_HB_SEND_MIN,
            hb_recv_desired: int = DEFAULT_HB_RECV_DESIRED,
            max_network_delay: int = DEFAULT_MAX_NETWORK_DELAY,
            host: str = '/',
            login: Optional[str] = None,
            passcode: Optional[str] = None,
            send_destination: str = '/exchange/smp',
    ):
        super().__init__(
            send_queue,
            recv_queue,
            hb_send_min=hb_send_min,
            hb_recv_desired=hb_recv_desired,
            max_network_delay=max_network_delay,
        )
        self._host = host
        self._login = login
        self._passcode = passcode
        self._send_destination = send_destination
        self._last_message_id: Optional[str] = None
        self._last_receipt_id: Optional[str] = None
        self._sent_disconnect = False

    def connection_made(
            self,
            transport: asyncio.Transport,  # type: ignore[override]
    ) -> None:
        super().connection_made(transport)
        headers = {
            'accept-version': '1.2',
            'host': self._host,
            'heart-beat': f'{self._hb_send_min},{self._hb_recv_desired}',
        }
        if self._login is not None:
            headers['login'] = self._login
        if self._passcode is not None:
            headers['passcode'] = self._passcode

        connect_frame = StompFrame(command='STOMP', headers=headers)
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

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if not self._done:
            _logger.warning('The connection to the STOMP server has been lost.')

        super().connection_lost(exc)

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

        self.recv_queue.put_nowait(receipt_id)
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
                'type': message.type,
                'content-type': message.content_type,
                'persistent': 'true',
                'receipt': message.id,
            },
            body=message.body,
        )
        assert self._transport
        self._transport.write(bytes(message_frame))
        self._last_message_id = message.id

    def _close_gracefully(self, error: Optional[ServerError]) -> None:
        last_msg_id = self._last_message_id
        if last_msg_id is None or last_msg_id == self._last_receipt_id:
            # All sent messages have been confirmed.
            self._close()
            return

        disconnect_frame = StompFrame('DISCONNECT', {'receipt': last_msg_id})
        assert self._transport
        self._transport.write(bytes(disconnect_frame))
        self._sent_disconnect = True

    def _close_with_error(self, message: str) -> None:
        _logger.error('Protocol error: %s', message)
        self._close()

    def _close_with_warning(self, message: str) -> None:
        _logger.warning('Protocol warning: %s', message)
        self._close()


class StompServer(_BaseStompProtocol[str, Message]):
    """STOMP 1.2 server that receives messages from a STOMP 1.2 client.

    The receive queue will contain an ordered sequence of messages, received
    from the client. Also, when the connection is closed, a `None` will be
    automatically added to the receive queue.

    The send queue must contain an ordered sequence of confirmed message
    IDs. When a given message is confirmed, this also implicitly confirms
    all preceding messages. In fact, it must be guaranteed that another
    confirmation will not be received for the given message, or the
    preceding messages. Putting `None`, or a `ServerError` instance, in the
    send queue will close the connection.

    NOTE: STOMP subscriptions and transactions are not supported. If a
    "SUBSCRIBE", "UNSUBSCRIBE", "ACK", "NACK", "BEGIN", "COMMIT", or "ABORT"
    command is received, the server will reply with an error. Also, this
    implementation requires all "SEND" frames to have "receipt" and "type"
    headers. Sending a message without a "type", or not requiring a
    confirmation from the server, simply does not make sense in
    Swaptacular's context.
    """
    def __init__(
            self,
            send_queue: asyncio.Queue[Union[str, None, ServerError]],
            recv_queue: WatermarkQueue[Union[Message, None]],
            *,
            hb_send_min: int = DEFAULT_HB_SEND_MIN,
            hb_recv_desired: int = DEFAULT_HB_RECV_DESIRED,
            max_network_delay: int = DEFAULT_MAX_NETWORK_DELAY,
            recv_destination: str = '/exchange/smp'
    ):
        super().__init__(
            send_queue,
            recv_queue,
            hb_send_min=hb_send_min,
            hb_recv_desired=hb_recv_desired,
            max_network_delay=max_network_delay,
        )
        self._recv_destination = recv_destination
        self._last_message_id: Optional[str] = None
        self._last_receipt_id: Optional[str] = None
        self._disconnect_receipt_id: Optional[str] = None

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
            elif cmd == 'STOMP' or cmd == 'CONNECT':
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
        versions = set(v for v in accept_version.split(',', 10))
        if '1.2' not in versions:
            self._close_with_error('STOMP 1.2 is not supported by the client.')
            return

        self._connection_started_at = self._loop.time()
        self._connect(frame.headers.get('heart-beat', '0,0'))

        if self._connected:
            connected_frame = StompFrame(
                command='CONNECTED',
                headers={
                    'version': '1.2',
                    'heart-beat': f'{self._hb_send_min},{self._hb_recv_desired}',
                },
            )
            assert self._transport
            self._transport
            self._transport.write(bytes(connected_frame))

    def _received_send_command(self, frame: StompFrame) -> None:
        if not self._connected:
            self._close_with_error('Received another command before CONNECT.')
            return

        headers = frame.headers
        content_type = headers.get('content-type', 'application/octet-stream')
        try:
            message_id = headers['receipt']
            message_type = headers['type']
            destination = headers['destination']
        except KeyError as e:
            header = e.args[0]
            self._close_with_error(f'SEND command without a {header} header.')
            return

        if destination != self._recv_destination:
            self._close_with_error(
                f'Invalid SEND destination "{destination}".',
                message_id,
                frame.body,
                message_type,
                content_type,
            )
            return

        message = Message(
            id=message_id,
            type=message_type,
            content_type=content_type,
            body=frame.body,
        )
        self.recv_queue.put_nowait(message)
        self._last_message_id = message_id

    def _received_disconnect_command(self, frame: StompFrame) -> None:
        if not self._connected:
            self._close_with_error('Received DISCONNECT before CONNECT.')
            return

        try:
            self._disconnect_receipt_id = frame.headers['receipt']
        except KeyError:
            self._close()
            return

        last_msg_id = self._last_message_id
        if last_msg_id is None or last_msg_id == self._last_receipt_id:
            # All received messages have been confirmed.
            self._send_receipt_command(self._disconnect_receipt_id)
            self._close()

    def _send_receipt_command(self, receipt_id: str) -> None:
        receipt_frame = StompFrame('RECEIPT', {'receipt-id': receipt_id})
        assert self._transport
        self._transport.write(bytes(receipt_frame))

    def _send_frame(self, receipt_id: str) -> None:
        if self._done:
            return

        disconnect_id = self._disconnect_receipt_id
        if disconnect_id is not None and receipt_id == self._last_message_id:
            self._send_receipt_command(disconnect_id)
            self._close()
            return

        self._send_receipt_command(receipt_id)
        self._last_receipt_id = receipt_id

    def _send_error_frame(
            self,
            message: str,
            receipt_id: Optional[str] = None,
            context: Optional[bytearray] = None,
            context_type: Optional[str] = None,
            context_content_type: Optional[str] = None,
    ) -> None:
        transport = self._transport
        assert transport
        if not transport.is_closing():
            headers = {'message': message}
            if receipt_id is not None:
                headers['receipt-id'] = receipt_id
            if context is not None and context_type is not None:
                headers['type'] = context_type
            if context is not None and context_content_type is not None:
                headers['content-type'] = context_content_type

            body = bytearray() if context is None else context
            error_frame = StompFrame('ERROR', headers, body)
            transport.write(bytes(error_frame))

    def _close_gracefully(self, error: Optional[ServerError]) -> None:
        if isinstance(error, ServerError):
            self._close_with_error(
                error.error_message,
                error.receipt_id,
                error.context,
                error.context_type,
                error.context_content_type,
            )
            return

        self._close_with_error('The connection has been closed by the server.')

    def _close_with_error(
            self,
            message: str,
            receipt_id: Optional[str] = None,
            context: Optional[bytearray] = None,
            context_type: Optional[str] = None,
            context_content_type: Optional[str] = None,
    ) -> None:
        # Log this as a warning, since this is client's problem, not ours.
        _logger.warning('Protocol error: %s', message)

        self._send_error_frame(
            message,
            receipt_id,
            context,
            context_type,
            context_content_type,
        )
        self._close()

    def _close_with_warning(self, message: str) -> None:
        _logger.warning('Protocol warning: %s', message)
        self._send_error_frame(message)
        self._close()
