import logging
import asyncio
import aio_pika
from functools import partial
from dataclasses import dataclass
from typing import Optional, Union, Callable
from collections import deque
from aio_pika.abc import HeadersType
from aio_pika.exceptions import CONNECTION_EXCEPTIONS
from pamqp.commands import Basic
from swpt_stomp.common import Message, WatermarkQueue, DEFAULT_MAX_NETWORK_DELAY
from swpt_stomp.aio_protocols import ServerError

_logger = logging.getLogger(__name__)
_RMQ_CONNECTION_ERRORS = CONNECTION_EXCEPTIONS + (asyncio.TimeoutError,)
_RMQ_RECONNECT_ATTEMPT_SECONDS = 10.0
_PERSISTENT = aio_pika.DeliveryMode.PERSISTENT


@dataclass
class _Delivery:
    __slots__ = ('message_id', 'confirmed')
    message_id: str
    confirmed: bool


@dataclass
class SmpMessage:
    body: bytearray
    headers: HeadersType
    type: str
    content_type: str
    routing_key: str


async def consume_rmq_queue(
        send_queue: asyncio.Queue[Union[Message, None, ServerError]],
        recv_queue: WatermarkQueue[Union[str, None]],
        *,
        rmq_url: str,
        queue_name: str,
        prefetch_size: int = 0,
        timeout: Optional[float] = DEFAULT_MAX_NETWORK_DELAY / 1000,
        transform_message_body: Callable[[bytes], bytearray] = bytearray,
) -> None:
    """Consumes from a RabbitMQ queue until the STOMP connection is closed.

    If the connection to the RabbitMQ server has been lost for some reason,
    attempts to reconnect will be made ad infinitum. `send_queue.maxsize`
    will determine the queue's prefetch count. If passed, the
    `transform_message_body` function may change the message body before
    sending it over the STOMP connection.
    """
    while True:
        try:
            await _consume_rmq_queue(
                send_queue,
                recv_queue,
                rmq_url=rmq_url,
                queue_name=queue_name,
                prefetch_size=prefetch_size,
                timeout=timeout,
                transform_message_body=transform_message_body,
            )
        except _RMQ_CONNECTION_ERRORS:  # This catches several error types.
            _logger.exception('Lost connection to %s.', rmq_url)
            await asyncio.sleep(_RMQ_RECONNECT_ATTEMPT_SECONDS)
        else:
            # `None` has been posted to the `recv_queue`, which means that
            # the STOMP connection has been closed.
            break


async def publish_to_rmq_exchange(
        send_queue: asyncio.Queue[Union[str, None, ServerError]],
        recv_queue: WatermarkQueue[Union[Message, None]],
        *,
        rmq_url: str,
        exchange_name: str,
        transform_message: Callable[[Message], SmpMessage],
        timeout: Optional[float] = DEFAULT_MAX_NETWORK_DELAY / 1000,
) -> None:
    while True:
        try:
            await _publish_to_rmq_exchange(
                send_queue,
                recv_queue,
                rmq_url=rmq_url,
                exchange_name=exchange_name,
                transform_message=transform_message,
                timeout=timeout,
            )
        except _RMQ_CONNECTION_ERRORS:  # This catches several error types.
            _logger.exception('Lost connection to %s.', rmq_url)
            await asyncio.sleep(_RMQ_RECONNECT_ATTEMPT_SECONDS)
        else:
            # `None` has been posted to the `recv_queue`, which means that
            # the STOMP connection has been closed.
            break


async def _consume_rmq_queue(
        send_queue: asyncio.Queue[Union[Message, None, ServerError]],
        recv_queue: WatermarkQueue[Union[str, None]],
        *,
        rmq_url: str,
        queue_name: str,
        prefetch_size: int = 0,
        timeout: Optional[float] = DEFAULT_MAX_NETWORK_DELAY / 1000,
        transform_message_body: Callable[[bytes], bytearray] = bytearray,
) -> None:
    _logger.info('Connecting to %s.', rmq_url)
    connection = await aio_pika.connect(rmq_url, timeout=timeout)

    async with connection:
        channel = await asyncio.wait_for(connection.channel(), timeout)

        await channel.set_qos(
            prefetch_count=max(send_queue.maxsize, 1),
            prefetch_size=prefetch_size,
            timeout=timeout,
        )

        async def consume_queue() -> None:
            queue = await channel.get_queue(queue_name, ensure=False)
            async with queue.iterator() as queue_iter:
                _logger.info('Started consuming from %s.', queue_name)
                async for message in queue_iter:
                    message_type = message.type
                    if message_type is None:
                        # It would be more natural to raise a RuntimeError
                        # here, but this would be erroneously treated as a
                        # connection error, because RuntimeError is in
                        # aio_pika's CONNECTION_EXCEPTIONS.
                        raise ValueError('Message without a type.')

                    message_content_type = message.content_type
                    if message_content_type is None:
                        raise ValueError('Message without a content-type.')

                    delivery_tag = message.delivery_tag
                    if delivery_tag is None:
                        raise ValueError('Message without a delivery tag.')

                    message_body = transform_message_body(message.body)
                    await send_queue.put(
                        Message(
                            id=str(delivery_tag),
                            type=message_type,
                            body=message_body,
                            content_type=message_content_type,
                        ))

        async def send_acks() -> None:
            aiormq_channel = channel.channel
            while receipt_id := await recv_queue.get():
                try:
                    delivery_tag = int(receipt_id)
                except ValueError:
                    _logger.error('Invalid receipt-id: %s', receipt_id)
                else:
                    await aiormq_channel.basic_ack(delivery_tag, multiple=True)
                recv_queue.task_done()

            consume_queue_task.cancel()

        loop = asyncio.get_event_loop()
        consume_queue_task = loop.create_task(consume_queue())
        send_acks_task = loop.create_task(send_acks())
        try:
            await asyncio.gather(consume_queue_task, send_acks_task)
        except asyncio.CancelledError:
            pass
        finally:
            consume_queue_task.cancel()
            send_acks_task.cancel()

    _logger.info('Disconnected from %s.', rmq_url)


async def _publish_to_rmq_exchange(
        send_queue: asyncio.Queue[Union[str, None, ServerError]],
        recv_queue: WatermarkQueue[Union[Message, None]],
        *,
        rmq_url: str,
        exchange_name: str,
        transform_message: Callable[[Message], SmpMessage],
        timeout: Optional[float] = DEFAULT_MAX_NETWORK_DELAY / 1000,
) -> None:
    _logger.info('Connecting to %s.', rmq_url)
    connection = await aio_pika.connect(rmq_url, timeout=timeout)

    async with connection:
        channel = await asyncio.wait_for(connection.channel(), timeout)
        exchange = await channel.get_exchange(exchange_name, ensure=False)
        deliveries: deque[_Delivery] = deque()
        pending_confirmations: set[asyncio.Future] = set()
        has_confirmed_deliveries = asyncio.Event()
        has_failed_confirmations = asyncio.Event()
        failed_confirmation: Optional[asyncio.Future] = None

        def on_confirmation(
                delivery: _Delivery,
                confirmation: asyncio.Future,
        ) -> None:
            nonlocal failed_confirmation
            pending_confirmations.remove(confirmation)

            if confirmation.cancelled() or confirmation.exception():
                if failed_confirmation is None:
                    failed_confirmation = confirmation
                    has_failed_confirmations.set()
                return

            delivery.confirmed = True
            has_confirmed_deliveries.set()

        async def publish(message: Message) -> None:
            m = transform_message(message)
            result = await exchange.publish(
                aio_pika.Message(
                    m.body,
                    headers=m.headers,
                    type=m.type,
                    content_type=m.content_type,
                    delivery_mode=_PERSISTENT,
                    app_id='swpt_stomp',
                ),
                routing_key=m.routing_key,
                mandatory=True,
                timeout=timeout,  # TODO: is this too small?
            )
            if result != Basic.Ack:
                raise Exception(
                    'Did not receive acknowledgement for message %s.',
                    message.id)

        async def publish_messages() -> None:
            while message := await recv_queue.get():
                delivery = _Delivery(message.id, False)

                # TODO: Pause if too much deliveries are waiting.
                deliveries.append(delivery)

                confirmation = asyncio.ensure_future(publish(message))
                confirmation.add_done_callback(
                    partial(on_confirmation, delivery))
                pending_confirmations.add(confirmation)
                recv_queue.task_done()

        async def send_receipts() -> None:
            while await has_confirmed_deliveries.wait():
                has_confirmed_deliveries.clear()
                receipt_id: Optional[str] = None
                while deliveries:
                    first = deliveries[0]
                    if not first.confirmed:
                        break
                    receipt_id = first.message_id
                    deliveries.popleft()

                if receipt_id is not None:
                    await send_queue.put(receipt_id)

        async def report_errors() -> None:
            await has_failed_confirmations.wait()
            assert failed_confirmation
            await failed_confirmation

        loop = asyncio.get_event_loop()
        publish_messages_task = loop.create_task(publish_messages())
        send_receipts_task = loop.create_task(send_receipts())
        report_errors_task = loop.create_task(report_errors())
        try:
            await asyncio.gather(
                publish_messages_task,
                send_receipts_task,
                report_errors_task,
            )
        except asyncio.CancelledError:
            pass
        finally:
            publish_messages_task.cancel()
            send_receipts_task.cancel()
            report_errors_task.cancel()
            for c in pending_confirmations:
                c.cancel()

    _logger.info('Disconnected from %s.', rmq_url)


# def _transform_message(m: Message) -> SmpMessage:
#     data = self.__marshmallow_schema__.dump(self)
#     message_type = data['type']
#     headers = {
#         'message-type': message_type,
#         'debtor-id': data['debtor_id'],
#         'creditor-id': data['creditor_id'],
#     }
#     if 'coordinator_id' in data:
#         headers['coordinator-id'] = data['coordinator_id']
#         headers['coordinator-type'] = data['coordinator_type']

#     body = json.dumps(
#         data,
#         ensure_ascii=False,
#         check_circular=False,
#         allow_nan=False,
#         separators=(',', ':'),
#     ).encode('utf8')
