import logging
import os
import asyncio
import aio_pika
from aio_pika.exceptions import CONNECTION_EXCEPTIONS
from functools import partial
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional, Union, Callable, Awaitable, AsyncGenerator
from collections import deque
from aio_pika.abc import HeadersType
from swpt_stomp.common import (
    Message,
    ServerError,
    WatermarkQueue,
    DEFAULT_MAX_NETWORK_DELAY,
    terminate_queue,
)

_logger = logging.getLogger(__name__)
_PERSISTENT = aio_pika.DeliveryMode.PERSISTENT
_RMQ_CONNECTION_ERRORS = CONNECTION_EXCEPTIONS + (asyncio.TimeoutError,)


def _NO_TM(m: "RmqMessage") -> Message:
    return Message(
        id=m.id,
        type=m.type,
        body=bytearray(m.body),
        content_type=m.content_type,
    )


APP_RMQ_CONNECTION_TIMEOUT_SECONDS = float(
    os.environ.get(
        "APP_RMQ_CONNECTION_TIMEOUT_SECONDS",
        str(DEFAULT_MAX_NETWORK_DELAY / 1000),
    )
)
APP_RMQ_CONFIRMATION_TIMEOUT_SECONDS = float(
    os.environ.get(
        "APP_RMQ_CONFIRMATION_TIMEOUT_SECONDS",
        "20",
    )
)
AbstractConnection = aio_pika.abc.AbstractConnection
AbstractChannel = aio_pika.abc.AbstractChannel


class _Delivery:
    __slots__ = ("message_id", "confirmed")

    def __init__(self, message_id: str):
        self.message_id = message_id
        self.confirmed = False


@dataclass
class RmqMessage:
    id: str
    body: bytes
    headers: HeadersType
    type: str
    content_type: str
    routing_key: Optional[str]


async def consume_from_queue(
    send_queue: asyncio.Queue[Union[Message, None, ServerError]],
    recv_queue: WatermarkQueue[Union[str, None]],
    *,
    url: str,
    queue_name: str,
    transform_message: Callable[[RmqMessage], Message] = _NO_TM,
    connection_timeout: float = APP_RMQ_CONNECTION_TIMEOUT_SECONDS,
    prefetch_size: int = 0,
) -> None:
    """Consumes messages from a RabbitMQ queue.

    The consumed messages will be added to the `send_queue`, awaiting
    receipt confirmations for them to arrive on the `recv_queue`. The
    consumption of messages will stop only when a `None` is received on the
    `recv_queue` (lost connection). At the end, a `None` (no error), or a
    `ServerError` will be added to the `send_queue`.

    A new connection will be initiated to the RabbbitMQ server specified by
    `url`. If the connection to the RabbitMQ server has been lost for some
    reason, no attempts to reconnect will be made.

    `send_queue.maxsize` will determine the RabbitMQ queue's prefetch count.
    If passed, the `transform_message` function may change the message
    before adding it to the `send_queue`.
    """
    try:
        async with _open_channel(
            url,
            timeout=connection_timeout,
            prefetch_count=max(send_queue.maxsize, 1),
            prefetch_size=prefetch_size,
        ) as channel:
            await _consume_from_queue(
                send_queue,
                recv_queue,
                channel=channel,
                queue_name=queue_name,
                transform_message=transform_message,
            )
    except (asyncio.CancelledError, Exception) as e:  # pragma: nocover
        terminate_queue(send_queue, ServerError("Abruptly closed connection."))
        if not isinstance(e, _RMQ_CONNECTION_ERRORS):
            raise
        _logger.exception("RabbitMQ connection error")
    else:
        terminate_queue(send_queue, None)


async def publish_to_exchange(
    send_queue: asyncio.Queue[Union[str, None, ServerError]],
    recv_queue: WatermarkQueue[Union[Message, None]],
    *,
    url: str,
    exchange_name: str,
    preprocess_message: Callable[[Message], Awaitable[RmqMessage]],
    confirmation_timeout: float = APP_RMQ_CONFIRMATION_TIMEOUT_SECONDS,
    connection_timeout: float = APP_RMQ_CONNECTION_TIMEOUT_SECONDS,
    channel: Optional[AbstractChannel] = None,
) -> None:
    """Publishes messages to a RabbitMQ exchange.

    The messages from the `recv_queue`, will be published to the RabbitMQ
    exchange, and when publish confirmations are received for them, the
    corresponding confirmations will be added to the `send_queue`. The
    publishing of messages will stop only when a `None` is received on the
    `recv_queue` (lost connection). At the end, a `None` (no error), or a
    `ServerError` will be added to the `send_queue`.

    If an open `channel` is passed, it will be used to communicate with the
    RabbbitMQ server. If it is `None`, a new connection will be initiated to
    the RabbbitMQ server specified by `url`. If the connection to the
    RabbitMQ server has been lost for some reason, no attempts to reconnect
    will be made.

    `send_queue.maxsize` will determine how many messages are allowed to be
    published in "a batch", without receiving publish confirmations for
    them. The `preprocess_message` coroutine function may validate the
    message, change the message body, add message headers, or raise a
    `ServerError`. But most importantly, it generates a routing key, before
    publishing the message to the RabbitMQ exchange.
    """

    async def publish_messages(ch: AbstractChannel) -> None:
        await _publish_to_exchange(
            send_queue,
            recv_queue,
            channel=ch,
            exchange_name=exchange_name,
            preprocess_message=preprocess_message,
            confirmation_timeout=confirmation_timeout,
        )

    try:
        if channel is None:
            async with _open_channel(url, connection_timeout) as channel:
                await publish_messages(channel)
        else:
            await publish_messages(channel)
    except ServerError as e:
        terminate_queue(send_queue, e)
    except (asyncio.CancelledError, Exception) as e:
        terminate_queue(send_queue, ServerError("Internal server error."))
        if not isinstance(e, _RMQ_CONNECTION_ERRORS):  # pragma: nocover
            raise
        _logger.exception("RabbitMQ connection error")
    else:
        terminate_queue(send_queue, None)


async def open_robust_channel(
    url: str,
    timeout: float = APP_RMQ_CONNECTION_TIMEOUT_SECONDS,
) -> tuple[AbstractConnection, AbstractChannel]:
    """Returns a robust RabbitMQ connection, and a robust RabbitMQ channel
    suitable for publishing messages.

    If the connection to the RabbitMQ server has been lost for some reason,
    automatic attempts to reconnect will be made ad-infinitum. To the
    publisher, the robust channel appears to be perpetually connected to the
    RabbitMQ. Note, however, that the publish confirmations for some of the
    published messages may still be lost during an automatic reconnect.
    """
    _logger.info("Creating a robust connection to %s.", url)
    connection = await aio_pika.connect_robust(url, timeout=timeout)
    channel = await asyncio.wait_for(
        connection.channel(
            publisher_confirms=True,
            on_return_raises=True,
        ),
        timeout,
    )
    return connection, channel


@asynccontextmanager
async def _open_channel(
    url: str,
    timeout: float,
    *,
    prefetch_count: int = 0,
    prefetch_size: int = 0,
) -> AsyncGenerator[AbstractChannel, None]:
    _logger.info("Connecting to %s.", url)
    async with await aio_pika.connect(url, timeout=timeout) as connection:
        channel = await asyncio.wait_for(
            connection.channel(
                publisher_confirms=True,
                on_return_raises=True,
            ),
            timeout,
        )
        if prefetch_count != 0 or prefetch_size != 0:
            await channel.set_qos(
                prefetch_count=prefetch_count,
                prefetch_size=prefetch_size,
                timeout=timeout,
            )
        yield channel
    _logger.info("Disconnected from %s.", url)


async def _consume_from_queue(
    send_queue: asyncio.Queue[Union[Message, None, ServerError]],
    recv_queue: WatermarkQueue[Union[str, None]],
    *,
    channel: AbstractChannel,
    queue_name: str,
    transform_message: Callable[[RmqMessage], Message],
) -> None:
    async def consume_queue() -> None:
        queue = await channel.get_queue(queue_name, ensure=False)
        async with queue.iterator() as queue_iter:
            _logger.info("Started consuming from %s.", queue_name)
            async for message in queue_iter:
                message_type = message.type
                if message_type is None:  # pragma: nocover
                    raise RuntimeError("Message without a type.")

                message_content_type = message.content_type
                if message_content_type is None:  # pragma: nocover
                    raise RuntimeError("Message without a content-type.")

                delivery_tag = message.delivery_tag
                if delivery_tag is None:  # pragma: nocover
                    raise RuntimeError("Message without a delivery tag.")

                m = transform_message(
                    RmqMessage(
                        id=str(delivery_tag),
                        body=message.body,
                        headers=message.headers,
                        type=message_type,
                        content_type=message_content_type,
                        routing_key=None,
                    )
                )
                await send_queue.put(m)

    async def send_acks() -> None:
        aiormq_channel = await channel.get_underlay_channel()
        while receipt_id := await recv_queue.get():
            try:
                delivery_tag = int(receipt_id)
            except ValueError:  # pragma: nocover
                _logger.error("Invalid receipt-id: %s", receipt_id)
            else:
                await aiormq_channel.basic_ack(delivery_tag, multiple=True)
            recv_queue.task_done()

        consume_queue_task.cancel()

    loop = asyncio.get_running_loop()
    consume_queue_task = loop.create_task(consume_queue())
    send_acks_task = loop.create_task(send_acks())
    tasks = [consume_queue_task, send_acks_task]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.wait(tasks)


async def _publish_to_exchange(
    send_queue: asyncio.Queue[Union[str, None, ServerError]],
    recv_queue: WatermarkQueue[Union[Message, None]],
    *,
    channel: AbstractChannel,
    exchange_name: str,
    preprocess_message: Callable[[Message], Awaitable[RmqMessage]],
    confirmation_timeout: float,
) -> None:
    exchange = await channel.get_exchange(exchange_name, ensure=False)
    deliveries: deque[_Delivery] = deque()
    max_parallel_deliveries = max(send_queue.maxsize, 1)
    can_make_deliveries = asyncio.Event()
    can_make_deliveries.set()
    pending_confirmations: set[asyncio.Future] = set()
    has_confirmed_deliveries = asyncio.Event()
    has_failed_confirmations = asyncio.Event()
    failed_confirmation: Optional[asyncio.Future] = None

    def on_confirmation(
        delivery: _Delivery,
        confirmation: asyncio.Future,
    ) -> None:
        pending_confirmations.remove(confirmation)
        failed = confirmation.cancelled() or confirmation.exception()
        if failed:
            nonlocal failed_confirmation
            if failed_confirmation is None:
                failed_confirmation = confirmation
                has_failed_confirmations.set()
        else:
            delivery.confirmed = True
            has_confirmed_deliveries.set()

    async def deliver_message(message: Message) -> None:
        m = await preprocess_message(message)
        await exchange.publish(
            aio_pika.Message(
                m.body,
                headers=m.headers,
                type=m.type,
                content_type=m.content_type,
                delivery_mode=_PERSISTENT,
                app_id="swpt_stomp",
            ),
            m.routing_key or "",
            mandatory=True,
            timeout=confirmation_timeout,
        )

    async def publish_messages() -> None:
        while message := await recv_queue.get():
            delivery = _Delivery(message.id)
            mark_as_confirmed = partial(on_confirmation, delivery)

            await can_make_deliveries.wait()
            deliveries.append(delivery)
            if len(deliveries) >= max_parallel_deliveries:
                can_make_deliveries.clear()

            confirmation = asyncio.ensure_future(deliver_message(message))
            confirmation.add_done_callback(mark_as_confirmed)
            pending_confirmations.add(confirmation)
            recv_queue.task_done()

        send_receipts_task.cancel()
        report_task.cancel()

    async def send_receipts() -> None:
        while True:
            receipt_id = None
            await has_confirmed_deliveries.wait()
            while deliveries:
                first = deliveries[0]
                if not first.confirmed:
                    break
                receipt_id = first.message_id
                deliveries.popleft()

            has_confirmed_deliveries.clear()
            if receipt_id is not None:
                if len(deliveries) < max_parallel_deliveries:
                    can_make_deliveries.set()
                await send_queue.put(receipt_id)

    async def report_failed_confirmations() -> None:
        await has_failed_confirmations.wait()
        assert failed_confirmation is not None
        await failed_confirmation

    loop = asyncio.get_running_loop()
    publish_messages_task = loop.create_task(publish_messages())
    send_receipts_task = loop.create_task(send_receipts())
    report_task = loop.create_task(report_failed_confirmations())
    tasks = [publish_messages_task, send_receipts_task, report_task]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        if pending_confirmations:  # pragma: nocover
            for c in pending_confirmations:
                c.cancel()
            await asyncio.wait(pending_confirmations)
        for t in tasks:
            t.cancel()
        await asyncio.wait(tasks)
