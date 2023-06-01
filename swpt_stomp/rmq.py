import logging
import asyncio
import aio_pika
from aio_pika.exceptions import CONNECTION_EXCEPTIONS
from typing import Optional, Union, Callable
from swpt_stomp.common import Message, WatermarkQueue, DEFAULT_MAX_NETWORK_DELAY
from swpt_stomp.aio_protocols import ServerError

_logger = logging.getLogger(__name__)
_RMQ_CONNECTION_ERRORS = CONNECTION_EXCEPTIONS + (asyncio.TimeoutError,)
_RMQ_RECONNECT_ATTEMPT_SECONDS = 10.0


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
    loop = asyncio.get_event_loop()
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
