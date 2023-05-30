import logging
import asyncio
import aio_pika
from typing import Optional, Union, Callable
from swpt_stomp.common import Message, WatermarkQueue, DEFAULT_MAX_NETWORK_DELAY
from swpt_stomp.aio_protocols import ServerError

_logger = logging.getLogger(__name__)


async def consume_rabbitmq_queue(
        send_queue: asyncio.Queue[Union[Message, None, ServerError]],
        recv_queue: WatermarkQueue[Union[str, None]],
        *,
        rmq_url: str,
        queue_name: str,
        prefetch_size: int = 0,
        timeout: Optional[float] = DEFAULT_MAX_NETWORK_DELAY / 1000,
        transform_message_body: Callable[[bytes], bytearray] = bytearray,
) -> None:
    loop = asyncio.get_event_loop()
    connection = await aio_pika.connect(rmq_url, timeout=timeout)

    async with connection:
        channel = await connection.channel()

        await channel.set_qos(
            prefetch_count=max(send_queue.maxsize, 1),
            prefetch_size=prefetch_size,
            timeout=timeout,
        )

        async def consume_queue() -> None:
            queue = await channel.get_queue(queue_name, ensure=False)
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    message_type = message.type
                    if message_type is None:
                        raise RuntimeError('Message without a type.')

                    message_content_type = message.content_type
                    if message_content_type is None:
                        raise RuntimeError('Message without a content-type.')

                    delivery_tag = message.delivery_tag
                    if delivery_tag is None:
                        raise RuntimeError('Message without a delivery tag.')

                    body = transform_message_body(message.body)
                    await send_queue.put(
                        Message(
                            id=str(delivery_tag),
                            type=message_type,
                            body=body,
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
        finally:
            consume_queue_task.cancel()
            send_acks_task.cancel()
