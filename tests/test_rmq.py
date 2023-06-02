import pytest
import asyncio
from contextlib import suppress
from swpt_stomp.common import WatermarkQueue
from swpt_stomp.rmq import consume_from_queue


@pytest.mark.skip('Requires external STOMP server.')
@pytest.mark.asyncio
async def test_consume_from_queue():
    loop = asyncio.get_running_loop()
    send_queue = asyncio.Queue(5)
    recv_queue = WatermarkQueue(5)

    async def confirm_sent_messages():
        n = 0
        while n < 20:
            message = await send_queue.get()
            await recv_queue.put(message.id)
            n += 1
            send_queue.task_done()
        await recv_queue.put(None)

    consume_task = loop.create_task(
        consume_from_queue(
            send_queue,
            recv_queue,
            rmq_url='amqp://guest:guest@127.0.0.1/',
            queue_name='test_stomp',
        ))
    confirm_task = loop.create_task(confirm_sent_messages())

    with suppress(asyncio.CancelledError):
        await asyncio.gather(consume_task, confirm_task)
