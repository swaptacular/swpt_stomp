import pytest
import asyncio
from aio_pika.exceptions import PublishError
from swpt_stomp.common import WatermarkQueue, Message, ServerError
from swpt_stomp.rmq import (
    open_robust_channel, consume_from_queue, publish_to_exchange, RmqMessage)


@pytest.mark.skip('Requires external STOMP server.')
@pytest.mark.asyncio
async def test_consume_from_queue():
    loop = asyncio.get_running_loop()
    send_queue = asyncio.Queue(5)
    recv_queue = WatermarkQueue(5)
    message_count = 100
    n = 0

    async def confirm_sent_messages():
        nonlocal n
        while n < message_count:
            message = await send_queue.get()
            await recv_queue.put(message.id)
            n += 1
            send_queue.task_done()
        await recv_queue.put(None)

    consume_task = loop.create_task(
        consume_from_queue(
            send_queue,
            recv_queue,
            url='amqp://guest:guest@127.0.0.1/',
            queue_name='test_stomp',
        ))
    confirm_task = loop.create_task(confirm_sent_messages())

    await consume_task
    await confirm_task
    assert n == message_count


@pytest.mark.skip('Requires external STOMP server.')
@pytest.mark.asyncio
async def test_publish_to_exchange():
    loop = asyncio.get_running_loop()
    send_queue = asyncio.Queue(5)
    recv_queue = WatermarkQueue(5)
    message_count = 100
    last_receipt = 0

    async def generate_messages():
        for n in range(1, message_count + 1):
            message = Message(
                id=str(n),
                type='TestMessage',
                body=bytearray(b'Message %i' % n),
                content_type='text/plain',
            )
            await recv_queue.put(message)
        await recv_queue.put(None)

    async def read_receipts():
        nonlocal last_receipt
        while receipt_id := await send_queue.get():
            n = int(receipt_id)
            assert last_receipt < n
            last_receipt = n
            send_queue.task_done()

    async def preprocess_message(m):
        return RmqMessage(
            body=m.body,
            headers={
                'message-type': m.type,
                'debtor-id': 1,
                'creditor-id': 2,
                'coordinator-id': 3,
            },
            type=m.type,
            content_type=m.content_type,
            routing_key='test_stomp',
        )

    publish_task = loop.create_task(
        publish_to_exchange(
            send_queue,
            recv_queue,
            url='amqp://guest:guest@127.0.0.1/',
            exchange_name='',
            preprocess_message=preprocess_message,
        ))
    generate_task = loop.create_task(generate_messages())
    read_task = loop.create_task(read_receipts())

    await publish_task
    await generate_task
    await read_task
    assert last_receipt == message_count


# @pytest.mark.skip('Requires external STOMP server.')
@pytest.mark.asyncio
async def test_publish_returned_message():
    loop = asyncio.get_running_loop()
    send_queue = asyncio.Queue(5)
    recv_queue = WatermarkQueue(5)

    async def generate_messages():
        for n in range(1, 11):
            message = Message(
                id=str(n),
                type='TestMessage',
                body=bytearray(b'Message %i' % n),
                content_type='text/plain',
            )
            await recv_queue.put(message)
        await recv_queue.put(None)

    async def preprocess_message(m):
        return RmqMessage(
            body=m.body,
            headers={
                'message-type': m.type,
                'debtor-id': 1,
                'creditor-id': 2,
                'coordinator-id': 3,
            },
            type=m.type,
            content_type=m.content_type,
            routing_key='nonexisting_queue',
        )

    connection, channel = await open_robust_channel(
        'amqp://guest:guest@127.0.0.1/', 10.0)
    generate_task = loop.create_task(generate_messages())
    publish_task = loop.create_task(
        publish_to_exchange(
            send_queue,
            recv_queue,
            url='',
            exchange_name='',
            preprocess_message=preprocess_message,
            channel=channel,
        ))

    with pytest.raises(PublishError):
        await publish_task

    await generate_task
    await channel.close()
    await connection.close()


@pytest.mark.skip('Requires external STOMP server.')
@pytest.mark.asyncio
async def test_publish_server_error():
    loop = asyncio.get_running_loop()
    send_queue = asyncio.Queue(5)
    recv_queue = WatermarkQueue(5)

    message = Message(
        id='1',
        type='TestMessage',
        body=bytearray(b'Erroneous message'),
        content_type='text/plain',
    )
    await recv_queue.put(message)
    await recv_queue.put(None)

    async def preprocess_message(m):
        raise ServerError('Test error')

    publish_task = loop.create_task(
        publish_to_exchange(
            send_queue,
            recv_queue,
            url='amqp://guest:guest@127.0.0.1/',
            exchange_name='',
            preprocess_message=preprocess_message,
        ))

    await publish_task
    m = await send_queue.get()
    assert isinstance(m, ServerError)
    assert m.error_message == 'Test error'
