import logging
import pytest
import asyncio
import aio_pika
from swpt_stomp.common import WatermarkQueue, Message, ServerError
from swpt_stomp.rmq import (
    open_robust_channel, consume_from_queue, publish_to_exchange, RmqMessage)


async def ready_queue(rmq_url: str) -> None:
    connection = await aio_pika.connect(rmq_url)
    channel = await connection.channel()
    async with connection, channel:
        queue = await channel.declare_queue('test_stomp')

        # Remove all existing messages from the queue.
        while await queue.get(no_ack=True, fail=False):
            pass

        # Add 100 messages to the queue.
        for i in range(1, 101):
            message = aio_pika.Message(
                str(i).encode('ascii'),
                type='TestMessage',
                content_type='text/plain',
            )
            await channel.default_exchange.publish(message, 'test_stomp')


@pytest.mark.asyncio
async def test_consume_from_queue(rmq_url):
    await ready_queue(rmq_url)
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
            url=rmq_url,
            queue_name='test_stomp',
        ))
    confirm_task = loop.create_task(confirm_sent_messages())

    await consume_task
    await confirm_task
    assert n == message_count


@pytest.mark.asyncio
async def test_publish_to_exchange(rmq_url):
    await ready_queue(rmq_url)
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
            url=rmq_url,
            exchange_name='',
            preprocess_message=preprocess_message,
        ))
    generate_task = loop.create_task(generate_messages())
    read_task = loop.create_task(read_receipts())

    await publish_task
    await generate_task
    await read_task
    assert last_receipt == message_count


@pytest.mark.asyncio
async def test_publish_returned_message(caplog, rmq_url):
    await ready_queue(rmq_url)
    caplog.set_level(logging.ERROR)
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

    connection, channel = await open_robust_channel(rmq_url, 10.0)
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

    await publish_task
    await generate_task
    await channel.close()
    await connection.close()

    assert len(caplog.records) > 0
    assert "PublishError" in caplog.text
    assert "RabbitMQ connection error" in caplog.text

    m = await send_queue.get()
    assert isinstance(m, ServerError)
    assert m.error_message == 'Internal server error.'


@pytest.mark.asyncio
async def test_publish_server_error(rmq_url):
    await ready_queue(rmq_url)
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
            url=rmq_url,
            exchange_name='',
            preprocess_message=preprocess_message,
        ))

    await publish_task
    m = await send_queue.get()
    assert isinstance(m, ServerError)
    assert m.error_message == 'Test error'
