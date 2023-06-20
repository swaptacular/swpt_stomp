import pytest
import asyncio
import os.path
import aio_pika
from contextlib import suppress
from swpt_stomp import server, client


def test_server_allowed_peer_connection():
    assert len(server._connection_counters) == 0
    with server._allowed_peer_connection('test'):
        assert len(server._connection_counters) == 1
        assert server._connection_counters['test'] == 1
        with server._allowed_peer_connection('test'):
            assert len(server._connection_counters) == 1
            assert server._connection_counters['test'] == 2
        assert len(server._connection_counters) == 1
        assert server._connection_counters['test'] == 1
    assert len(server._connection_counters) == 0


@pytest.mark.skip('Requires external RabbitMQ server.')
@pytest.mark.asyncio
async def test_connect_to_server(datadir, rmq_url):
    loop = asyncio.get_running_loop()

    # Ensure client and server queues are configured.
    connection = await aio_pika.connect(rmq_url)
    channel = await connection.channel()
    client_queue = await channel.declare_queue('test_client')
    server_exchange = await channel.declare_exchange('smp')
    server_queue = await channel.declare_queue('test_server')
    await server_queue.bind(server_exchange, '')

    # Empty client and server queues.
    while await client_queue.get(no_ack=True, fail=False):
        pass
    while await server_queue.get(no_ack=True, fail=False):
        pass

    # Add 100 messages to the client queue.
    for i in range(1, 101):
        message = aio_pika.Message(
            str(i).encode('ascii'),
            type='TestMessage',
            content_type='text/plain',
        )
        await channel.default_exchange.publish(message, 'test_client')

    # Start a server.
    server_started = asyncio.Event()
    server_task = loop.create_task(server.serve(
        protocol_broker_url=rmq_url,
        preprocess_message=server.NO_PPM,
        server_cert=os.path.abspath(f'{datadir["AA"]}/server.crt'),
        server_key=os.path.abspath(f'{datadir["AA"]}/server.key'),
        nodedata_url=f'file://{datadir["AA"]}',
        server_started_event=server_started,
    ))
    await server_started.wait()

    # Connect to the server.
    client_task = loop.create_task(client.connect(
        protocol_broker_url=rmq_url,
        transform_message_body=client.NO_TMP,
        peer_node_id='1234abcd',
        server_cert=os.path.abspath(f'{datadir["CA"]}/server.crt'),
        server_key=os.path.abspath(f'{datadir["CA"]}/server.key'),
        nodedata_url=f'file://{datadir["CA"]}',
        protocol_broker_queue='test_client',
    ))

    async def read_messages():
        nonlocal messages_ok
        i = 0
        async with server_queue.iterator() as q:
            async for message in q:
                i += 1
                assert message.body == str(i).encode('ascii')
                assert message.type == 'TestMessage'
                assert message.content_type == 'text/plain'
                await message.ack()
                if i == 100:
                    break
        messages_ok = True
        client_task.cancel()
        server_task.cancel()

    messages_ok = False
    read_task = loop.create_task(read_messages())
    with suppress(asyncio.CancelledError):
        await asyncio.gather(server_task, client_task, read_task)

    assert messages_ok
    await channel.close()
    await connection.close()
    await asyncio.sleep(0.1)
