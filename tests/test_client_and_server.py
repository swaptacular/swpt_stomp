import pytest
import asyncio
import os.path
import aio_pika
import json
from typing import Optional
from contextlib import suppress
from swpt_stomp import server, client


def create_prepare_transfer_msg(
        debtor_id: int,
        creditor_id: int,
        coordinator_type: str = 'direct',
        coordinator_id: Optional[int] = None,
) -> str:
    if coordinator_id is None:
        coordinator_id = creditor_id

    props = f"""
      "type": "PrepareTransfer",
      "debtor_id": {debtor_id},
      "creditor_id": {creditor_id},
      "min_locked_amount": 1000,
      "max_locked_amount": 2000,
      "recipient": "RECIPIENT",
      "min_interest_rate": -10.0,
      "max_commit_delay": 100000,
      "coordinator_type": "{coordinator_type}",
      "coordinator_id": {coordinator_id},
      "coordinator_request_id": 1111,
      "ts": "2023-01-01T12:00:00+00:00"
    """
    return '{' + props + '}'


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


@pytest.mark.asyncio
async def test_connect_to_server(datadir, rmq_url):
    loop = asyncio.get_running_loop()

    # Ensure client and server queues are configured.
    connection = await aio_pika.connect(rmq_url)
    channel = await connection.channel()
    client_queue = await channel.declare_queue('test_client')
    server_exchange = await channel.declare_exchange('smp', 'topic')
    server_queue = await channel.declare_queue('test_server')
    await server_queue.bind(server_exchange, '#')

    # Empty client and server queues.
    while await client_queue.get(no_ack=True, fail=False):
        pass
    while await server_queue.get(no_ack=True, fail=False):
        pass

    # Add 100 messages to the client queue.
    for i in range(1, 101):
        s = create_prepare_transfer_msg(0x1234abcd00000001, 0x0000080000000001)
        message = aio_pika.Message(
            s.encode('utf8'),
            type='PrepareTransfer',
            content_type='application/json',
        )
        await channel.default_exchange.publish(message, 'test_client')

    # Start a server.
    server_started = asyncio.Event()
    server_task = loop.create_task(server.serve(
        protocol_broker_url=rmq_url,
        server_cert=os.path.abspath(f'{datadir["AA"]}/server.crt'),
        server_key=os.path.abspath(f'{datadir["AA"]}/server.key'),
        server_port=1234,
        server_queue_size=100,
        nodedata_url=f'file://{datadir["AA"]}',
        server_started_event=server_started,
    ))
    await server_started.wait()

    # Connect to the server.
    client_task = loop.create_task(client.connect(
        protocol_broker_url=rmq_url,
        peer_node_id='1234abcd',
        server_cert=os.path.abspath(f'{datadir["CA"]}/server.crt'),
        server_key=os.path.abspath(f'{datadir["CA"]}/server.key'),
        nodedata_url=f'file://{datadir["CA"]}',
        protocol_broker_queue='test_client',
        client_queue_size=100,
    ))

    async def read_messages():
        nonlocal messages_ok
        i = 0
        async with server_queue.iterator() as q:
            async for message in q:
                i += 1
                assert message.type == 'PrepareTransfer'
                assert message.content_type == 'application/json'
                assert json.loads(message.body.decode('utf8')) == \
                    json.loads(create_prepare_transfer_msg(
                        0x1234abcd00000001, 0x0000010000000001))
                assert message.headers == {
                    'message-type': 'PrepareTransfer',
                    'debtor-id': 0x1234abcd00000001,
                    'creditor-id': 0x0000010000000001,
                    'coordinator-type': 'direct',
                    'coordinator-id': 0x0000010000000001,
                }
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
    await asyncio.wait([server_task, client_task, read_task])
