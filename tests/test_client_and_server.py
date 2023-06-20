import pytest
import asyncio
import os.path
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


# @pytest.mark.skip('Requires external RabbitMQ server.')
@pytest.mark.asyncio
async def test_connect_to_server(datadir):
    loop = asyncio.get_running_loop()
    server_started = asyncio.Event()
    server_task = loop.create_task(server.serve(
        preprocess_message=server.NO_PPM,
        server_cert=os.path.abspath(f'{datadir["AA"]}/server.crt'),
        server_key=os.path.abspath(f'{datadir["AA"]}/server.key'),
        nodedata_dir=f'{datadir["AA"]}',
        server_started_event=server_started,
    ))
    await server_started.wait()

    client_task = loop.create_task(client.connect(
        transform_message_body=client.NO_TMP,
        peer_node_id='1234abcd',
        server_cert=os.path.abspath(f'{datadir["CA"]}/server.crt'),
        server_key=os.path.abspath(f'{datadir["CA"]}/server.key'),
        nodedata_dir=f'{datadir["CA"]}',
        protocol_broker_queue='test_client',
    ))
    loop.call_later(5, server_task.cancel)

    with suppress(asyncio.CancelledError):
        await asyncio.gather(server_task, client_task)
