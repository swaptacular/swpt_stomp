import pytest
import asyncio
from unittest.mock import NonCallableMock, Mock, patch
from swpt_stomp.common import WatermarkQueue, Message
from swpt_stomp.aio_protocols import ServerError, StompClient, StompServer


#######################
# `StompClient` tests #
#######################

def test_client_connection():
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(10)

    # create instance
    c = StompClient(
        input_queue,
        output_queue,
        hb_send_min=1000,
        hb_recv_desired=90,
        send_destination='dest',
    )
    assert c.input_queue is input_queue
    assert c.output_queue is output_queue

    # Make a connection to the server.
    transport = NonCallableMock(get_extra_info=Mock(return_value=('my',)))
    c.connection_made(transport)
    transport.write.assert_called_with(
        b'CONNECT\naccept-version:1.2\nhost:my\nheart-beat:1000,90\n\n\x00')
    transport.write.reset_mock()
    transport.close.assert_not_called()
    assert not c._connected
    assert not c._done
    assert c._writer_task is None
    assert c._watchdog_task is None

    # Received "CONNECTED" from the server.
    c.data_received(b'CONNECTED\nversion:1.2\nheart-beat:500,8000\n\n\x00')
    transport.write.assert_not_called()
    transport.close.assert_not_called()
    assert c._connected
    assert not c._done
    assert c._hb_send == 8000
    assert c._hb_recv == 500
    assert isinstance(c._writer_task, asyncio.Task)
    assert isinstance(c._watchdog_task, asyncio.Task)

    # Put a message in the input queue.
    m = Message(id='m1', content_type='text/plain', body=bytearray(b'1'))
    input_queue.put_nowait(m)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(input_queue.join())
    transport.write.assert_called_with(
        b'SEND\n'
        b'destination:dest\n'
        b'content-type:text/plain\n'
        b'receipt:m1\n'
        b'content-length:1\n'
        b'\n'
        b'1\x00')
    transport.write.reset_mock()
    transport.close.assert_not_called()
    assert c._connected
    assert not c._done

    # Get a receipt confirmation from the server.
    c.data_received(b'RECEIPT\nreceipt-id:m1\n\n\x00')
    transport.write.assert_not_called()
    transport.close.assert_not_called()
    assert output_queue.get_nowait() == 'm1'
    assert c._connected
    assert not c._done

    # Receive a server error.
    c.data_received(b'ERROR\nmessage:test-error\n\n\x00')
    transport.write.assert_not_called()
    transport.close.assert_called_once()
    transport.close.reset_mock()
    assert c._connected
    assert c._done

    # Receive data on a closed connection.
    c.data_received(b'XXX\n\n\x00')
    transport.write.assert_not_called()
    transport.close.assert_not_called()
    assert c._connected
    assert c._done

    # Send message to a closed connection.
    c._send_frame(m)
    transport.write.assert_not_called()
    transport.close.assert_not_called()
    assert c._connected
    assert c._done

    # The connection has been lost.
    c.connection_lost(None)
    assert output_queue.get_nowait() is None
    transport.write.assert_not_called()
    transport.close.assert_not_called()

    async def wait_for_cancelation():
        while not (c._writer_task.cancelled() and c._watchdog_task.cancelled()):
            await asyncio.sleep(0)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(wait_for_cancelation())


@pytest.mark.parametrize("data", [
    b'protocol error',
    b'INVALIDCMD\n\n\x00',
    b'CONNECTED\nversion:1.0\nheart-beat:500,8000\n\n\x00',
    b'CONNECTED\nversion:1.2\nheart-beat:invalid\n\n\x00',
    b'CONNECTED\nversion:1.2\nheart-beat:-10,0\n\n\x00',
    b'RECEIPT\nreceipt-id:m1\n\n\x00',
])
def test_client_connection_error(data):
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(10)
    transport = NonCallableMock(get_extra_info=Mock(return_value=None))
    c = StompClient(input_queue, output_queue)
    c.connection_made(transport)
    transport.write.reset_mock()
    c.data_received(data)
    transport.write.assert_not_called()
    transport.close.assert_called_once()
    assert not c._connected
    assert c._done

    c.connection_lost(None)
    assert output_queue.get_nowait() is None


@pytest.mark.parametrize("data", [
    b'protocol error',
    b'INVALIDCMD\n\n\x00',
    b'CONNECTED\nversion:1.2\nheart-beat:500,8000\n\n\x00',
    b'RECEIPT\n\n\x00',
])
def test_client_post_connection_error(data):
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(10)
    transport = NonCallableMock(get_extra_info=Mock(return_value=None))
    c = StompClient(input_queue, output_queue, hb_send_min=0, hb_recv_desired=0)
    c.connection_made(transport)
    transport.write.reset_mock()
    c.data_received(b'CONNECTED\nversion:1.2\n\n\x00')
    assert c._connected
    assert not c._done
    assert c._hb_send == 0
    assert c._hb_recv == 0
    c.data_received(data)
    transport.write.assert_not_called()
    transport.close.assert_called_once()
    assert c._connected
    assert c._done

    c.connection_lost(None)
    assert output_queue.get_nowait() is None


def test_client_pause_writing():
    loop = asyncio.get_event_loop()

    def run_once():
        loop.call_soon(loop.stop)
        loop.run_forever()

    # Make a proper connection.
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(10)
    transport = NonCallableMock(get_extra_info=Mock(return_value=None))
    c = StompClient(input_queue, output_queue)
    c.connection_made(transport)
    c.data_received(b'CONNECTED\nversion:1.2\n\n\x00')
    transport.write.reset_mock()

    # Pause writing and queue a message.
    c.pause_writing()
    m = Message(id='m1', content_type='text/plain', body=bytearray(b'1'))
    input_queue.put_nowait(m)
    run_once()
    run_once()
    run_once()
    transport.write.assert_not_called()

    # Resume writing.
    loop.call_soon(c.resume_writing)
    loop.run_until_complete(input_queue.join())
    transport.write.assert_called_once()

    c.connection_lost(None)
    assert output_queue.get_nowait() is None


def test_client_pause_reading():
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(2)
    transport = NonCallableMock(get_extra_info=Mock(return_value=None))
    c = StompClient(input_queue, output_queue)
    c.connection_made(transport)
    c.data_received(b'CONNECTED\nversion:1.2\n\n\x00')
    transport.pause_reading.assert_not_called()
    transport.resume_reading.reset_mock()

    # The first message do not cause a pause.
    c.data_received(b'RECEIPT\nreceipt-id:m1\n\n\x00')
    transport.pause_reading.assert_not_called()
    transport.resume_reading.assert_not_called()

    # The second message causes a pause.
    c.data_received(b'RECEIPT\nreceipt-id:m2\n\n\x00')
    transport.pause_reading.assert_called_once()
    transport.resume_reading.assert_not_called()

    # Removing both messages from the queue resumes reading.
    output_queue.get_nowait()
    output_queue.task_done()
    output_queue.get_nowait()
    output_queue.task_done()
    transport.pause_reading.assert_called_once()
    transport.resume_reading.assert_called_once()

    c.connection_lost(None)
    assert output_queue.get_nowait() is None


def test_client_send_heartbeats():
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(10)
    transport = NonCallableMock(get_extra_info=Mock(return_value=None))
    c = StompClient(input_queue, output_queue, hb_send_min=1)
    c.connection_made(transport)
    c.data_received(b'CONNECTED\nversion:1.2\nheart-beat:0,1\n\n\x00')
    transport.write.reset_mock()
    assert c._hb_send == 1

    async def wait_for_write():
        while not transport.write.called:
            await asyncio.sleep(0)

    transport.write.assert_not_called()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(wait_for_write())
    transport.write.assert_called_with(b'\n')

    c.connection_lost(None)
    assert output_queue.get_nowait() is None


@patch('swpt_stomp.aio_protocols.DEFAULT_HB_SEND_MIN', new=1)
def test_client_recv_heartbeats():
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(10)
    transport = NonCallableMock(get_extra_info=Mock(return_value=None))
    c = StompClient(
        input_queue, output_queue, hb_recv_desired=1, max_network_delay=1)
    c.connection_made(transport)
    c.data_received(b'CONNECTED\nversion:1.2\nheart-beat:1,0\n\n\x00')
    assert c._hb_recv == 1

    async def wait_disconnect():
        while not c._done:
            await asyncio.sleep(0)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(wait_disconnect())

    c.connection_lost(None)


def test_client_connected_timeout():
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(10)
    transport = NonCallableMock(get_extra_info=Mock(return_value=None))
    c = StompClient(input_queue, output_queue, max_network_delay=1)
    c.connection_made(transport)

    async def wait_disconnect():
        while not c._done:
            await asyncio.sleep(0)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(wait_disconnect())

    c.connection_lost(None)
    assert output_queue.get_nowait() is None


def test_client_graceful_disconnect_no_messages():
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(10)
    transport = NonCallableMock(get_extra_info=Mock(return_value=None))
    c = StompClient(input_queue, output_queue, hb_send_min=0, hb_recv_desired=0)
    c.connection_made(transport)
    transport.write.reset_mock()
    c.data_received(b'CONNECTED\nversion:1.2\n\n\x00')

    async def wait_disconnect():
        while not c._done:
            await asyncio.sleep(0)

    # The connection is closed immediately.
    input_queue.put_nowait(None)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(wait_disconnect())
    assert c._done

    c.connection_lost(None)
    assert output_queue.get_nowait() is None


def test_client_graceful_disconnect():
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(10)
    transport = NonCallableMock(get_extra_info=Mock(return_value=None))
    c = StompClient(input_queue, output_queue, hb_send_min=0, hb_recv_desired=0)
    c.connection_made(transport)
    transport.write.reset_mock()
    c.data_received(b'CONNECTED\nversion:1.2\n\n\x00')
    m = Message(id='m1', content_type='text/plain', body=bytearray(b'1'))
    input_queue.put_nowait(m)

    async def wait_disconnect():
        while not c._sent_disconnect:
            await asyncio.sleep(0)

    # The connection is NOT closed immediately.
    input_queue.put_nowait(None)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(wait_disconnect())
    assert not c._done

    # The connection is closed only after the "m1" receipt.
    c.data_received(b'RECEIPT\nreceipt-id:m0\n\n\x00')
    assert not c._done
    c.data_received(b'RECEIPT\nreceipt-id:m1\n\n\x00')
    assert c._done

    c.connection_lost(None)
    assert output_queue.get_nowait() == 'm0'
    assert output_queue.get_nowait() == 'm1'
    assert output_queue.get_nowait() is None


#######################
# `StompServer` tests #
#######################

@pytest.mark.parametrize("cmd", [b'CONNECT', b'STOMP'])
def test_server_connection(cmd):
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(10)

    # create instance
    c = StompServer(
        input_queue,
        output_queue,
        hb_send_min=1000,
        hb_recv_desired=90,
        recv_destination='dest',
    )
    assert c.input_queue is input_queue
    assert c.output_queue is output_queue

    # Receive a connection from the client.
    transport = NonCallableMock(is_closing=Mock(return_value=False))
    c.connection_made(transport)
    transport.write.assert_not_called()
    transport.close.assert_not_called()
    assert not c._connected
    assert not c._done
    assert c._writer_task is None
    assert c._watchdog_task is None

    # Received "CONNECT" or "STOMP" from the client.
    c.data_received(
        cmd + b'\naccept-version:1.1,1.2\nhost:my\nheart-beat:500,8000\n\n\x00')
    transport.write.assert_called_with(
        b'CONNECTED\nversion:1.2\nheart-beat:1000,90\n\n\x00')
    transport.write.reset_mock()
    transport.close.assert_not_called()
    assert c._connected
    assert not c._done
    assert c._hb_send == 8000
    assert c._hb_recv == 500
    assert isinstance(c._writer_task, asyncio.Task)
    assert isinstance(c._watchdog_task, asyncio.Task)

    # Receive a message.
    c.data_received(
        b'SEND\n'
        b'destination:dest\n'
        b'content-type:text/plain\n'
        b'receipt:m1\n'
        b'content-length:1\n'
        b'\n'
        b'1\x00'
    )
    m = output_queue.get_nowait()
    assert m.id == 'm1'
    assert m.content_type == 'text/plain'
    assert m.body == b'1'
    transport.write.assert_not_called()
    transport.close.assert_not_called()
    assert c._connected
    assert not c._done

    # Send a confirmation to the client.
    input_queue.put_nowait('m1')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(input_queue.join())
    transport.write.assert_called_with(b'RECEIPT\nreceipt-id:m1\n\n\x00')
    transport.write.reset_mock()
    transport.close.assert_not_called()
    assert c._connected
    assert not c._done

    # Receive a disconnect command.
    c.data_received(b'DISCONNECT\nreceipt:m1\n\n\x00')
    transport.write.assert_called_with(b'RECEIPT\nreceipt-id:m1\n\n\x00')
    transport.write.reset_mock()
    transport.close.assert_called_once()
    transport.close.reset_mock()
    assert c._connected
    assert c._done

    # Receive data on a closed connection.
    c.data_received(b'XXX\n\n\x00')
    transport.write.assert_not_called()
    transport.close.assert_not_called()
    assert output_queue.empty()
    assert c._connected
    assert c._done

    # Send message to a closed connection.
    c._send_frame('x')
    transport.write.assert_not_called()
    transport.close.assert_not_called()
    assert c._connected
    assert c._done

    # The connection has been lost.
    c.connection_lost(None)
    assert output_queue.get_nowait() is None
    transport.write.assert_not_called()
    transport.close.assert_not_called()

    async def wait_for_cancelation():
        while not (c._writer_task.cancelled() and c._watchdog_task.cancelled()):
            await asyncio.sleep(0)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(wait_for_cancelation())
    assert c._writer_task.cancelled()
    assert c._watchdog_task.cancelled()


@pytest.mark.parametrize("data", [
    b'protocol error',
    b'INVALIDCMD\n\n\x00',
    b'CONNECT\naccept-version:1.0\nheart-beat:500,8000\n\n\x00',
    b'CONNECT\naccept-version:1.0,1.1\nheart-beat:500,8000\n\n\x00',
    b'CONNECT\naccept-version:1.2\nheart-beat:invalid\n\n\x00',
    b'CONNECT\naccept-version:1.2\nheart-beat:-10,0\n\n\x00',
    b'DISCONNECT\nreceipt:m1\n\n\x00',
    b'SEND\ndestination:dest\nreceipt:m1\n\n\body\x00',
])
def test_server_connection_error(data):
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(10)
    transport = NonCallableMock(is_closing=Mock(return_value=False))
    c = StompServer(input_queue, output_queue)
    c.connection_made(transport)
    assert not c._connected
    assert not c._done

    c.data_received(data)
    transport.write.assert_called_once()
    assert b'ERROR' in transport.write.call_args[0][0]
    transport.close.assert_called_once()
    assert not c._connected
    assert c._done

    c.connection_lost(None)
    assert output_queue.get_nowait() is None


@pytest.mark.parametrize("data", [
    b'protocol error',
    b'INVALIDCMD\n\n\x00',
    b'CONNECT\naccept-version:1.2\nheart-beat:0,0\n\n\x00',
    b'SEND\ndestination:smp\n\nbody\x00',
    b'SEND\ndestination:xxx\nreceipt:m1\n\nbody\x00',
    b'DISCONNECT\n\n\x00',
])
def test_server_post_connection_error(data):
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(10)
    transport = NonCallableMock(is_closing=Mock(return_value=False))
    c = StompServer(input_queue, output_queue)
    c.connection_made(transport)
    c.data_received(b'CONNECT\naccept-version:1.2\nheart-beat:0,0\n\n\x00')
    transport.write.reset_mock()
    assert c._connected
    assert not c._done

    c.data_received(data)
    transport.write.assert_called_once()
    assert b'ERROR' in transport.write.call_args[0][0]
    transport.close.assert_called_once()
    assert c._connected
    assert c._done

    c.connection_lost(None)
    assert output_queue.get_nowait() is None


def test_server_command_after_disconnect():
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(10)
    transport = NonCallableMock(is_closing=Mock(return_value=False))
    c = StompServer(input_queue, output_queue)
    c.connection_made(transport)
    c.data_received(b'CONNECT\naccept-version:1.2\nheart-beat:0,0\n\n\x00')
    transport.write.reset_mock()
    c.data_received(b'SEND\ndestination:smp\nreceipt:m1\n\n\body\x00',)
    c.data_received(b'DISCONNECT\nreceipt:m1\n\n\x00')
    transport.write.assert_not_called()
    assert c._connected
    assert not c._done

    c.data_received(b'SEND\ndestination:smp\nreceipt:m2\n\n\body\x00',)
    transport.write.assert_called_once()
    assert (
        b'ERROR\nmessage:Received command after DISCONNECT.\n\n\x00'
        == transport.write.call_args[0][0]
    )
    transport.close.assert_called_once()
    assert c._connected
    assert c._done

    c.connection_lost(None)
    assert output_queue.get_nowait().id == 'm1'
    assert output_queue.get_nowait() is None


def test_server_close_gracefully():
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(10)
    transport = NonCallableMock(is_closing=Mock(return_value=False))
    c = StompServer(input_queue, output_queue)
    c.connection_made(transport)
    c.data_received(b'CONNECT\naccept-version:1.2\nheart-beat:0,0\n\n\x00')
    transport.write.reset_mock()
    assert c._connected
    assert not c._done

    input_queue.put_nowait(None)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(input_queue.join())
    transport.write.assert_called_once()
    assert (
        b'ERROR\nmessage:The connection has been closed by the server.\n\n\x00'
        == transport.write.call_args[0][0]
    )
    transport.close.assert_called_once()
    assert c._connected
    assert c._done

    c.connection_lost(None)
    assert output_queue.get_nowait() is None


def test_server_close_gracefully_with_error():
    input_queue = asyncio.Queue()
    output_queue = WatermarkQueue(10)
    transport = NonCallableMock(is_closing=Mock(return_value=False))
    c = StompServer(input_queue, output_queue)
    c.connection_made(transport)
    c.data_received(b'CONNECT\naccept-version:1.2\nheart-beat:0,0\n\n\x00')
    transport.write.reset_mock()
    assert c._connected
    assert not c._done

    input_queue.put_nowait(ServerError('err1', 'm1', b'c1'))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(input_queue.join())
    transport.write.assert_called_once()
    error_frame = transport.write.call_args[0][0]
    assert error_frame.startswith(b'ERROR\n')
    assert error_frame.endswith(b'\n\nc1\x00')
    assert b'message:err1\n' in error_frame
    assert b'receipt-id:m1\n' in error_frame
    transport.close.assert_called_once()
    assert c._connected
    assert c._done

    c.connection_lost(None)
    assert output_queue.get_nowait() is None
