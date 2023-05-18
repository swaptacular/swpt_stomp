import pytest
import asyncio
from unittest.mock import NonCallableMock, Mock
from swpt_stomp.common import WatermarkQueue, Message
from swpt_stomp.aio_protocols import StompClient


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
    assert not c._closed
    assert c._writer_task is None
    assert c._watchdog_task is None

    # Received "CONNECTED" from the server.
    c.data_received(b'CONNECTED\nversion:1.2\nheart-beat:500,8000\n\n\x00')
    transport.write.assert_not_called()
    transport.close.assert_not_called()
    assert c._connected
    assert not c._closed
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
    assert not c._closed

    # Get a receipt confirmation from the server.
    c.data_received(b'RECEIPT\nreceipt-id:m1\n\n\x00')
    transport.write.assert_not_called()
    transport.close.assert_not_called()
    assert output_queue.get_nowait() == 'm1'
    assert c._connected
    assert not c._closed

    # Receive a server error.
    c.data_received(b'ERROR\nmessage:test-error\n\n\x00')
    transport.write.assert_not_called()
    transport.close.assert_called_once()
    transport.close.reset_mock()
    assert c._connected
    assert c._closed

    # Receive data on a closed connection.
    c.data_received(b'XXX\n\n\x00')
    transport.write.assert_not_called()
    transport.close.assert_not_called()
    assert c._connected
    assert c._closed

    # Send message to a closed connection.
    c._send_message(m)
    transport.write.assert_not_called()
    transport.close.assert_not_called()
    assert c._connected
    assert c._closed

    # The connection has been lost.
    c.connection_lost(None)
    assert c._writer_task is None
    assert c._watchdog_task is None
    transport.write.assert_not_called()
    transport.close.assert_not_called()


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
    assert c._closed


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
    assert not c._closed
    assert c._hb_send == 0
    assert c._hb_recv == 0
    c.data_received(data)
    transport.write.assert_not_called()
    transport.close.assert_called_once()
    assert c._connected
    assert c._closed
