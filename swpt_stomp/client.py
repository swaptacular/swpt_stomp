##############################################################################
# Implements a STOMP client that consumes messages from a RabbitMQ queue,
# and sends them to a STOMP server.
#
# Here is how the different parts fit together:
#
#                             send_queue
#   messages  /------------\  (messages)  /----------\  messages   /-------\
#  <----------|            |<-------------|          |<------------|       |
#    STOMP    |StompClient |              |consumer  |             |Rabbit |
#    over     |asyncio     |              |asyncio   |  AMQP 0.9.1 |MQ     |
#    SSL      |protocol    |              |task      |             |Server |
#  ---------->|            |------------->|          |------------>|       |
#  msg. acks  \------------/  recv_queue  \----------/  AMQP       \-------/
#                   |         (msg. acks)       |       acks
#                   |                           |
#                   |                           |
#                   V                           V
#                /---------------------------------\
#                |       Node Peers Database       |
#                \---------------------------------/
#
# There are two important message processing parts: the "StompClient asyncio
# protocol" instance, and the "consumer asyncio task". They talk to each
# other via two asyncio queues: "send_queue" and "recv_queue". Putting a
# `None` or a `ServerError in the "send_queue" signals the end of the
# connection. In the other direction, putting `None` in the "recv_queue"
# ends the connection immediately.
#
# The "Node Peers Database" contains information about the peers of the
# given node. The "StompClient" uses this information during the SSL
# authentication, and the "consumer" uses it to transform the message before
# sending it to the server.
##############################################################################

import logging
import tempfile
import os
import asyncio
import ssl
import random
from typing import Union, Callable
from functools import partial
from swpt_stomp.common import (
    WatermarkQueue, ServerError, Message, SSL_HANDSHAKE_TIMEOUT,
    SERVER_KEY, SERVER_CERT, NODEDATA_URL, PROTOCOL_BROKER_URL,
    get_peer_serial_number, terminate_queue,
)
from swpt_stomp.rmq import consume_from_queue
from swpt_stomp.peer_data import get_database_instance, NodeData, PeerData
from swpt_stomp.aio_protocols import StompClient

PROTOCOL_BROKER_QUEUE = os.environ.get('PROTOCOL_BROKER_QUEUE', 'default')
PEER_NODE_ID = os.environ.get('PEER_NODE_ID', '00000000')
CLIENT_QUEUE_SIZE = int(os.environ.get('CLIENT_QUEUE_SIZE', '100'))
_logger = logging.getLogger(__name__)


def NO_TMP(n: NodeData, p: PeerData, body: bytes) -> bytearray:
    """This is mainly useful for testing purposes.
    """
    return bytearray(body)


async def connect(
        *,
        # TODO: change the default to the real message body transformer.
        transform_message_body: Callable[
            [NodeData, PeerData, bytes], bytearray] = NO_TMP,
        peer_node_id: str = PEER_NODE_ID,
        server_cert: str = SERVER_CERT,
        server_key: str = SERVER_KEY,
        nodedata_url: str = NODEDATA_URL,
        protocol_broker_url: str = PROTOCOL_BROKER_URL,
        protocol_broker_queue: str = PROTOCOL_BROKER_QUEUE,
        ssl_handshake_timeout: float = SSL_HANDSHAKE_TIMEOUT,
        client_queue_size: int = CLIENT_QUEUE_SIZE,
):
    loop = asyncio.get_running_loop()
    db = get_database_instance(url=nodedata_url)
    owner_node_data = await db.get_node_data()
    peer_data = await db.get_peer_data(peer_node_id)
    if peer_data is None:  # pragma: nocover
        raise RuntimeError(f'Peer {peer_node_id} is not in the database.')

    def create_protocol() -> StompClient:
        assert peer_data
        send_queue: asyncio.Queue[Union[Message, None, ServerError]] = (
            asyncio.Queue(client_queue_size))
        recv_queue: WatermarkQueue[Union[str, None]] = (
            WatermarkQueue(client_queue_size))

        async def consume(transport: asyncio.Transport) -> None:
            assert peer_data
            try:
                peer_serial_number = get_peer_serial_number(transport)
                if peer_serial_number != peer_data.node_id:  # pragma: nocover
                    raise ServerError('Invalid certificate subject.')
            except ServerError as e:  # pragma: nocover
                terminate_queue(send_queue, e)
            except (asyncio.CancelledError, Exception):  # pragma: nocover
                terminate_queue(send_queue, ServerError(
                    'Abruptly closed connection.'))
                raise
            else:
                await consume_from_queue(
                    send_queue,
                    recv_queue,
                    url=protocol_broker_url,
                    queue_name=protocol_broker_queue,
                    transform_message_body=partial(
                        transform_message_body, owner_node_data, peer_data),
                )

        return StompClient(
            send_queue,
            recv_queue,
            start_message_processor=lambda t: loop.create_task(consume(t)),
            host=peer_data.stomp_host,
            send_destination=peer_data.stomp_destination,
        )

    # To be correctly authenticated by the server, we must present both the
    # server certificate, and the sub-CA certificate issued by the peer's
    # root CA. Here we create a temporary file containing both certificates.
    # Note that this is a blocking operation, but this is OK, because we
    # will open no more than one client connection per process.
    with tempfile.NamedTemporaryFile() as certfile:
        with open(server_cert, 'br') as f:
            certfile.write(f.read())

        certfile.write(b'\n')
        certfile.write(peer_data.sub_cert)
        certfile.flush()

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.check_hostname = False
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
        ssl_context.load_verify_locations(
            cadata=peer_data.root_cert.decode('ascii'))
        ssl_context.load_cert_chain(certfile=certfile.name, keyfile=server_key)

        server_host, server_port = random.choice(peer_data.servers)
        transport, protocol = await loop.create_connection(
            create_protocol,
            host=server_host,
            port=server_port,
            ssl=ssl_context,
            ssl_handshake_timeout=ssl_handshake_timeout,
        )
        await protocol.connection_lost_event.wait()


if __name__ == '__main__':  # pragma: nocover
    from swpt_stomp.logging import configure_logging

    configure_logging()
    asyncio.run(connect(), debug=True)
