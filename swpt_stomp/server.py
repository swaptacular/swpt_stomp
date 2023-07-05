##############################################################################
# Implements a STOMP server that reads messages from a STOMP client, and
# publishes them to a RabbitMQ exchange.
#
# Here is how the different parts fit together:
#
#                             recv_queue
#   messages  /------------\  (messages)  /----------\  messages   /-------\
#  ---------->|            |------------->|          |------------>|       |
#    STOMP    |StompServer |              |publisher |             |Rabbit |
#    over     |asyncio     |              |asyncio   |  AMQP 0.9.1 |MQ     |
#    SSL      |protocol    |              |task      |             |Server |
#  <----------|            |<-------------|          |<------------|       |
#  msg. acks  \------------/  send_queue  \----------/  AMQP       \-------/
#                   |         (msg. acks)       |       publisher
#                   |                           |       confirms
#                   |                           |
#                   V                           V
#                /---------------------------------\
#                |       Node Peers Database       |
#                \---------------------------------/
#
# There are two important message processing parts: the "StompServer asyncio
# protocol" instance, and the "publisher asyncio task". They talk to each
# other via two asyncio queues: "recv_queue" and "send_queue". Putting a
# `None` or a `ServerError in the "send_queue" signals the end of the
# connection. In the other direction, putting `None` in the "recv_queue"
# cancels the publisher task immediately.
#
# The "Node Peers Database" contains information about the peers of the
# given node. The "StompServer" uses this information during the SSL
# authentication, and the "publisher" uses it to transform and add necessary
# data to the AMQP messages.
##############################################################################

import logging
import os
import asyncio
import ssl
from contextlib import contextmanager
from typing import Union, Callable, Awaitable, Optional
from functools import partial
from swpt_stomp.common import (
    WatermarkQueue, ServerError, Message, SSL_HANDSHAKE_TIMEOUT,
    STOMP_SERVER_KEY, STOMP_SERVER_CERT, NODEDATA_URL, PROTOCOL_BROKER_URL,
    get_peer_serial_number, terminate_queue,
)
from swpt_stomp.rmq import publish_to_exchange, open_robust_channel, RmqMessage
from swpt_stomp.peer_data import get_database_instance, NodeData, PeerData
from swpt_stomp.aio_protocols import StompServer
from swpt_stomp.process_messages import preprocess_message

STOMP_SERVER_PORT = int(os.environ.get('STOMP_SERVER_PORT', '1234'))
STOMP_SERVER_BACKLOG = int(os.environ.get('STOMP_SERVER_BACKLOG', '100'))
STOMP_SERVER_QUEUE_SIZE = int(os.environ.get('STOMP_SERVER_QUEUE_SIZE', '100'))
MAX_CONNECTIONS_PER_PEER = int(
    os.environ.get('MAX_CONNECTIONS_PER_PEER', '10'))
_connection_counters: dict[str, int] = dict()
_logger = logging.getLogger(__name__)


async def serve(
        *,
        preprocess_message: Callable[
            [NodeData, PeerData, Message], Awaitable[RmqMessage]
        ] = preprocess_message,
        server_cert: str = STOMP_SERVER_CERT,
        server_key: str = STOMP_SERVER_KEY,
        server_port: int = STOMP_SERVER_PORT,
        server_backlog: int = STOMP_SERVER_BACKLOG,
        nodedata_url: str = NODEDATA_URL,
        protocol_broker_url: str = PROTOCOL_BROKER_URL,
        ssl_handshake_timeout: float = SSL_HANDSHAKE_TIMEOUT,
        max_connections_per_peer: int = MAX_CONNECTIONS_PER_PEER,
        server_queue_size: int = STOMP_SERVER_QUEUE_SIZE,
        server_started_event: Optional[asyncio.Event] = None,
):
    loop = asyncio.get_running_loop()
    db = get_database_instance(url=nodedata_url)
    owner_node_data = await db.get_node_data()
    connection, channel = await open_robust_channel(protocol_broker_url)

    def create_protocol() -> StompServer:
        send_queue: asyncio.Queue[Union[str, None, ServerError]] = (
            asyncio.Queue(server_queue_size))
        recv_queue: WatermarkQueue[Union[Message, None]] = (
            WatermarkQueue(server_queue_size))

        async def publish(transport: asyncio.Transport) -> None:
            try:
                owner_node_data = await db.get_node_data()
                peer_serial_number = get_peer_serial_number(transport)
                if peer_serial_number is None:  # pragma: nocover
                    raise ServerError('Invalid certificate subject.')
                peer_data = await db.get_peer_data(peer_serial_number)
                if peer_data is None:  # pragma: nocover
                    raise ServerError('Unknown peer serial number.')
                n = _connection_counters.get(peer_data.node_id, 0)
                if n >= max_connections_per_peer:  # pragma: nocover
                    raise ServerError('Too many connections from one peer.')
            except ServerError as e:  # pragma: nocover
                terminate_queue(send_queue, e)
            except (asyncio.CancelledError, Exception):  # pragma: nocover
                terminate_queue(send_queue, ServerError(
                    'Internal server error.'))
                raise
            else:
                with _allowed_peer_connection(peer_data.node_id):
                    await publish_to_exchange(
                        send_queue,
                        recv_queue,
                        url=protocol_broker_url,
                        exchange_name='smp',
                        preprocess_message=partial(
                            preprocess_message, owner_node_data, peer_data),
                        channel=channel,
                    )

        return StompServer(
            send_queue,
            recv_queue,
            start_message_processor=lambda t: loop.create_task(publish(t)),
        )

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
    ssl_context.load_verify_locations(
        cadata=owner_node_data.root_cert.decode('ascii'))
    ssl_context.load_cert_chain(certfile=server_cert, keyfile=server_key)

    async with connection, channel:
        server = await loop.create_server(
            create_protocol,
            port=server_port,
            backlog=server_backlog,
            ssl=ssl_context,
            ssl_handshake_timeout=ssl_handshake_timeout,
        )
        if server_started_event:
            server_started_event.set()
        async with server:
            _logger.info('Started STOMP server at port %i.', server_port)
            await server.serve_forever()


@contextmanager
def _allowed_peer_connection(node_id: str):
    _connection_counters[node_id] = _connection_counters.get(node_id, 0) + 1
    try:
        yield
    finally:
        n = _connection_counters.get(node_id, 0) - 1
        if n <= 0:
            _connection_counters.pop(node_id, None)
        else:
            _connection_counters[node_id] = n


if __name__ == '__main__':  # pragma: nocover
    from swpt_stomp.loggers import configure_logging

    configure_logging()
    asyncio.run(serve(), debug=True)
