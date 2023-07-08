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
import os.path
import asyncio
import ssl
import click
from swpt_stomp.loggers import configure_logging
from contextlib import contextmanager
from typing import Union, Callable, Awaitable, Optional
from functools import partial
from swpt_stomp.common import (
    WatermarkQueue, ServerError, Message, APP_SSL_HANDSHAKE_TIMEOUT,
    get_peer_serial_number, terminate_queue, set_event_loop_policy,
)
from swpt_stomp.rmq import publish_to_exchange, open_robust_channel, RmqMessage
from swpt_stomp.peer_data import (
    get_database_instance, NodeData, PeerData, NodeType,
)
from swpt_stomp.aio_protocols import StompServer
from swpt_stomp.process_messages import preprocess_message

APP_STOMP_SERVER_BACKLOG = int(
    os.environ.get('APP_STOMP_SERVER_BACKLOG', '100'))
APP_MAX_CONNECTIONS_PER_PEER = int(
    os.environ.get('APP_MAX_CONNECTIONS_PER_PEER', '10'))

_EXCHANGE_NAMES = {
    NodeType.AA: 'accounts_in',
    NodeType.CA: 'creditors_in',
    NodeType.DA: 'debtors_in'
}
_connection_counters: dict[str, int] = dict()
_logger = logging.getLogger(__name__)


async def serve(
        *,
        server_cert: str,
        server_key: str,
        server_port: int,
        server_queue_size: int,
        nodedata_url: str,
        protocol_broker_url: str,
        server_backlog: int = APP_STOMP_SERVER_BACKLOG,
        server_started_event: Optional[asyncio.Event] = None,
        ssl_handshake_timeout: float = APP_SSL_HANDSHAKE_TIMEOUT,
        max_connections_per_peer: int = APP_MAX_CONNECTIONS_PER_PEER,
        preprocess_message: Callable[
            [NodeData, PeerData, Message], Awaitable[RmqMessage]
        ] = preprocess_message,
):
    loop = asyncio.get_running_loop()
    db = get_database_instance(url=nodedata_url)
    owner_node_data = await db.get_node_data()
    exchange_name = _EXCHANGE_NAMES[owner_node_data.node_type]
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
                        exchange_name=exchange_name,
                        preprocess_message=partial(
                            preprocess_message, owner_node_data, peer_data),
                        channel=channel,
                    )

        return StompServer(
            send_queue,
            recv_queue,
            recv_destination=f'/exchange/{exchange_name}',
            start_message_processor=lambda t: loop.create_task(publish(t)),
        )

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
    ssl_context.load_verify_locations(
        cadata=owner_node_data.root_cert.decode('ascii'))
    ssl_context.load_cert_chain(
        certfile=os.path.normpath(server_cert),
        keyfile=os.path.normpath(server_key),
    )
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


@click.command()
@click.option(
    '-p', '--server-port',
    type=int,
    envvar='SWPT_SERVER_PORT',
    default=1234,
    show_envvar=True,
    show_default=True,
    help="TCP port to accept connections on.")
@click.option(
    '-c', '--server-cert',
    envvar='SWPT_SERVER_CERT',
    default='/etc/swpt/server.crt',
    show_envvar=True,
    show_default=True,
    help="A path to a server certificate PEM file. The certificate will be "
         "used to authenticate before STOMP clients.")
@click.option(
    '-k', '--server-key',
    envvar='SWPT_SERVER_KEY',
    default='/secrets/swpt-server.key',
    show_envvar=True,
    show_default=True,
    help="A path to a PEM file containing an unencrypted private key. The "
         "key will be used to authenticate before STOMP clients.")
@click.option(
    '-n', '--nodedata-url',
    envvar='SWPT_NODEDATA_URL',
    default='file:///var/lib/swpt-nodedata',
    show_envvar=True,
    show_default=True,
    help="URL of the database that contains current node's data, including "
         "information about peer nodes.")
@click.option(
    '-u', '--broker-url',
    envvar='PROTOCOL_BROKER_URL',
    default='amqp://guest:guest@localhost:5672',
    show_envvar=True,
    show_default=True,
    help="URL of the RabbitMQ broker to connect to.")
@click.option(
    '-b', '--server-buffer',
    type=int,
    envvar='SWPT_SERVER_BUFFER',
    default=100,
    show_envvar=True,
    show_default=True,
    help="Maximum number of messages to store in memory.")
@click.option(
    '-l', '--log-level',
    type=click.Choice(['error', 'warning', 'info', 'debug']),
    envvar='APP_LOG_LEVEL',
    default='info',
    show_envvar=True,
    show_default=True,
    help="Application log level.")
@click.option(
    '-f', '--log-format',
    type=click.Choice(['text', 'json']),
    envvar='APP_LOG_FORMAT',
    default='text',
    show_envvar=True,
    show_default=True,
    help="Application log format.")
def server(
        server_port: int,
        server_cert: str,
        server_key: str,
        nodedata_url: str,
        broker_url: str,
        server_buffer: int,
        log_level: str,
        log_format: str,
):
    """Starts a STOMP server for a Swaptacular node.

    EXCHANGE_NAME: The name of the RabbitMQ exchange to publish messages to.
    """
    set_event_loop_policy()
    configure_logging(level=log_level, format=log_format)
    asyncio.run(serve(
        server_cert=server_cert,
        server_key=server_key,
        server_port=server_port,
        nodedata_url=nodedata_url,
        server_queue_size=server_buffer,
        protocol_broker_url=broker_url,
    ))


if __name__ == '__main__':  # pragma: nocover
    server()
