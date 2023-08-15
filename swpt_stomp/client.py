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
# cancels the consumer task immediately.
#
# The "Node Peers Database" contains information about the peers of the
# given node. The "StompClient" uses this information during the SSL
# authentication, and the "consumer" uses it to transform the message before
# sending it to the server.
##############################################################################

import logging
import sys
import tempfile
import asyncio
import ssl
import random
import click
import os.path
from swpt_stomp.loggers import configure_logging
from typing import Union, Callable
from functools import partial
from swpt_stomp.common import (
    WatermarkQueue,
    ServerError,
    Message,
    APP_SSL_HANDSHAKE_TIMEOUT,
    get_peer_serial_number,
    terminate_queue,
    set_event_loop_policy,
)
from swpt_stomp.rmq import consume_from_queue, RmqMessage
from swpt_stomp.peer_data import get_database_instance, NodeData, PeerData
from swpt_stomp.aio_protocols import StompClient
from swpt_stomp.process_messages import transform_message

_logger = logging.getLogger(__name__)


async def connect(
    *,
    peer_node_id: str,
    server_cert: str,
    server_key: str,
    nodedata_url: str,
    protocol_broker_url,
    protocol_broker_queue,
    client_queue_size: int,
    ssl_handshake_timeout: float = APP_SSL_HANDSHAKE_TIMEOUT,
    transform_message: Callable[
        [NodeData, PeerData, RmqMessage], Message
    ] = transform_message,
):
    loop = asyncio.get_running_loop()
    db = get_database_instance(url=nodedata_url)
    owner_node_data = await db.get_node_data()
    peer_data = await db.get_peer_data(peer_node_id)
    if peer_data is None:  # pragma: nocover
        raise RuntimeError(f"Peer {peer_node_id} is not in the database.")

    def create_protocol() -> StompClient:
        assert peer_data
        send_queue: asyncio.Queue[
            Union[Message, None, ServerError]
        ] = asyncio.Queue(client_queue_size)
        recv_queue: WatermarkQueue[Union[str, None]] = WatermarkQueue(
            client_queue_size
        )

        async def consume(transport: asyncio.Transport) -> None:
            assert peer_data
            try:
                peer_serial_number = get_peer_serial_number(transport)
                if peer_serial_number != peer_data.node_id:  # pragma: nocover
                    raise ServerError("Invalid certificate subject.")
            except ServerError as e:  # pragma: nocover
                terminate_queue(send_queue, e)
            except (asyncio.CancelledError, Exception):  # pragma: nocover
                terminate_queue(
                    send_queue, ServerError("Abruptly closed connection.")
                )
                raise
            else:
                await consume_from_queue(
                    send_queue,
                    recv_queue,
                    url=protocol_broker_url,
                    queue_name=protocol_broker_queue,
                    transform_message=partial(
                        transform_message, owner_node_data, peer_data
                    ),
                )

        c = peer_data.stomp_config
        return StompClient(
            send_queue,
            recv_queue,
            start_message_processor=lambda t: loop.create_task(consume(t)),
            host=c.host,
            login=c.login,
            passcode=c.passcode,
            send_destination=c.destination,
        )

    # To be correctly authenticated by the server, we must present both the
    # server certificate, and the sub-CA certificate issued by the peer's
    # root CA. Here we create a temporary file containing both certificates.
    # Note that this is a blocking operation, but this is OK, because we
    # will open no more than one client connection per process.
    with tempfile.NamedTemporaryFile() as certfile:
        with open(os.path.normpath(server_cert), "br") as f:
            certfile.write(f.read())

        certfile.write(b"\n")
        certfile.write(peer_data.sub_cert)
        certfile.flush()

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.check_hostname = False
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
        ssl_context.load_verify_locations(
            cadata=peer_data.root_cert.decode("ascii")
        )
        ssl_context.load_cert_chain(
            certfile=certfile.name, keyfile=os.path.normpath(server_key)
        )

        server_host, server_port = random.choice(
            peer_data.stomp_config.servers
        )
        transport, protocol = await loop.create_connection(
            create_protocol,
            host=server_host,
            port=server_port,
            ssl=ssl_context,
            ssl_handshake_timeout=ssl_handshake_timeout,
        )
        await protocol.connection_lost_event.wait()


@click.command()
@click.argument("peer_node_id")
@click.argument("queue_name")
@click.option(
    "-c",
    "--server-cert",
    envvar="SWPT_SERVER_CERT",
    default="/etc/swpt/server.crt",
    show_envvar=True,
    show_default=True,
    help=(
        "A path to a server certificate PEM file. The certificate will be "
        "used to authenticate before the peer's STOMP server."
    ),
)
@click.option(
    "-k",
    "--server-key",
    envvar="SWPT_SERVER_KEY",
    default="/secrets/swpt-server.key",
    show_envvar=True,
    show_default=True,
    help=(
        "A path to a PEM file containing an unencrypted private key. The "
        "key will be used to authenticate before the peer's STOMP server."
    ),
)
@click.option(
    "-n",
    "--nodedata-url",
    envvar="SWPT_NODEDATA_URL",
    default="file:///var/lib/swpt-nodedata",
    show_envvar=True,
    show_default=True,
    help=(
        "URL of the database that contains current node's data, including "
        "information about peer nodes."
    ),
)
@click.option(
    "-u",
    "--broker-url",
    envvar="PROTOCOL_BROKER_URL",
    default="amqp://guest:guest@localhost:5672",
    show_envvar=True,
    show_default=True,
    help="URL of the RabbitMQ broker to connect to.",
)
@click.option(
    "-b",
    "--client-buffer",
    type=int,
    envvar="SWPT_CLIENT_BUFFER",
    default=100,
    show_envvar=True,
    show_default=True,
    help="Maximum number of messages to store in memory.",
)
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(["error", "warning", "info", "debug"]),
    envvar="APP_LOG_LEVEL",
    default="info",
    show_envvar=True,
    show_default=True,
    help="Application log level.",
)
@click.option(
    "-f",
    "--log-format",
    type=click.Choice(["text", "json"]),
    envvar="APP_LOG_FORMAT",
    default="text",
    show_envvar=True,
    show_default=True,
    help="Application log format.",
)
def client(
    peer_node_id: str,
    queue_name: str,
    server_cert: str,
    server_key: str,
    nodedata_url: str,
    broker_url: str,
    client_buffer: int,
    log_level: str,
    log_format: str,
):
    """Initiates a long-lived client STOMP connection to a peer Swaptacular
    node.

    PEER_NODE_ID: The node ID of the peer Swaptacular node.

    QUEUE_NAME: The name of the RabbitMQ queue to consume messages from.
    """
    set_event_loop_policy()
    configure_logging(level=log_level, format=log_format)

    asyncio.run(
        connect(
            peer_node_id=peer_node_id,
            server_cert=server_cert,
            server_key=server_key,
            nodedata_url=nodedata_url,
            client_queue_size=client_buffer,
            protocol_broker_url=broker_url,
            protocol_broker_queue=queue_name,
        )
    )
    sys.exit(1)  # pragma: nocover


if __name__ == '__main__':  # pragma: nocover
    client()
