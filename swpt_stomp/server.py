import logging
import os
import asyncio
import ssl
from typing import Union
from functools import partial
from swpt_stomp.logging import configure_logging
from swpt_stomp.common import (
    WatermarkQueue, ServerError, Message, SSL_HANDSHAKE_TIMEOUT,
    SERVER_KEY, SERVER_CERT, NODEDATA_DIR, PROTOCOL_BROKER_URL,
    get_peer_serial_number,
)
from swpt_stomp.rmq import (
    publish_to_exchange, open_robust_channel, RmqMessage, AbstractChannel,
)
from swpt_stomp.peer_data import (
    get_database_instance, NodeData, PeerData, NodePeersDatabase,
)
from swpt_stomp.aio_protocols import StompServer

SERVER_PORT = int(os.environ.get('SERVER_PORT', '1234'))
SERVER_BACKLOG = int(os.environ.get('SERVER_BACKLOG', '100'))
SERVER_SEND_QUEUE_SIZE = int(os.environ.get('SERVER_SEND_QUEUE_SIZE', '100'))
SERVER_RECV_QUEUE_SIZE = int(os.environ.get('SERVER_RECV_QUEUE_SIZE', '100'))
_logger = logging.getLogger(__name__)


async def serve():
    loop = asyncio.get_running_loop()
    db = get_database_instance(url=NODEDATA_DIR)
    owner_node_data = await db.get_node_data()
    owner_root_cert = owner_node_data.root_cert.decode('ascii')

    # Configure SSL context:
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
    ssl_context.load_verify_locations(cadata=owner_root_cert)
    ssl_context.load_cert_chain(certfile=SERVER_CERT, keyfile=SERVER_KEY)

    connection, channel = await open_robust_channel(
        'amqp://guest:guest@127.0.0.1/', 10.0)
    async with channel, connection:
        server = await loop.create_server(
            partial(_create_server_protocol, db, channel),
            port=SERVER_PORT,
            backlog=SERVER_BACKLOG,
            ssl=ssl_context,
            ssl_handshake_timeout=SSL_HANDSHAKE_TIMEOUT,
        )
        async with server:
            _logger.info('Started STOMP server at port %i.', SERVER_PORT)
            await server.serve_forever()


def _create_server_protocol(
        db: NodePeersDatabase,
        channel: AbstractChannel,
) -> StompServer:
    send_queue: asyncio.Queue[Union[str, None, ServerError]] = asyncio.Queue(
        SERVER_SEND_QUEUE_SIZE)
    recv_queue: WatermarkQueue[Union[Message, None]] = WatermarkQueue(
        SERVER_RECV_QUEUE_SIZE)

    async def publish(transport: asyncio.Transport) -> None:
        try:
            owner_node_data = await db.get_node_data()
            peer_serial_number = get_peer_serial_number(transport)
            peer_data = await db.get_peer_data(peer_serial_number)
            if peer_data is None:
                raise ServerError('Unknown peer serial number.')
        except ServerError as e:
            await send_queue.put(e)
        except (asyncio.CancelledError, Exception):
            await send_queue.put(ServerError('Internal server error.'))
            raise
        else:
            await publish_to_exchange(
                send_queue,
                recv_queue,
                url=PROTOCOL_BROKER_URL,
                exchange_name='smp',
                preprocess_message=partial(
                    _preprocess_message, owner_node_data, peer_data),
                channel=channel,
            )

    loop = asyncio.get_running_loop()
    return StompServer(
        send_queue,
        recv_queue,
        start_message_processor=lambda t: loop.create_task(publish(t)),
    )


async def _preprocess_message(
        owner_node_data: NodeData,
        peer_data: PeerData,
        message: Message,
) -> RmqMessage:
    raise Exception


if __name__ == '__main__':
    configure_logging()
    asyncio.run(serve(), debug=True)
