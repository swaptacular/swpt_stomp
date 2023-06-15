import logging
import os
import asyncio
import ssl
from typing import Union, Callable, Awaitable
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
SERVER_QUEUE_SIZE = int(os.environ.get('SERVER_QUEUE_SIZE', '100'))
_logger = logging.getLogger(__name__)


async def NO_PPM(n: NodeData, p: PeerData, m: Message) -> RmqMessage:
    """This is mainly useful for testing purposes.
    """
    return RmqMessage(
        body=m.body,
        headers={},
        type=m.type,
        content_type=m.content_type,
        routing_key='',
    )


async def serve(
        *,
        # TODO: change the default to the real message preprocessor.
        preprocess_message: Callable[
            [NodeData, PeerData, Message], Awaitable[RmqMessage]] = NO_PPM,
        server_cert: str = SERVER_CERT,
        server_key: str = SERVER_KEY,
        server_port: int = SERVER_PORT,
        server_backlog: int = SERVER_BACKLOG,
        nodedata_dir: str = NODEDATA_DIR,
        protocol_broker_url: str = PROTOCOL_BROKER_URL,
        ssl_handshake_timeout: float = SSL_HANDSHAKE_TIMEOUT,
        server_queue_size: int = SERVER_QUEUE_SIZE,
):
    db = get_database_instance(url=nodedata_dir)
    owner_node_data = await db.get_node_data()

    # Configure SSL context:
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
    ssl_context.load_verify_locations(
        cadata=owner_node_data.root_cert.decode('ascii'))
    ssl_context.load_cert_chain(certfile=server_cert, keyfile=server_key)

    connection, channel = await open_robust_channel(
        'amqp://guest:guest@127.0.0.1/')
    async with connection, channel:
        loop = asyncio.get_running_loop()
        server = await loop.create_server(
            partial(
                _create_server_protocol,
                loop=loop,
                preprocess_message=preprocess_message,
                db=db,
                protocol_broker_url=protocol_broker_url,
                channel=channel,
                server_queue_size=server_queue_size,
            ),
            port=server_port,
            backlog=server_backlog,
            ssl=ssl_context,
            ssl_handshake_timeout=ssl_handshake_timeout,
        )
        async with server:
            _logger.info('Started STOMP server at port %i.', server_port)
            await server.serve_forever()


def _create_server_protocol(
        *,
        loop: asyncio.AbstractEventLoop,
        preprocess_message: Callable[
            [NodeData, PeerData, Message], Awaitable[RmqMessage]],
        db: NodePeersDatabase,
        protocol_broker_url: str,
        channel: AbstractChannel,
        server_queue_size: int,
) -> StompServer:
    send_queue: asyncio.Queue[Union[str, None, ServerError]] = (
        asyncio.Queue(server_queue_size))
    recv_queue: WatermarkQueue[Union[Message, None]] = (
        WatermarkQueue(server_queue_size))

    async def publish(transport: asyncio.Transport) -> None:
        try:
            owner_node_data = await db.get_node_data()
            peer_serial_number = get_peer_serial_number(transport)
            if peer_serial_number is None:
                raise ServerError('Invalid certificate subject.')
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


if __name__ == '__main__':
    configure_logging()
    asyncio.run(serve(), debug=True)
