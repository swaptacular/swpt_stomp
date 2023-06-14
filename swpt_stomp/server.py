import logging
import os
import asyncio
import ssl
from typing import Union
from functools import partial
from swpt_stomp.common import WatermarkQueue, ServerError, Message
from swpt_stomp.rmq import publish_to_exchange, RmqMessage
from swpt_stomp.peer_data import (
    get_database_instance, NodeData, PeerData, NodePeersDatabase)
from swpt_stomp.aio_protocols import StompServer

PROTOCOL_BROKER_URL = os.environ['PROTOCOL_BROKER_URL']
NODE_DATA_DIR = os.environ['NODE_DATA_DIR']
SERVER_CERT = os.environ['SERVER_CERT']
SERVER_KEY = os.environ['SERVER_KEY']
SERVER_PORT = int(os.environ.get('SERVER_PORT', '1234'))
SERVER_BACKLOG = int(os.environ.get('SERVER_BACKLOG', '100'))
SSL_HANDSHAKE_TIMEOUT = float(os.environ.get('SSL_HANDSHAKE_TIMEOUT', '5'))
_logger = logging.getLogger(__name__)


async def serve():
    loop = asyncio.get_running_loop()
    node_db = get_database_instance(url=NODE_DATA_DIR)
    owner_node_data = await node_db.get_node_data()
    owner_root_cert = owner_node_data.root_cert.decode('ascii')

    # Configure SSL context:
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
    ssl_context.load_verify_locations(cadata=owner_root_cert)
    ssl_context.load_cert_chain(certfile=SERVER_CERT, keyfile=SERVER_KEY)
    print(ssl_context.get_ca_certs())

    server = await loop.create_server(
        partial(_create_server_protocol, node_db),
        port=SERVER_PORT,
        backlog=SERVER_BACKLOG,
        ssl=ssl_context,
        ssl_handshake_timeout=SSL_HANDSHAKE_TIMEOUT,
    )
    async with server:
        await server.serve_forever()


def _create_server_protocol(node_db: NodePeersDatabase) -> StompServer:
    loop = asyncio.get_running_loop()

    # TODO: set proper queue sizes.
    send_queue: asyncio.Queue[Union[str, None, ServerError]] = asyncio.Queue()
    recv_queue: WatermarkQueue[Union[Message, None]] = WatermarkQueue(2)

    async def publish(transport: asyncio.Transport) -> None:
        try:
            owner_node_data = await node_db.get_node_data()
            peercert = transport.get_extra_info('peercert')
            peer_serial_number = _get_peer_serial_number(peercert)
            peer_data = node_db.get_peer_data(peer_serial_number)
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
            )

    return StompServer(
        send_queue,
        recv_queue,
        start_message_processor=lambda t: loop.create_task(publish(t)),
    )


def _get_peer_serial_number(peercert) -> str:
    subject = peercert['subject']
    for rdns in subject:  # traverse all relative distinguished names
        key, value = rdns[0]
        if key == 'serialNumber':
            return value

    raise ValueError('invalid certificate DN')


async def _preprocess_message(
        owner_node_data: NodeData,
        peer_data: PeerData,
        message: Message,
) -> RmqMessage:
    raise Exception


if __name__ == '__main__':
    asyncio.run(serve())
