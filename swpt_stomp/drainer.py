##############################################################################
# Implements a drainer that consumes messages from existing queues
# associated with already deactivated Swaptacular peers.
#
# Here is how the different parts fit together:
#
#              messages_queue                  responses_queue
#  /-------\   (messages)       /----------\   (resp. messages)  /-------\
#  |       |------------------->|          |-------------------->|       |
#  |Rabbit |                    |drainer   |                     |Rabbit |
#  |MQ     |  AMQP 0.9.1        |asyncio   |  AMQP 0.9.1         |MQ     |
#  |Server |                    |task      |                     |Server |
#  |       |<-------------------|          |<--------------------|       |
#  \-------/   acks_queue       \----------/   confirms_queue    \-------/
#              (msg. acks)           |         (publ. confirms)
#                                    |
#                                    |
#                                    V
#                    /---------------------------------\
#                    |       Node Peers Database       |
#                    \---------------------------------/
#
# The "drainer asyncio task" reads messages from the RabbitMQ Server
# and acks them immediately. However, for some of the messages the
# drainer task will generate fake responses (and will publish them to
# the RabbitMQ Server), as if the responses were generated by the
# deactivated peer. The role of the fake responses is to free up
# resources.
#
# The "Node Peers Database" contains information about the peers of
# the given node.
##############################################################################

import logging
import sys
import asyncio
import click
from datetime import datetime, timezone
from swpt_stomp.loggers import configure_logging
from typing import Union, Optional
from functools import partial
from swpt_stomp.common import (
    WatermarkQueue,
    ServerError,
    Message,
    set_event_loop_policy,
)
from swpt_stomp import smp_schemas
from swpt_stomp.rmq import consume_from_queue, publish_to_exchange
from swpt_stomp.server import EXCHANGE_NAMES
from swpt_stomp.peer_data import get_database_instance
from swpt_stomp.process_messages import (
    transform_message,
    preprocess_message,
    parse_message_body,
)

_logger = logging.getLogger(__name__)
_configure_account_message = smp_schemas.ConfigureAccountMessageSchema()
_finalize_transfer_message = smp_schemas.FinalizeTransferMessageSchema()
_rejected_config_message = smp_schemas.RejectedConfigMessageSchema()


def generate_optional_response(message: Message) -> Optional[Message]:
    if message.type == "AccountUpdate":
        msg_data = parse_message_body(message)
        assert msg_data["type"] == message.type
        response_type = "ConfigureAccount"
        response_json = _configure_account_message.dumps({
            "type": response_type,
            "creditor_id": msg_data["creditor_id"],
            "debtor_id": msg_data["debtor_id"],
            "negligible_amount": 1e30,
            "config_data": "",
            "config_flags": 1,  # scheduled for deletion
            "seqnum": 0,
            "ts": datetime.now(tz=timezone.utc),
        })
    elif message.type == "PreparedTransfer":
        msg_data = parse_message_body(message)
        assert msg_data["type"] == message.type
        response_type = "FinalizeTransfer"
        response_json = _finalize_transfer_message.dumps({
            "type": response_type,
            "creditor_id": msg_data["creditor_id"],
            "debtor_id": msg_data["debtor_id"],
            "transfer_id": msg_data["transfer_id"],
            "coordinator_type": msg_data["coordinator_type"],
            "coordinator_id": msg_data["coordinator_id"],
            "coordinator_request_id": msg_data["coordinator_request_id"],
            "committed_amount": 0,
            "transfer_note": "",
            "transfer_note_format": "",
            "ts": datetime.now(tz=timezone.utc),
        })
    elif message.type == "ConfigureAccount":
        msg_data = parse_message_body(message)
        assert msg_data["type"] == message.type
        response_type = "RejectedConfig"
        response_json = _rejected_config_message.dumps({
            "type": response_type,
            "creditor_id": msg_data["creditor_id"],
            "debtor_id": msg_data["debtor_id"],
            "config_ts": msg_data["ts"],
            "config_seqnum": msg_data["seqnum"],
            "config_flags": msg_data["config_flags"],
            "negligible_amount": msg_data["negligible_amount"],
            "config_data": msg_data["config_data"],
            "rejection_code": "NO_CONNECTION_TO_DEBTOR",
            "ts": datetime.now(tz=timezone.utc),
        })
    else:
        return None

    return Message(
        id=message.id,
        type=response_type,
        body=bytearray(response_json.encode("utf8")),
        content_type="application/json",
    )


async def drain(
    *,
    peer_node_id: str,
    nodedata_url: str,
    protocol_broker_url,
    protocol_broker_queue,
    client_queue_size: int,
    server_queue_size: int,
):
    db = get_database_instance(url=nodedata_url)
    owner_node_data = await db.get_node_data()
    peer_data = await db.get_peer_data(peer_node_id, active_peers_only=False)
    if peer_data is None:
        raise RuntimeError(f"Peer {peer_node_id} is not in the database.")
    if not peer_data.is_deactivated:
        raise RuntimeError(f"Peer {peer_node_id} has not been deactivated.")

    acks_queue: WatermarkQueue[Union[str, None]] = WatermarkQueue(
        client_queue_size
    )
    messages_queue: asyncio.Queue[
        Union[Message, None, ServerError]
    ] = asyncio.Queue(client_queue_size)

    responses_queue: WatermarkQueue[Union[Message, None]] = WatermarkQueue(
        server_queue_size
    )
    confirms_queue: asyncio.Queue[
        Union[str, None, ServerError]
    ] = asyncio.Queue(server_queue_size)

    async def respond_to_messages_if_necessary():
        while message := await messages_queue.get():
            if isinstance(message, ServerError):
                messages_queue.task_done()
                raise message
            if r := generate_optional_response(message):
                await responses_queue.put(r)
            await acks_queue.put(message.id)
            messages_queue.task_done()
        messages_queue.task_done()

    async def ignore_confirmations():
        while confirm := await confirms_queue.get():
            if isinstance(confirm, ServerError):
                confirms_queue.task_done()
                raise confirm
            confirms_queue.task_done()
        confirms_queue.task_done()

    loop = asyncio.get_running_loop()
    read_messages_task = loop.create_task(
        consume_from_queue(
            messages_queue,
            acks_queue,
            url=protocol_broker_url,
            queue_name=protocol_broker_queue,
            transform_message=partial(
                transform_message, owner_node_data, peer_data
            ),
        )
    )
    respond_to_messages_task = loop.create_task(
        respond_to_messages_if_necessary()
    )
    publish_responses_task = loop.create_task(
        publish_to_exchange(
            confirms_queue,
            responses_queue,
            url=protocol_broker_url,
            exchange_name=EXCHANGE_NAMES[owner_node_data.node_type],
            preprocess_message=partial(
                preprocess_message, owner_node_data, peer_data
            ),
        )
    )
    ignore_confirmations_task = loop.create_task(
        ignore_confirmations()
    )

    tasks = [
        read_messages_task,
        respond_to_messages_task,
        publish_responses_task,
        ignore_confirmations_task,
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.wait(tasks)


@click.command()
@click.argument("peer_node_id")
@click.argument("queue_name")
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
    help="Maximum number of consumed messages to store in memory.",
)
@click.option(
    "-b",
    "--server-buffer",
    type=int,
    envvar="SWPT_SERVER_BUFFER",
    default=100,
    show_envvar=True,
    show_default=True,
    help="Maximum number of response messages to store in memory.",
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
def drainer(
    peer_node_id: str,
    queue_name: str,
    nodedata_url: str,
    broker_url: str,
    client_buffer: int,
    server_buffer: int,
    log_level: str,
    log_format: str,
):
    """Consumes (drains) an existing queue associated with an already
    deactivated Swaptacular node.

    PEER_NODE_ID: The node ID of the deactivated peer Swaptacular node.

    QUEUE_NAME: The name of the RabbitMQ queue to consume messages from.
    """
    set_event_loop_policy()
    configure_logging(level=log_level, format=log_format)

    asyncio.run(
        drain(
            peer_node_id=peer_node_id,
            nodedata_url=nodedata_url,
            client_queue_size=client_buffer,
            server_queue_size=server_buffer,
            protocol_broker_url=broker_url,
            protocol_broker_queue=queue_name,
        )
    )
    sys.exit(1)  # pragma: nocover


if __name__ == "__main__":  # pragma: nocover
    drainer()
