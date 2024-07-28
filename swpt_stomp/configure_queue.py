import logging
import sys
import asyncio
import click
import aio_pika
from swpt_stomp.loggers import configure_logging
from swpt_stomp.common import set_event_loop_policy
from swpt_stomp.peer_data import get_database_instance, NodeType

_logger = logging.getLogger(__name__)


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
def configure_queue(
    peer_node_id: str,
    queue_name: str,
    nodedata_url: str,
    broker_url: str,
    log_level: str,
    log_format: str,
):
    """Configures a RabbitMQ queue that will contain messages which have to
    be sent to a specific peer Swaptacular node.

    PEER_NODE_ID: The node ID of the peer Swaptacular node.

    QUEUE_NAME: The name of the RabbitMQ queue that should be configured.
    """

    async def bind_queue() -> None:
        db = get_database_instance(url=nodedata_url)
        owner_node_data = await db.get_node_data()
        peer_data = await db.get_peer_data(
            peer_node_id, active_peers_only=False
        )
        if peer_data is None:
            _logger.error("Peer %s is not in the database.", peer_node_id)
            sys.exit(1)

        connection = await aio_pika.connect(broker_url)
        channel = await connection.channel()

        # Declare the main exchange to which all outgoing messages to peers
        # of the same type (`peer_data.node_type`) will be published.
        main_exchange: aio_pika.abc.AbstractExchange
        binding_key: str
        owner_prefix: str
        peer_node_type = peer_data.node_type
        owner_node_type = owner_node_data.node_type
        if owner_node_type == NodeType.AA:
            owner_prefix = "aa"
            if peer_node_type == NodeType.CA:
                main_exchange = await channel.declare_exchange(
                    "to_creditors", "topic", durable=True
                )
                binding_key = peer_data.creditors_subnet.binding_key
            else:
                assert peer_node_type == NodeType.DA
                main_exchange = await channel.declare_exchange(
                    "to_debtors", "topic", durable=True
                )
                binding_key = peer_data.debtors_subnet.binding_key
        elif owner_node_type == NodeType.CA:
            assert peer_node_type == NodeType.AA
            owner_prefix = "ca"
            await channel.declare_exchange(
                "ca.loopback_filter", "headers", durable=True
            )
            main_exchange = await channel.declare_exchange(
                "creditors_out",
                "topic",
                durable=True,
                arguments={"alternate-exchange": "ca.loopback_filter"},
            )
            binding_key = peer_data.debtors_subnet.binding_key
        else:
            assert owner_node_type == NodeType.DA
            assert peer_node_type == NodeType.AA
            owner_prefix = "da"
            main_exchange = await channel.declare_exchange(
                "debtors_out", "fanout", durable=True
            )
            binding_key = ""

        # Declare a peer random exchange that evenly distributes the
        # messages which have to be sent to the peer, among the peer's
        # configured queues.
        peer_exchange = await channel.declare_exchange(
            f"{owner_prefix}.x.{peer_node_id}",
            "x-random",
            durable=True,
        )
        await peer_exchange.bind(main_exchange, binding_key)

        # Declare the requested queue along with its corresponding
        # dead-letter queue, and bind it to the peer exchange.
        #
        # TODO: It would probably be better to use a "stream" instead
        #       of classic queues here, given that we have figured out
        #       how to do the stream offset tracking. This would allow
        #       for high-availability. Using a "quorum" queues here is
        #       almost certainly not a good idea, because quorum
        #       queues consume lots of memory when there are lots of
        #       messages in the queue, which should be expected.
        dead_letter_queue_name = queue_name + ".XQ"
        await channel.declare_queue(
            dead_letter_queue_name,
            durable=True,
        )
        queue = await channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": dead_letter_queue_name,
            },
        )
        await queue.bind(peer_exchange)

    set_event_loop_policy()
    configure_logging(level=log_level, format=log_format)
    asyncio.run(bind_queue())
    _logger.info(
        "Successully configured queue: %s", queue_name
    )  # pragma: no cover


if __name__ == "__main__":  # pragma: nocover
    configure_queue()
