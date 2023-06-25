from swpt_stomp.common import Message
from swpt_stomp.peer_data import NodeData, PeerData
from swpt_stomp.rmq import RmqMessage


def transform_message(
        owner_node_data: NodeData,
        peer_data: PeerData,
        message: RmqMessage,
) -> Message:
    return Message(
        id=message.id,
        type=message.type,
        body=bytearray(message.body),
        content_type=message.content_type,
    )


async def preprocess_message(
        owner_node_data: NodeData,
        peer_data: PeerData,
        message: Message,
) -> RmqMessage:
    return RmqMessage(
        id=message.id,
        body=bytes(message.body),
        headers={},
        type=message.type,
        content_type=message.content_type,
        routing_key='',
    )
