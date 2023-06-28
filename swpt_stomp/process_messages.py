from typing import Union, Any
from swpt_stomp.common import Message
from swpt_stomp.peer_data import NodeData, PeerData, NodeType
from swpt_stomp.rmq import RmqMessage
from swpt_stomp.smp_schemas import (
    JSON_SCHEMAS, CLIENT_MESSAGE_TYPES, SERVER_MESSAGE_TYPES, ValidationError,
)


class ParseError(ValueError):
    """Indicates that message can not be parsed."""


def transform_message(
        owner_node_data: NodeData,
        peer_data: PeerData,
        message: RmqMessage,
) -> Message:
    owner_node_type = owner_node_data.node_type
    msg_data = _parse_message_body(
        message,
        allow_client_messages=(owner_node_type != NodeType.AA),
        allow_server_messages=(owner_node_type == NodeType.AA),
    )

    creditor_id: int = msg_data['creditor_id']
    if not owner_node_data.creditors_subnet.match(creditor_id):
        raise ValueError(f'invalid creditor ID: {creditor_id}')

    debtor_id: int = msg_data['debtor_id']
    if not peer_data.debtors_subnet.match(debtor_id):
        raise ValueError(f'invalid debtor ID: {debtor_id}')

    if owner_node_type == NodeType.CA:
        # Translate "creditor_id" from owner's subnet to peer's subnet.
        peer_subnet = peer_data.creditors_subnet
        mask = peer_subnet.subnet_mask
        assert mask == owner_node_data.creditors_subnet.subnet_mask
        relative_id = (creditor_id & mask) ^ creditor_id
        msg_data['creditor_id'] = peer_subnet.subnet | relative_id

    msg_json = JSON_SCHEMAS[message.type].dumps(msg_data)
    return Message(
        id=message.id,
        type=message.type,
        body=bytearray(msg_json.encode('utf8')),
        content_type='application/json',
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


def _parse_message_body(
        m: Union[Message, RmqMessage],
        *,
        allow_client_messages: bool = True,
        allow_server_messages: bool = True,
) -> Any:
    if m.content_type != 'application/json':
        raise ParseError(f'unsupported content type: {m.content_type}')

    msg_type = m.type
    try:
        if not allow_client_messages and msg_type in CLIENT_MESSAGE_TYPES:
            raise KeyError
        if not allow_server_messages and msg_type in SERVER_MESSAGE_TYPES:
            raise KeyError
        schema = JSON_SCHEMAS[msg_type]
    except KeyError:
        raise ParseError(f'invalid message type: {msg_type}')

    try:
        body = m.body.decode('utf8')
    except UnicodeDecodeError:
        raise ParseError('UTF-8 decode error')

    try:
        return schema.loads(body)
    except ValidationError as e:
        raise ParseError(f'invalid {msg_type} message') from e
