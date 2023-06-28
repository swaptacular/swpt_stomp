from typing import Union, Any
from swpt_stomp.common import Message
from swpt_stomp.peer_data import NodeData, PeerData, NodeType, Subnet
from swpt_stomp.rmq import RmqMessage, HeadersType
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
        msg_data['creditor_id'] = _change_subnet(
            creditor_id,
            from_=owner_node_data.creditors_subnet,
            to_=peer_data.creditors_subnet,
        )

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
    # TODO: raise `ServerError`s.

    owner_node_type = owner_node_data.node_type
    msg_data = _parse_message_body(
        message,
        allow_client_messages=(owner_node_type == NodeType.AA),
        allow_server_messages=(owner_node_type != NodeType.AA),
    )

    creditor_id: int = msg_data['creditor_id']
    if not peer_data.creditors_subnet.match(creditor_id):
        raise ValueError(f'invalid creditor ID: {creditor_id}')

    debtor_id: int = msg_data['debtor_id']
    if not peer_data.debtors_subnet.match(debtor_id):
        raise ValueError(f'invalid debtor ID: {debtor_id}')

    if owner_node_type == NodeType.CA:
        msg_data['creditor_id'] = _change_subnet(
            creditor_id,
            from_=peer_data.creditors_subnet,
            to_=owner_node_data.creditors_subnet,
        )

    msg_type = message.type
    headers: HeadersType = {
        'message-type': msg_type,
        'debtor-id': debtor_id,
        'creditor-id': creditor_id,
    }
    if 'coordinator_id' in msg_data:
        headers['coordinator-id'] = msg_data['coordinator_id']
        headers['coordinator-type'] = msg_data['coordinator_type']
        # TODO: Verify "coordinator-type".

    msg_json = JSON_SCHEMAS[msg_type].dumps(msg_data)
    return RmqMessage(
        id=message.id,
        body=msg_json.encode('utf8'),
        headers=headers,
        type=msg_type,
        content_type='application/json',
        routing_key='',  # TODO: Set a routing key.
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


def _change_subnet(creditor_id, *, from_: Subnet, to_: Subnet) -> int:
    """Translate `creditor_id` from one subnet to another.
    """
    mask = from_.subnet_mask
    assert mask == to_.subnet_mask
    subnet = creditor_id & mask
    assert subnet == from_.subnet
    relative_id = subnet ^ creditor_id
    return to_.subnet | relative_id
