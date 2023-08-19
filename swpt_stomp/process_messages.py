import json
from hashlib import md5
from typing import Union, Any
from datetime import datetime, timezone
from swpt_stomp.common import Message, ServerError
from swpt_stomp.peer_data import NodeData, PeerData, NodeType, Subnet
from swpt_stomp.rmq import RmqMessage, HeadersType
from swpt_stomp.smp_schemas import (
    JSON_SCHEMAS,
    IN_MESSAGE_TYPES,
    OUT_MESSAGE_TYPES,
    ValidationError,
)


class ProcessingError(Exception):
    """Indicates that the message can not be processed."""

    def __init__(self, error_message: str):
        super().__init__(error_message)
        self.error_message = error_message


def transform_message(
    owner_node_data: NodeData,
    peer_data: PeerData,
    message: RmqMessage,
) -> Message:
    owner_node_type = owner_node_data.node_type
    msg_data = _parse_message_body(
        message,
        allow_in_messages=(owner_node_type != NodeType.AA),
        allow_out_messages=(owner_node_type == NodeType.AA),
    )
    creditor_id: int = msg_data["creditor_id"]
    debtor_id: int = msg_data["debtor_id"]

    if owner_node_type == NodeType.CA:
        if not owner_node_data.creditors_subnet.match(creditor_id):
            raise ProcessingError(
                f"Invalid creditor ID: {_as_hex(creditor_id)}."
            )

        # NOTE: For creditors agent nodes, before we send a message to an
        # accounting authority, we must update "creditor_id" and
        # "coordinator_id" fields, so as they are in the range (subnet)
        # reserved for the creditors agent.
        orig_id = creditor_id
        msg_data["creditor_id"] = creditor_id = _change_subnet(
            creditor_id,
            from_=owner_node_data.creditors_subnet,
            to_=peer_data.creditors_subnet,
        )
        if (
            "coordinator_id" in msg_data
            and msg_data["coordinator_type"] == "direct"
        ):
            if msg_data["coordinator_id"] != orig_id:  # pragma: nocover
                raise ProcessingError("Invalid coordinator ID.")
            msg_data["coordinator_id"] = creditor_id

    if not peer_data.creditors_subnet.match(creditor_id):
        raise ProcessingError(f"Invalid creditor ID: {_as_hex(creditor_id)}.")

    if not peer_data.debtors_subnet.match(debtor_id):
        raise ProcessingError(f"Invalid debtor ID: {_as_hex(debtor_id)}.")

    current_ts = datetime.now(tz=timezone.utc)
    secs_ahead = (msg_data["ts"] - current_ts).total_seconds()
    if secs_ahead > 7200:  # pragma: no cover
        raise ProcessingError(
            f"The message timestamp is {secs_ahead:.0f} seconds in the future."
        )

    msg_json = JSON_SCHEMAS[message.type].dumps(msg_data)
    return Message(
        id=message.id,
        type=message.type,
        body=bytearray(msg_json.encode("utf8")),
        content_type="application/json",
    )


async def preprocess_message(
    owner_node_data: NodeData,
    peer_data: PeerData,
    message: Message,
) -> RmqMessage:
    try:
        owner_node_type = owner_node_data.node_type
        msg_data = _parse_message_body(
            message,
            allow_in_messages=(owner_node_type == NodeType.AA),
            allow_out_messages=(owner_node_type != NodeType.AA),
        )
        creditor_id: int = msg_data["creditor_id"]
        debtor_id: int = msg_data["debtor_id"]

        if not peer_data.creditors_subnet.match(creditor_id):
            raise ProcessingError(
                f"Invalid creditor ID: {_as_hex(creditor_id)}."
            )

        if not peer_data.debtors_subnet.match(debtor_id):
            raise ProcessingError(f"Invalid debtor ID: {_as_hex(debtor_id)}.")

        if owner_node_type == NodeType.CA:
            # NOTE: For creditors agent nodes, before we accept a message
            # from an accounting authority, we must update "creditor_id" and
            # "coordinator_id" fields, so as they are in the range (subnet)
            # used by the creditors agent.
            orig_id = creditor_id
            msg_data["creditor_id"] = creditor_id = _change_subnet(
                creditor_id,
                from_=peer_data.creditors_subnet,
                to_=owner_node_data.creditors_subnet,
            )
            if (
                "coordinator_id" in msg_data
                and msg_data["coordinator_type"] == "direct"
            ):
                if msg_data["coordinator_id"] != orig_id:  # pragma: nocover
                    raise ProcessingError("Invalid coordinator ID.")
                msg_data["coordinator_id"] = creditor_id

        msg_type = message.type
        headers: HeadersType = {
            "message-type": msg_type,
            "debtor-id": debtor_id,
            "creditor-id": creditor_id,
        }
        if "coordinator_id" in msg_data:
            headers["coordinator-id"] = msg_data["coordinator_id"]
            headers["coordinator-type"] = c_type = msg_data["coordinator_type"]
            peer_type = peer_data.node_type
            if (peer_type == NodeType.CA and c_type != "direct") or (
                peer_type == NodeType.DA and c_type != "issuing"
            ):
                raise ProcessingError(f"Invalid coordinator type: {c_type}.")

        if owner_node_type == NodeType.AA:
            routing_key = _calc_bin_routing_key(debtor_id, creditor_id)
        elif owner_node_type == NodeType.CA:
            routing_key = _calc_bin_routing_key(creditor_id)
        else:
            assert owner_node_type == NodeType.DA
            routing_key = _calc_bin_routing_key(debtor_id)

        msg_json = JSON_SCHEMAS[msg_type].dumps(msg_data)
        return RmqMessage(
            id=message.id,
            body=msg_json.encode("utf8"),
            headers=headers,
            type=msg_type,
            content_type="application/json",
            routing_key=routing_key,
        )

    except ProcessingError as e:
        raise ServerError(
            error_message=e.error_message,
            receipt_id=message.id,
            context=message.body,
            context_type=message.type,
            context_content_type=message.content_type,
        )


def _parse_message_body(
    m: Union[Message, RmqMessage],
    *,
    allow_in_messages: bool = True,
    allow_out_messages: bool = True,
) -> Any:
    if m.content_type != "application/json":
        raise ProcessingError(f"Unsupported content type: {m.content_type}.")

    msg_type = m.type
    try:
        if not allow_in_messages and msg_type in IN_MESSAGE_TYPES:
            raise KeyError
        if not allow_out_messages and msg_type in OUT_MESSAGE_TYPES:
            raise KeyError
        schema = JSON_SCHEMAS[msg_type]
    except KeyError:
        raise ProcessingError(f"Invalid message type: {msg_type}.")

    try:
        return schema.loads(m.body.decode("utf8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as e:
        raise ProcessingError(f"Invalid {msg_type} message.") from e


def _change_subnet(creditor_id, *, from_: Subnet, to_: Subnet) -> int:
    """Translate `creditor_id` from one subnet to another."""
    mask = from_.subnet_mask
    assert mask == to_.subnet_mask
    subnet = creditor_id & mask
    assert subnet == from_.subnet
    relative_id = subnet ^ creditor_id
    return to_.subnet | relative_id


def _as_hex(n: int) -> str:
    return "0x" + n.to_bytes(8, byteorder="big", signed=True).hex()


def _calc_bin_routing_key(first: int, *rest: int) -> str:
    """Calculate a binary RabbitMQ routing key from one or more i64 numbers.

    The binary routing key is calculated by taking the highest 24
    bits, separated with dots, of the MD5 digest of the passed
    numbers. For example:

      >>> calc_bin_routing_key(123)
      '1.1.1.1.1.1.0.0.0.0.0.1.0.0.0.0.0.1.1.0.0.0.1.1'
      >>> calc_bin_routing_key(-123)
      '1.1.0.0.0.0.1.1.1.1.1.1.1.1.1.0.1.0.1.0.1.1.1.1'
      >>> calc_bin_routing_key(123, 456)
      '0.0.0.0.1.0.0.0.0.1.0.0.0.1.0.0.0.0.1.1.0.1.0.0'
    """
    m = md5()
    m.update(first.to_bytes(8, byteorder="big", signed=True))
    for n in rest:
        m.update(n.to_bytes(8, byteorder="big", signed=True))
    s = "".join([format(byte, "08b") for byte in m.digest()[:3]])
    assert len(s) == 24
    return ".".join(s)
