import pytest
import json
from swpt_stomp.common import Message, ServerError
from swpt_stomp.rmq import RmqMessage
from swpt_stomp.peer_data import Subnet, get_database_instance
from swpt_stomp.process_messages import (
    ProcessingError, transform_message, preprocess_message,
)


def create_account_purge_msg(debtor_id: int, creditor_id: int) -> str:
    props = f"""
      "type": "AccountPurge",
      "debtor_id": {debtor_id},
      "creditor_id": {creditor_id},
      "creation_date": "2001-01-01",
      "ts": "2023-01-01T12:00:00+00:00"
    """
    return '{' + props + '}'


def create_prepare_transfer_msg(
        debtor_id: int,
        creditor_id: int,
        coordinator_type: str = 'test',
        coordinator_id: int = 789,
) -> str:
    props = f"""
      "type": "PrepareTransfer",
      "debtor_id": {debtor_id},
      "creditor_id": {creditor_id},
      "min_locked_amount": 1000,
      "max_locked_amount": 2000,
      "recipient": "RECIPIENT",
      "min_interest_rate": -10.0,
      "max_commit_delay": 100000,
      "coordinator_type": "{coordinator_type}",
      "coordinator_id": {coordinator_id},
      "coordinator_request_id": 1111,
      "ts": "2023-01-01T12:00:00+00:00"
    """
    return '{' + props + '}'


def test_calc_bin_routing_key():
    from swpt_stomp.process_messages import _calc_bin_routing_key
    assert _calc_bin_routing_key(123) == \
        '1.1.1.1.1.1.0.0.0.0.0.1.0.0.0.0.0.1.1.0.0.0.1.1'
    assert _calc_bin_routing_key(-123) == \
        '1.1.0.0.0.0.1.1.1.1.1.1.1.1.1.0.1.0.1.0.1.1.1.1'
    assert _calc_bin_routing_key(123, 456) == \
        '0.0.0.0.1.0.0.0.0.1.0.0.0.1.0.0.0.0.1.1.0.1.0.0'

    with pytest.raises(OverflowError):
        _calc_bin_routing_key(99999999999999999999999999999999999)
    with pytest.raises(Exception):
        _calc_bin_routing_key('')


def test_as_hex():
    from swpt_stomp.process_messages import _as_hex
    assert _as_hex(15) == '0x000000000000000f'


def test_change_subnet():
    from swpt_stomp.process_messages import _change_subnet

    assert _change_subnet(
        0x0100000000000abc,
        from_=Subnet.parse('01'),
        to_=Subnet.parse('02'),
    ) == 0x0200000000000abc

    with pytest.raises(Exception):
        _change_subnet(
            0x0100000000000abc,
            from_=Subnet.parse('01'),
            to_=Subnet.parse('002'),
        )
    with pytest.raises(Exception):
        _change_subnet(
            0x0100000000000abc,
            from_=Subnet.parse('001'),
            to_=Subnet.parse('02'),
        )


def test_parse_message_body():
    from swpt_stomp.process_messages import _parse_message_body

    acc_purge_body = bytearray(
        create_account_purge_msg(123, 456).encode('utf8'))
    prep_transfer_body = bytearray(
        create_prepare_transfer_msg(123, 456).encode('utf8'))

    with pytest.raises(ProcessingError):
        _parse_message_body(Message(
            id='1',
            type='AccountPurge',
            body=acc_purge_body,
            content_type='application/unknown',
        ))
    with pytest.raises(ProcessingError):
        _parse_message_body(Message(
            id='1',
            type='WrongType',
            body=acc_purge_body,
            content_type='application/json',
        ))
    with pytest.raises(ProcessingError):
        _parse_message_body(Message(
            id='1',
            type='AccountPurge',
            body=bytearray(b'\xa0\x20'),
            content_type='application/json',
        ))
    with pytest.raises(ProcessingError):
        _parse_message_body(Message(
            id='1',
            type='AccountPurge',
            body=bytearray(b'INVALID JSON'),
            content_type='application/json',
        ))
    with pytest.raises(ProcessingError):
        _parse_message_body(Message(
            id='1',
            type='AccountPurge',
            body=bytearray(b'{}'),
            content_type='application/json',
        ))
    with pytest.raises(ProcessingError):
        _parse_message_body(Message(
            id='1',
            type='AccountPurge',
            body=bytearray(b'"xxx"'),
            content_type='application/json',
        ))

    obj = _parse_message_body(Message(
        id='1',
        type='AccountPurge',
        body=acc_purge_body,
        content_type='application/json',
    ))
    assert obj["type"] == 'AccountPurge'
    assert obj["debtor_id"] == 123
    assert obj["creditor_id"] == 456

    with pytest.raises(ProcessingError):
        _parse_message_body(
            Message(
                id='1',
                type='AccountPurge',
                body=acc_purge_body,
                content_type='application/json',
            ),
            allow_out_messages=False)

    with pytest.raises(ProcessingError):
        _parse_message_body(
            Message(
                id='1',
                type='AccountPurge',
                body=acc_purge_body,
                content_type='application/json',
            ),
            allow_out_messages=False)
    with pytest.raises(ProcessingError):
        _parse_message_body(
            Message(
                id='1',
                type='PrepareTransfer',
                body=prep_transfer_body,
                content_type='application/json',
            ),
            allow_in_messages=False)

    obj = _parse_message_body(Message(
        id='1',
        type='PrepareTransfer',
        body=prep_transfer_body,
        content_type='application/json',
    ))
    assert obj["type"] == 'PrepareTransfer'
    assert obj["debtor_id"] == 123
    assert obj["creditor_id"] == 456
    assert obj["coordinator_type"] == "test"
    assert obj["coordinator_id"] == 789


@pytest.mark.asyncio
async def test_transform_message_aa(datadir):
    db = get_database_instance(url=f'file://{datadir["AA"]}')
    owner_node_data = await db.get_node_data()

    def transform(s: str) -> Message:
        message = RmqMessage(
            id='1',
            type='AccountPurge',
            body=s.encode('utf8'),
            content_type='application/json',
            headers={},
            routing_key=None,
        )
        return transform_message(owner_node_data, peer_data, message)

    # Test sending AccountPurge messages to CA:
    peer_data = await db.get_peer_data('5921983fe0e6eb987aeedca54ad3c708')
    s = create_account_purge_msg(0x1234abcd00000001, 0x0000010000000abc)
    m = transform(s)
    assert isinstance(m, Message)
    assert m.id == '1'
    assert m.type == 'AccountPurge'
    assert m.content_type == 'application/json'
    assert json.loads(m.body.decode('utf8')) == json.loads(s)

    s = create_account_purge_msg(0x1234abce00000001, 0x0000010000000abc)
    with pytest.raises(ProcessingError):
        # invalid debtor ID
        transform(s)

    s = create_account_purge_msg(0x1234abcd00000001, 0x0000020000000abc)
    with pytest.raises(ProcessingError):
        # invalid creditor ID
        transform(s)

    # Test sending AccountPurge messages to DA:
    peer_data = await db.get_peer_data('060791aeca7637fa3357dfc0299fb4c5')
    s = create_account_purge_msg(0x1234abcd00000001, 0x0000000000000000)
    m = transform(s)
    assert isinstance(m, Message)
    assert m.id == '1'
    assert m.type == 'AccountPurge'
    assert m.content_type == 'application/json'
    assert json.loads(m.body.decode('utf8')) == json.loads(s)

    s = create_account_purge_msg(0x1234abce01000001, 0x0000000000000000)
    with pytest.raises(ProcessingError):
        # invalid debtor ID
        transform(s)

    s = create_account_purge_msg(0x1234abcd00000001, 0x0000000000000001)
    with pytest.raises(ProcessingError):
        # invalid creditor ID
        transform(s)


@pytest.mark.asyncio
async def test_transform_message_ca(datadir):
    db = get_database_instance(url=f'file://{datadir["CA"]}')
    owner_node_data = await db.get_node_data()
    peer_data = await db.get_peer_data('1234abcd')

    def transform(s: str) -> Message:
        message = RmqMessage(
            id='1',
            type='PrepareTransfer',
            body=s.encode('utf8'),
            content_type='application/json',
            headers={},
            routing_key=None,
        )
        return transform_message(owner_node_data, peer_data, message)

    s = create_prepare_transfer_msg(0x1234abcd00000001, 0x0000080000000abc)
    m = transform(s)
    assert isinstance(m, Message)
    assert m.id == '1'
    assert m.type == 'PrepareTransfer'
    assert m.content_type == 'application/json'
    assert json.loads(m.body.decode('utf8')) == json.loads(
        create_prepare_transfer_msg(0x1234abcd00000001, 0x0000010000000abc))

    # Invalid debtor ID:
    s = create_prepare_transfer_msg(0x1234abce00000001, 0x0000080000000abc)
    with pytest.raises(ProcessingError):
        transform(s)

    # Invalid creditor ID:
    s = create_prepare_transfer_msg(0x1234abcd00000001, 0x0000020000000abc)
    with pytest.raises(ProcessingError):
        transform(s)


@pytest.mark.asyncio
async def test_transform_message_da(datadir):
    db = get_database_instance(url=f'file://{datadir["DA"]}')
    owner_node_data = await db.get_node_data()
    peer_data = await db.get_peer_data('1234abcd')

    def transform(s: str) -> Message:
        message = RmqMessage(
            id='1',
            type='PrepareTransfer',
            body=s.encode('utf8'),
            content_type='application/json',
            headers={},
            routing_key=None,
        )
        return transform_message(owner_node_data, peer_data, message)

    s = create_prepare_transfer_msg(0x1234abcd00000001, 0x0000000000000000)
    m = transform(s)
    assert isinstance(m, Message)
    assert m.id == '1'
    assert m.type == 'PrepareTransfer'
    assert m.content_type == 'application/json'
    assert json.loads(m.body.decode('utf8')) == json.loads(s)

    # Invalid debtor ID:
    s = create_prepare_transfer_msg(0x1234abcd01000001, 0x0000000000000000)
    with pytest.raises(ProcessingError):
        transform(s)

    # Invalid creditor ID:
    s = create_prepare_transfer_msg(0x1234abcd00000001, 0x00000000000000001)
    with pytest.raises(ProcessingError):
        transform(s)


@pytest.mark.asyncio
async def test_preprocess_message_aa(datadir):
    db = get_database_instance(url=f'file://{datadir["AA"]}')
    owner_node_data = await db.get_node_data()

    async def preprocess(s: str) -> RmqMessage:
        message = Message(
            id='1',
            type='PrepareTransfer',
            body=bytearray(s.encode('utf8')),
            content_type='application/json',
        )
        return await preprocess_message(owner_node_data, peer_data, message)

    # Test receiving PrepareTransfer messages from CA:
    peer_data = await db.get_peer_data('5921983fe0e6eb987aeedca54ad3c708')
    s = create_prepare_transfer_msg(
        0x1234abcd00000001, 0x0000010000000abc, 'direct', 0x0000010000000abc)
    m = await preprocess(s)
    assert isinstance(m, RmqMessage)
    assert m.id == '1'
    assert m.type == 'PrepareTransfer'
    assert m.content_type == 'application/json'
    assert m.headers == {
        'message-type': 'PrepareTransfer',
        'debtor-id': 0x1234abcd00000001,
        'creditor-id': 0x0000010000000abc,
        'coordinator-id': 0x0000010000000abc,
        'coordinator-type': 'direct',
    }
    assert json.loads(m.body.decode('utf8')) == json.loads(s)

    s = create_prepare_transfer_msg(
        0x1234abce00000001, 0x0000010000000abc, 'direct', 0x0000010000000abc)
    with pytest.raises(ServerError):
        # invalid debtor ID
        await preprocess(s)

    s = create_prepare_transfer_msg(
        0x1234abcd00000001, 0x0000020000000abc, 'direct', 0x0000020000000abc)
    with pytest.raises(ServerError):
        # invalid creditor ID
        await preprocess(s)

    s = create_prepare_transfer_msg(
        0x1234abcd00000001, 0x0000010000000abc, 'invalid', 0x0000020000000abc)
    with pytest.raises(ServerError):
        # invalid coordinator type
        await preprocess(s)

    # Test receiving PrepareTransfer messages from DA:
    peer_data = await db.get_peer_data('060791aeca7637fa3357dfc0299fb4c5')
    s = create_prepare_transfer_msg(
        0x1234abcd00000001, 0x0000000000000000, 'issuing', 0x1234abcd00000001)
    m = await preprocess(s)
    assert isinstance(m, RmqMessage)
    assert m.id == '1'
    assert m.type == 'PrepareTransfer'
    assert m.content_type == 'application/json'
    assert m.headers == {
        'message-type': 'PrepareTransfer',
        'debtor-id': 0x1234abcd00000001,
        'creditor-id': 0x0000000000000000,
        'coordinator-id': 0x1234abcd00000001,
        'coordinator-type': 'issuing',
    }
    assert json.loads(m.body.decode('utf8')) == json.loads(s)

    s = create_prepare_transfer_msg(
        0x1234abce01000001, 0x0000000000000000, 'issuing', 0x1234abce01000001)
    with pytest.raises(ServerError):
        # invalid debtor ID
        await preprocess(s)

    s = create_prepare_transfer_msg(
        0x1234abcd00000001, 0x0000000000000001, 'issuing', 0x1234abcd00000001)
    with pytest.raises(ServerError):
        # invalid creditor ID
        await preprocess(s)

    s = create_prepare_transfer_msg(
        0x1234abcd00000001, 0x0000000000000000, 'invalid', 0x1234abcd00000001)
    with pytest.raises(ServerError):
        # invalid coordinator type
        await preprocess(s)


@pytest.mark.asyncio
async def test_preprocess_message_ca(datadir):
    db = get_database_instance(url=f'file://{datadir["CA"]}')
    owner_node_data = await db.get_node_data()

    async def preprocess(s: str) -> RmqMessage:
        message = Message(
            id='1',
            type='AccountPurge',
            body=bytearray(s.encode('utf8')),
            content_type='application/json',
        )
        return await preprocess_message(owner_node_data, peer_data, message)

    peer_data = await db.get_peer_data('1234abcd')
    s = create_account_purge_msg(0x1234abcd00000001, 0x0000010000000abc)
    m = await preprocess(s)
    assert isinstance(m, RmqMessage)
    assert m.id == '1'
    assert m.type == 'AccountPurge'
    assert m.content_type == 'application/json'
    assert m.headers == {
        'message-type': 'AccountPurge',
        'debtor-id': 0x1234abcd00000001,
        'creditor-id': 0x0000080000000abc,
    }
    assert json.loads(m.body.decode('utf8')) == json.loads(
        create_account_purge_msg(0x1234abcd00000001, 0x0000080000000abc))

    s = create_account_purge_msg(0x1234abce00000001, 0x0000010000000abc)
    with pytest.raises(ServerError):
        # invalid debtor ID
        await preprocess(s)

    s = create_account_purge_msg(0x1234abcd00000001, 0x0000020000000abc)
    with pytest.raises(ServerError):
        # invalid creditor ID
        await preprocess(s)


@pytest.mark.asyncio
async def test_preprocess_message_da(datadir):
    db = get_database_instance(url=f'file://{datadir["DA"]}')
    owner_node_data = await db.get_node_data()

    async def preprocess(s: str) -> RmqMessage:
        message = Message(
            id='1',
            type='AccountPurge',
            body=bytearray(s.encode('utf8')),
            content_type='application/json',
        )
        return await preprocess_message(owner_node_data, peer_data, message)

    peer_data = await db.get_peer_data('1234abcd')
    s = create_account_purge_msg(0x1234abcd00000001, 0x0000000000000000)
    m = await preprocess(s)
    assert isinstance(m, RmqMessage)
    assert m.id == '1'
    assert m.type == 'AccountPurge'
    assert m.content_type == 'application/json'
    assert m.headers == {
        'message-type': 'AccountPurge',
        'debtor-id': 0x1234abcd00000001,
        'creditor-id': 0x0000000000000000,
    }
    assert json.loads(m.body.decode('utf8')) == json.loads(s)

    s = create_account_purge_msg(0x1234abcd01000001, 0x0000000000000000)
    with pytest.raises(ServerError):
        # invalid debtor ID
        await preprocess(s)

    s = create_account_purge_msg(0x1234abcd00000001, 0x0000000000000001)
    with pytest.raises(ServerError):
        # invalid creditor ID
        await preprocess(s)
