import pytest
from swpt_stomp.common import Message
from swpt_stomp.rmq import RmqMessage
from swpt_stomp.process_messages import (
    ProcessingError, transform_message, preprocess_message,
)


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
    from swpt_stomp.peer_data import Subnet
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

    acc_purge_body = bytearray("""{
      "type": "AccountPurge",
      "debtor_id": 123,
      "creditor_id": 456,
      "creation_date": "2001-01-01",
      "ts": "2023-01-01T12:00:00Z"
    }""".encode('utf8'))

    prep_transfer_body = bytearray("""{
      "type": "PrepareTransfer",
      "debtor_id": 123,
      "creditor_id": 456,
      "min_locked_amount": 1000,
      "max_locked_amount": 2000,
      "recipient": "RECIPIENT",
      "min_interest_rate": -10.0,
      "max_commit_delay": 100000,
      "coordinator_type": "test",
      "coordinator_id": 789,
      "coordinator_request_id": 1111,
      "ts": "2023-01-01T12:00:00Z"
    }""".encode('utf8'))

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
            allow_out_messages=False,
        )

    with pytest.raises(ProcessingError):
        _parse_message_body(
            Message(
                id='1',
                type='AccountPurge',
                body=acc_purge_body,
                content_type='application/json',
            ),
            allow_out_messages=False,
        )

    with pytest.raises(ProcessingError):
        _parse_message_body(
            Message(
                id='1',
                type='PrepareTransfer',
                body=prep_transfer_body,
                content_type='application/json',
            ),
            allow_in_messages=False,
        )

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
