import pytest
import asyncio
import aio_pika
import json
from typing import Optional
from contextlib import suppress
from swpt_stomp import drainer
from swpt_stomp.common import ServerError

_MAX_INT64 = (1 << 63) - 1
_MAX_UINT64 = (1 << 64) - 1
_I64_SPAN = _MAX_UINT64 + 1


def u64_to_i64(value: int) -> int:
    if value > _MAX_UINT64 or value < 0:
        raise ValueError()
    if value <= _MAX_INT64:
        return value
    return value - _I64_SPAN


def create_account_update_msg(debtor_id: int, creditor_id: int) -> str:
    props = f"""
      "type": "AccountUpdate",
      "debtor_id": {debtor_id},
      "creditor_id": {creditor_id},
      "creation_date": "2020-01-01",
      "last_change_ts": "2022-01-01T12:00:00+00:00",
      "last_change_seqnum": 123,
      "principal": 1000,
      "interest": 5.4,
      "interest_rate": 3.14,
      "last_interest_rate_change_ts": "2022-01-01T12:00:00+00:00",
      "last_config_ts": "2022-01-01T12:00:00+00:00",
      "last_config_seqnum": 456,
      "negligible_amount": 100.0,
      "config_flags": 0,
      "config_data": "TEST_CONFIG",
      "account_id": "TEST_ACCOUNT",
      "debtor_info_iri": "",
      "debtor_info_content_type": "",
      "debtor_info_sha256": "",
      "last_transfer_number": 789,
      "last_transfer_committed_at": "2022-01-01T12:00:00+00:00",
      "demurrage_rate": -10.0,
      "commit_period": 5000000,
      "transfer_note_max_bytes": 300,
      "ttl": 10000,
      "ts": "2023-01-01T12:00:00+00:00"
    """
    return "{" + props + "}"


def create_prepare_transfer_msg(
    debtor_id: int,
    creditor_id: int,
    coordinator_type: str = "direct",
    coordinator_id: Optional[int] = None,
) -> str:
    if coordinator_id is None:
        coordinator_id = creditor_id

    props = f"""
      "type": "PrepareTransfer",
      "debtor_id": {debtor_id},
      "creditor_id": {creditor_id},
      "min_locked_amount": 1000,
      "max_locked_amount": 2000,
      "recipient": "RECIPIENT",
      "final_interest_rate_ts": "9999-12-31T23:59:59+00:00",
      "max_commit_delay": 100000,
      "coordinator_type": "{coordinator_type}",
      "coordinator_id": {coordinator_id},
      "coordinator_request_id": 1111,
      "ts": "2023-01-01T12:00:00+00:00"
    """
    return "{" + props + "}"


def create_prepared_transfer_msg(
    debtor_id: int,
    creditor_id: int,
    transfer_id: int,
    coordinator_type: str = "direct",
    coordinator_id: Optional[int] = None,
    coordinator_request_id: int = 1234,
) -> str:
    if coordinator_id is None:
        coordinator_id = creditor_id

    props = f"""
      "type": "PreparedTransfer",
      "debtor_id": {debtor_id},
      "creditor_id": {creditor_id},
      "transfer_id": {transfer_id},
      "coordinator_type": "{coordinator_type}",
      "coordinator_id": {coordinator_id},
      "coordinator_request_id": {coordinator_request_id},
      "locked_amount": 1000,
      "recipient": "RECIPIENT",
      "demurrage_rate": -10.0,
      "deadline": "2024-01-01T12:00:00+00:00",
      "final_interest_rate_ts": "9999-12-31T23:59:59+00:00",
      "prepared_at": "2023-01-01T11:00:00+00:00",
      "ts": "2023-01-01T12:00:00+00:00"
    """
    return "{" + props + "}"


def create_configure_account_msg(debtor_id: int, creditor_id: int) -> str:
    props = f"""
      "type": "ConfigureAccount",
      "debtor_id": {debtor_id},
      "creditor_id": {creditor_id},
      "negligible_amount": 1000.0,
      "config_flags": 666,
      "config_data": "TEST_CONFIG",
      "seqnum": 1234,
      "ts": "2023-01-01T12:00:00+00:00"
    """
    return "{" + props + "}"


@pytest.mark.asyncio
async def test_ca_drainer(datadir, rmq_url):
    loop = asyncio.get_running_loop()

    # Ensure client and server queues are configured.
    connection = await aio_pika.connect(rmq_url)
    channel = await connection.channel()
    client_queue = await channel.declare_queue("test_client")
    server_exchange = await channel.declare_exchange(
        "creditors_in", "headers", durable=True
    )
    drainer_test_queue = await channel.declare_queue("test_drainer")
    await drainer_test_queue.bind(server_exchange, "#")

    # Empty client and server queues.
    while await client_queue.get(no_ack=True, fail=False):
        pass
    while await drainer_test_queue.get(no_ack=True, fail=False):
        pass

    debtor_id = u64_to_i64(0xDEAC00ED00000001)
    creditor_id = 0x0000081000000001

    # Add test "PrepareTransfer "message.
    s = create_prepare_transfer_msg(debtor_id, creditor_id)
    message = aio_pika.Message(
        s.encode("utf8"),
        type="PrepareTransfer",
        content_type="application/json",
    )
    await channel.default_exchange.publish(message, "test_client")

    # Add two identical test "ConfigureAccount "messages.
    s = create_configure_account_msg(debtor_id, creditor_id)
    message = aio_pika.Message(
        s.encode("utf8"),
        type="ConfigureAccount",
        content_type="application/json",
    )
    await channel.default_exchange.publish(message, "test_client")
    await channel.default_exchange.publish(message, "test_client")

    drainer_task = loop.create_task(
        drainer.drain(
            protocol_broker_url=rmq_url,
            peer_node_id="deac00ed",
            nodedata_url=f'file://{datadir["CA"]}',
            protocol_broker_queue="test_client",
            client_queue_size=1,
            server_queue_size=1,
        )
    )

    async def read_messages():
        nonlocal messages_ok
        i = 0
        async with drainer_test_queue.iterator() as q:
            async for message in q:
                i += 1
                assert message.type == "RejectedConfig"
                assert message.content_type == "application/json"
                msg_data = json.loads(message.body.decode("utf8"))
                assert msg_data == {
                    "type": "RejectedConfig",
                    "creditor_id": creditor_id,
                    "debtor_id": debtor_id,
                    "negligible_amount": 1000.0,
                    "config_data": "TEST_CONFIG",
                    "config_flags": 666,
                    "config_seqnum": 1234,
                    "config_ts": "2023-01-01T12:00:00+00:00",
                    "rejection_code": "NO_CONNECTION_TO_DEBTOR",
                    "ts": msg_data["ts"],
                }
                assert message.headers == {
                    "message-type": "RejectedConfig",
                    "debtor-id": debtor_id,
                    "creditor-id": creditor_id,
                    "ca-creditors": True,
                    "ca-trade": False,
                }
                await message.ack()
                if i == 2:
                    break
        messages_ok = True
        drainer_task.cancel()

    messages_ok = False
    read_task = loop.create_task(read_messages())
    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(
            asyncio.gather(drainer_task, read_task),
            15.0,
        )

    assert messages_ok
    await channel.close()
    await connection.close()
    await asyncio.wait([drainer_task, read_task])


@pytest.mark.asyncio
async def test_ca_drainer_consume_error(datadir, rmq_url):
    loop = asyncio.get_running_loop()
    drainer_task = loop.create_task(
        drainer.drain(
            protocol_broker_url=rmq_url,
            peer_node_id="deac00ed",
            nodedata_url=f'file://{datadir["CA"]}',
            protocol_broker_queue="non_existent_queue",
            client_queue_size=100,
            server_queue_size=100,
        )
    )
    with pytest.raises(ServerError):
        await asyncio.wait_for(asyncio.gather(drainer_task), 15.0)


@pytest.mark.asyncio
async def test_aa_drainer_prepared_transfer(datadir, rmq_url):
    loop = asyncio.get_running_loop()

    # Ensure client and server queues are configured.
    connection = await aio_pika.connect(rmq_url)
    channel = await connection.channel()
    client_queue = await channel.declare_queue("test_client")
    server_exchange = await channel.declare_exchange("accounts_in", "topic")
    drainer_test_queue = await channel.declare_queue("test_drainer")
    await drainer_test_queue.bind(server_exchange, "#")

    # Empty client and server queues.
    while await client_queue.get(no_ack=True, fail=False):
        pass
    while await drainer_test_queue.get(no_ack=True, fail=False):
        pass

    debtor_id = u64_to_i64(0x1234abcd00000001)
    creditor_id = 0x0000020000000001
    transfer_id = 12345
    coordinator_id = 0x0000020000000002

    # Add test "PreparedTransfer "message.
    s = create_prepared_transfer_msg(
        debtor_id, creditor_id, transfer_id, 'agent', coordinator_id
    )
    message = aio_pika.Message(
        s.encode("utf8"),
        type="PreparedTransfer",
        content_type="application/json",
    )
    await channel.default_exchange.publish(message, "test_client")

    drainer_task = loop.create_task(
        drainer.drain(
            protocol_broker_url=rmq_url,
            peer_node_id="09cab36b6079e09c32a9dfd60446c469",
            nodedata_url=f'file://{datadir["AA"]}',
            protocol_broker_queue="test_client",
            client_queue_size=100,
            server_queue_size=100,
        )
    )

    async def read_messages():
        nonlocal messages_ok
        i = 0
        async with drainer_test_queue.iterator() as q:
            async for message in q:
                i += 1
                assert message.type == "FinalizeTransfer"
                assert message.content_type == "application/json"
                msg_data = json.loads(message.body.decode("utf8"))
                assert msg_data == {
                    "type": "FinalizeTransfer",
                    "creditor_id": creditor_id,
                    "debtor_id": debtor_id,
                    "committed_amount": 0,
                    "coordinator_id": coordinator_id,
                    "coordinator_request_id": 1234,
                    "coordinator_type": "agent",
                    "transfer_id": transfer_id,
                    "transfer_note": "",
                    "transfer_note_format": "",
                    "ts": msg_data["ts"],
                }
                assert message.headers == {
                    "message-type": "FinalizeTransfer",
                    "debtor-id": debtor_id,
                    "creditor-id": creditor_id,
                    "coordinator-id": coordinator_id,
                    "coordinator-type": "agent",
                }
                await message.ack()
                if i == 1:
                    break
        messages_ok = True
        drainer_task.cancel()

    messages_ok = False
    read_task = loop.create_task(read_messages())
    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(
            asyncio.gather(drainer_task, read_task),
            15.0,
        )

    assert messages_ok
    await channel.close()
    await connection.close()
    await asyncio.wait([drainer_task, read_task])


@pytest.mark.asyncio
async def test_aa_drainer_account_update(datadir, rmq_url):
    loop = asyncio.get_running_loop()

    # Ensure client and server queues are configured.
    connection = await aio_pika.connect(rmq_url)
    channel = await connection.channel()
    client_queue = await channel.declare_queue("test_client")
    server_exchange = await channel.declare_exchange("accounts_in", "topic")
    drainer_test_queue = await channel.declare_queue("test_drainer")
    await drainer_test_queue.bind(server_exchange, "#")

    # Empty client and server queues.
    while await client_queue.get(no_ack=True, fail=False):
        pass
    while await drainer_test_queue.get(no_ack=True, fail=False):
        pass

    debtor_id = u64_to_i64(0x1234abcd00000001)
    creditor_id = 0x0000020000000001

    # Add two identicaltest "AccountUpdate "messages.
    s = create_account_update_msg(debtor_id, creditor_id)
    message = aio_pika.Message(
        s.encode("utf8"),
        type="AccountUpdate",
        content_type="application/json",
    )
    await channel.default_exchange.publish(message, "test_client")
    await channel.default_exchange.publish(message, "test_client")

    drainer_task = loop.create_task(
        drainer.drain(
            protocol_broker_url=rmq_url,
            peer_node_id="09cab36b6079e09c32a9dfd60446c469",
            nodedata_url=f'file://{datadir["AA"]}',
            protocol_broker_queue="test_client",
            client_queue_size=1,
            server_queue_size=1,
        )
    )

    async def read_messages():
        nonlocal messages_ok
        i = 0
        async with drainer_test_queue.iterator() as q:
            async for message in q:
                i += 1
                assert message.type == "ConfigureAccount"
                assert message.content_type == "application/json"
                msg_data = json.loads(message.body.decode("utf8"))
                assert msg_data == {
                    "type": "ConfigureAccount",
                    "config_data": "",
                    "config_flags": 1,
                    "creditor_id": creditor_id,
                    "debtor_id": debtor_id,
                    "negligible_amount": 1e+30,
                    "seqnum": 0,
                    "ts": msg_data["ts"],
                }
                assert message.headers == {
                    "message-type": "ConfigureAccount",
                    "debtor-id": debtor_id,
                    "creditor-id": creditor_id,
                }
                await message.ack()
                if i == 2:
                    break
        messages_ok = True
        drainer_task.cancel()

    messages_ok = False
    read_task = loop.create_task(read_messages())
    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(
            asyncio.gather(drainer_task, read_task),
            15.0,
        )

    assert messages_ok
    await channel.close()
    await connection.close()
    await asyncio.wait([drainer_task, read_task])
