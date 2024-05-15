import pytest
import math
from marshmallow import ValidationError
from datetime import datetime, date
from swpt_stomp import smp_schemas as ps


def test_configure_account():
    s = ps.ConfigureAccountMessageSchema()

    data = s.loads(
        """{
    "type": "ConfigureAccount",
    "creditor_id": 1,
    "debtor_id": 2,
    "negligible_amount": 3.14,
    "config_data": "test config data",
    "config_flags": 128,
    "seqnum": 0,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }"""
    )

    assert data["type"] == "ConfigureAccount"
    assert data["creditor_id"] == 1
    assert type(data["creditor_id"]) is int
    assert data["debtor_id"] == 2
    assert type(data["debtor_id"]) is int
    assert data["negligible_amount"] == 3.14
    assert data["config_data"] == "test config data"
    assert data["config_flags"] == 128
    assert type(data["config_flags"]) is int
    assert data["seqnum"] == 0
    assert type(data["seqnum"]) is int
    assert data["ts"] == datetime.fromisoformat("2022-01-01T00:00:00+00:00")
    assert "unknown" not in data

    wrong_type = data.copy()
    wrong_type["type"] = "WrongType"
    wrong_type = s.dumps(wrong_type)
    with pytest.raises(ValidationError, match="Invalid type."):
        s.loads(wrong_type)

    wrong_config_data = data.copy()
    wrong_config_data["config_data"] = 1500 * "Щ"
    wrong_config_data = s.dumps(wrong_config_data)
    with pytest.raises(
        ValidationError, match="The length of config_data exceeds 2000 bytes"
    ):
        s.loads(wrong_config_data)

    with pytest.raises(ValidationError, match="Invalid input type"):
        s.loads("1")

    with pytest.raises(ValidationError, match="Invalid input type"):
        s.loads("1.0")

    with pytest.raises(ValidationError, match="Invalid input type"):
        s.loads("false")

    with pytest.raises(ValidationError, match="Invalid input type"):
        s.loads('"text"')

    with pytest.raises(ValidationError, match="Invalid input type"):
        s.loads("null")

    with pytest.raises(ValidationError, match="Invalid input type"):
        s.loads("[]")

    try:
        s.loads("{}")
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(
            m == ["Missing data for required field."]
            for m in e.messages.values()
        )


def test_configure_account_infinity():
    s = ps.ConfigureAccountMessageSchema()

    with pytest.raises(
        ValidationError,
        match=r"Special numeric values \(nan "
        r"or infinity\) are not permitted",
    ):
        s.loads(
            """{
        "type": "ConfigureAccount",
        "creditor_id": 1,
        "debtor_id": 2,
        "negligible_amount": 1e500,
        "config_data": "test config data",
        "config_flags": 128,
        "seqnum": 0,
        "ts": "2022-01-01T00:00:00Z",
        "unknown": "ignored"
        }"""
        )

    with pytest.raises(
        ValidationError,
        match=r"Special numeric values \(nan "
        r"or infinity\) are not permitted",
    ):
        s.load(
            {
                "type": "ConfigureAccount",
                "creditor_id": 1,
                "debtor_id": 2,
                "negligible_amount": math.inf,
                "config_data": "test config data",
                "config_flags": 128,
                "seqnum": 0,
                "ts": "2022-01-01T00:00:00Z",
                "unknown": "ignored",
            }
        )


def test_rejected_config():
    s = ps.RejectedConfigMessageSchema()

    data = s.loads(
        """{
    "type": "RejectedConfig",
    "creditor_id": -1,
    "debtor_id": -2,
    "negligible_amount": 0,
    "config_data": "test config data",
    "config_flags": -128,
    "config_seqnum": 2147483647,
    "config_ts": "2022-01-01T00:00:00+00:00",
    "rejection_code": "ERROR2",
    "ts": "2022-01-02T00:00:00Z",
    "unknown": "ignored"
    }"""
    )

    assert data["type"] == "RejectedConfig"
    assert data["creditor_id"] == -1
    assert data["debtor_id"] == -2
    assert data["negligible_amount"] == 0
    assert type(data["negligible_amount"]) is float
    assert data["config_data"] == "test config data"
    assert data["config_flags"] == -128
    assert type(data["config_flags"]) is int
    assert data["config_seqnum"] == 2147483647
    assert type(data["config_seqnum"]) is int
    assert data["config_ts"] == datetime.fromisoformat(
        "2022-01-01T00:00:00+00:00"
    )
    assert data["rejection_code"] == "ERROR2"
    assert data["ts"] == datetime.fromisoformat("2022-01-02T00:00:00+00:00")
    assert "unknown" not in data

    wrong_rejection_code = data.copy()
    wrong_rejection_code["rejection_code"] = "Кирилица"
    wrong_rejection_code = s.dumps(wrong_rejection_code)
    with pytest.raises(
        ValidationError,
        match="The rejection_code field contains non-ASCII characters",
    ):
        s.loads(wrong_rejection_code)

    wrong_config_data = data.copy()
    wrong_config_data["config_data"] = 1500 * "Щ"
    wrong_config_data = s.dumps(wrong_config_data)
    with pytest.raises(
        ValidationError, match="The length of config_data exceeds 2000 bytes"
    ):
        s.loads(wrong_config_data)

    try:
        s.loads("{}")
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(
            m == ["Missing data for required field."]
            for m in e.messages.values()
        )


def test_prepare_transfer():
    s = ps.PrepareTransferMessageSchema()

    data = s.loads(
        """{
    "type": "PrepareTransfer",
    "creditor_id": -1000000000000000,
    "debtor_id": -2000000000000000,
    "coordinator_type": "direct",
    "coordinator_id": -1000000000000000,
    "coordinator_request_id": 123456789012345,
    "min_locked_amount": 1000000000000,
    "max_locked_amount": 2000000000000,
    "recipient": "test recipient",
    "final_interest_rate_ts": "9999-12-31T23:59:59+00:00",
    "max_commit_delay": 2147483647,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }"""
    )

    assert data["type"] == "PrepareTransfer"
    assert data["creditor_id"] == -1000000000000000
    assert data["debtor_id"] == -2000000000000000
    assert data["coordinator_type"] == "direct"
    assert data["coordinator_id"] == -1000000000000000
    assert type(data["coordinator_id"]) is int
    assert data["coordinator_request_id"] == 123456789012345
    assert type(data["coordinator_request_id"]) is int
    assert data["min_locked_amount"] == 1000000000000
    assert type(data["min_locked_amount"]) is int
    assert data["max_locked_amount"] == 2000000000000
    assert type(data["max_locked_amount"]) is int
    assert data["recipient"] == "test recipient"
    assert data["final_interest_rate_ts"] == (
        datetime.fromisoformat("9999-12-31T23:59:59+00:00")
    )
    assert data["max_commit_delay"] == 2147483647
    assert type(data["max_commit_delay"]) is int
    assert data["ts"] == datetime.fromisoformat("2022-01-01T00:00:00+00:00")
    assert "unknown" not in data

    wrong_max_locked_amount = data.copy()
    wrong_max_locked_amount["max_locked_amount"] = 0
    wrong_max_locked_amount = s.dumps(wrong_max_locked_amount)
    with pytest.raises(
        ValidationError,
        match=(
            "max_locked_amount must be equal or greater than min_locked_amount"
        ),
    ):
        s.loads(wrong_max_locked_amount)

    wrong_recipient = data.copy()
    wrong_recipient["recipient"] = "Кирилица"
    wrong_recipient = s.dumps(wrong_recipient)
    with pytest.raises(
        ValidationError,
        match="The recipient field contains non-ASCII characters",
    ):
        s.loads(wrong_recipient)

    issuing = data.copy()
    issuing["coordinator_type"] = "issuing"
    issuing["coordinator_id"] = data["debtor_id"]
    issuing["creditor_id"] = 0
    s.loads(s.dumps(issuing))

    other_coordinator = data.copy()
    other_coordinator["coordinator_type"] = "other"
    other_coordinator["coordinator_id"] = 1111111111111111111
    s.loads(s.dumps(other_coordinator))

    s.loads(s.dumps(issuing))
    wrong_direct_coordinator_id = data.copy()
    wrong_direct_coordinator_id["coordinator_type"] = "direct"
    wrong_direct_coordinator_id["coordinator_id"] = 11111111111111111
    wrong_direct_coordinator_id = s.dumps(wrong_direct_coordinator_id)
    with pytest.raises(
        ValidationError, match="Invalid coordinator_id for direct transfer."
    ):
        s.loads(wrong_direct_coordinator_id)

    wrong_issuing_coordinator_id = data.copy()
    wrong_issuing_coordinator_id["coordinator_type"] = "issuing"
    wrong_issuing_coordinator_id["coordinator_id"] = 11111111111111111
    wrong_issuing_coordinator_id = s.dumps(wrong_issuing_coordinator_id)
    with pytest.raises(
        ValidationError, match="Invalid coordinator_id for issuing transfer."
    ):
        s.loads(wrong_issuing_coordinator_id)

    wrong_issuing_creditor_id = data.copy()
    wrong_issuing_creditor_id["coordinator_type"] = "issuing"
    wrong_issuing_creditor_id["coordinator_id"] = data["debtor_id"]
    wrong_issuing_creditor_id["creditor_id"] = 11111111111111111
    wrong_issuing_creditor_id = s.dumps(wrong_issuing_creditor_id)
    with pytest.raises(
        ValidationError,
        match="Invalid sender creditor_id for issuing transfer.",
    ):
        s.loads(wrong_issuing_creditor_id)

    try:
        s.loads("{}")
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(
            m == ["Missing data for required field."]
            for m in e.messages.values()
        )


def test_prepared_transfer():
    s = ps.PreparedTransferMessageSchema()

    data = s.loads(
        """{
    "type": "PreparedTransfer",
    "creditor_id": -1000000000000000,
    "debtor_id": -2000000000000000,
    "transfer_id": -3000000000000000,
    "coordinator_type": "direct",
    "coordinator_id": 1111111111111111,
    "coordinator_request_id": 123456789012345,
    "locked_amount": 1230000000000,
    "recipient": "test recipient",
    "prepared_at": "2022-01-01T00:00:00Z",
    "demurrage_rate": -5.5e0,
    "deadline": "2022-02-01T00:00:00Z",
    "final_interest_rate_ts": "9999-12-31T23:59:59+00:00",
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }"""
    )

    assert data["type"] == "PreparedTransfer"
    assert data["creditor_id"] == -1000000000000000
    assert data["debtor_id"] == -2000000000000000
    assert data["transfer_id"] == -3000000000000000
    assert type(data["transfer_id"]) is int
    assert data["coordinator_type"] == "direct"
    assert data["coordinator_id"] == 1111111111111111
    assert data["coordinator_request_id"] == 123456789012345
    assert data["locked_amount"] == 1230000000000
    assert type(data["locked_amount"]) is int
    assert data["recipient"] == "test recipient"
    assert data["prepared_at"] == datetime.fromisoformat(
        "2022-01-01T00:00:00+00:00"
    )
    assert data["demurrage_rate"] == -5.5
    assert data["deadline"] == datetime.fromisoformat(
        "2022-02-01T00:00:00+00:00"
    )
    assert data["final_interest_rate_ts"] == (
        datetime.fromisoformat("9999-12-31T23:59:59+00:00")
    )
    assert data["ts"] == datetime.fromisoformat("2022-01-01T00:00:00+00:00")
    assert "unknown" not in data

    wrong_recipient = data.copy()
    wrong_recipient["recipient"] = "Кирилица"
    wrong_recipient = s.dumps(wrong_recipient)
    with pytest.raises(
        ValidationError,
        match="The recipient field contains non-ASCII characters",
    ):
        s.loads(wrong_recipient)

    try:
        s.loads("{}")
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(
            m == ["Missing data for required field."]
            for m in e.messages.values()
        )


def test_finalize_transfer():
    s = ps.FinalizeTransferMessageSchema()

    data = s.loads(
        """{
    "type": "FinalizeTransfer",
    "creditor_id": -1000000000000000,
    "debtor_id": -2000000000000000,
    "transfer_id": -3000000000000000,
    "coordinator_type": "direct",
    "coordinator_id": 1111111111111111,
    "coordinator_request_id": 123456789012345,
    "committed_amount": 1230000000000,
    "transfer_note": "test note",
    "transfer_note_format": "",
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }"""
    )

    assert data["type"] == "FinalizeTransfer"
    assert data["creditor_id"] == -1000000000000000
    assert data["debtor_id"] == -2000000000000000
    assert data["transfer_id"] == -3000000000000000
    assert type(data["transfer_id"]) is int
    assert data["coordinator_type"] == "direct"
    assert data["coordinator_id"] == 1111111111111111
    assert data["coordinator_request_id"] == 123456789012345
    assert data["committed_amount"] == 1230000000000
    assert type(data["committed_amount"]) is int
    assert data["transfer_note"] == "test note"
    assert data["transfer_note_format"] == ""
    assert data["ts"] == datetime.fromisoformat("2022-01-01T00:00:00+00:00")
    assert "unknown" not in data

    wrong_transfer_note = data.copy()
    wrong_transfer_note["transfer_note"] = 350 * "Щ"
    wrong_transfer_note = s.dumps(wrong_transfer_note)
    with pytest.raises(
        ValidationError, match="The length of transfer_note exceeds 500 bytes"
    ):
        s.loads(wrong_transfer_note)

    try:
        s.loads("{}")
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(
            m == ["Missing data for required field."]
            for m in e.messages.values()
        )


def test_finalized_transfer():
    s = ps.FinalizedTransferMessageSchema()

    data = s.loads(
        """{
    "type": "FinalizedTransfer",
    "creditor_id": -1000000000000000,
    "debtor_id": -2000000000000000,
    "transfer_id": -3000000000000000,
    "coordinator_type": "direct",
    "coordinator_id": 1111111111111111,
    "coordinator_request_id": 123456789012345,
    "committed_amount": 1230000000000,
    "status_code": "OK",
    "total_locked_amount": 0,
    "prepared_at": "2022-01-01T00:00:00Z",
    "ts": "2022-01-01T00:00:05Z",
    "unknown": "ignored"
    }"""
    )

    assert data["type"] == "FinalizedTransfer"
    assert data["creditor_id"] == -1000000000000000
    assert data["debtor_id"] == -2000000000000000
    assert data["transfer_id"] == -3000000000000000
    assert type(data["transfer_id"]) is int
    assert data["coordinator_type"] == "direct"
    assert data["coordinator_id"] == 1111111111111111
    assert data["coordinator_request_id"] == 123456789012345
    assert data["committed_amount"] == 1230000000000
    assert type(data["committed_amount"]) is int
    assert data["status_code"] == "OK"
    assert data["total_locked_amount"] == 0
    assert type(data["total_locked_amount"]) is int
    assert data["prepared_at"] == datetime.fromisoformat(
        "2022-01-01T00:00:00+00:00"
    )
    assert data["ts"] == datetime.fromisoformat("2022-01-01T00:00:05+00:00")
    assert "unknown" not in data

    wrong_status_code1 = data.copy()
    wrong_status_code1["status_code"] = "NOT OK"
    wrong_status_code1 = s.dumps(wrong_status_code1)
    with pytest.raises(
        ValidationError,
        match='The committed_amount must be zero when status_code is not "OK"',
    ):
        s.loads(wrong_status_code1)

    wrong_status_code2 = data.copy()
    wrong_status_code2["status_code"] = "Кирилица"
    wrong_status_code2 = s.dumps(wrong_status_code2)
    with pytest.raises(
        ValidationError,
        match="The status_code field contains non-ASCII characters",
    ):
        s.loads(wrong_status_code2)

    try:
        s.loads("{}")
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(
            m == ["Missing data for required field."]
            for m in e.messages.values()
        )


def test_rejected_transfer():
    s = ps.RejectedTransferMessageSchema()

    data = s.loads(
        """{
    "type": "RejectedTransfer",
    "creditor_id": 1000000000000000,
    "debtor_id": 2000000000000000,
    "coordinator_type": "direct",
    "coordinator_id": 11,
    "coordinator_request_id": 1234,
    "status_code": "ERROR1",
    "total_locked_amount": 0,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }"""
    )

    assert data["type"] == "RejectedTransfer"
    assert data["creditor_id"] == 1000000000000000
    assert data["debtor_id"] == 2000000000000000
    assert data["coordinator_type"] == "direct"
    assert data["coordinator_id"] == 11
    assert data["coordinator_request_id"] == 1234
    assert data["status_code"] == "ERROR1"
    assert data["total_locked_amount"] == 0
    assert type(data["total_locked_amount"]) is int
    assert data["ts"] == datetime.fromisoformat("2022-01-01T00:00:00+00:00")
    assert "unknown" not in data

    wrong_status_code1 = data.copy()
    wrong_status_code1["status_code"] = "OK"
    wrong_status_code1 = s.dumps(wrong_status_code1)
    with pytest.raises(
        ValidationError,
        match="The status_code field contains an invalid value",
    ):
        s.loads(wrong_status_code1)

    wrong_status_code2 = data.copy()
    wrong_status_code2["status_code"] = "Кирилица"
    wrong_status_code2 = s.dumps(wrong_status_code2)
    with pytest.raises(
        ValidationError,
        match="The status_code field contains non-ASCII characters",
    ):
        s.loads(wrong_status_code2)

    wrong_coordinator_type = data.copy()
    wrong_coordinator_type["coordinator_type"] = "Кирилица"
    wrong_coordinator_type = s.dumps(wrong_coordinator_type)
    with pytest.raises(
        ValidationError,
        match="The coordinator_type field contains non-ASCII characters",
    ):
        s.loads(wrong_coordinator_type)

    empty_coordinator_type = data.copy()
    empty_coordinator_type["coordinator_type"] = ""
    empty_coordinator_type = s.dumps(empty_coordinator_type)
    with pytest.raises(ValidationError, match="Length must be between 1 and"):
        s.loads(empty_coordinator_type)

    try:
        s.loads("{}")
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(
            m == ["Missing data for required field."]
            for m in e.messages.values()
        )


def test_account_purge():
    s = ps.AccountPurgeMessageSchema()

    data = s.loads(
        """{
    "type": "AccountPurge",
    "creditor_id": 1000000000000000,
    "debtor_id": 2000000000000000,
    "creation_date": "2021-01-30",
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }"""
    )

    assert data["type"] == "AccountPurge"
    assert data["creditor_id"] == 1000000000000000
    assert data["debtor_id"] == 2000000000000000
    assert data["creation_date"] == date(2021, 1, 30)
    assert data["ts"] == datetime.fromisoformat("2022-01-01T00:00:00+00:00")
    assert "unknown" not in data

    try:
        s.loads("{}")
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(
            m == ["Missing data for required field."]
            for m in e.messages.values()
        )


def test_account_update():
    s = ps.AccountUpdateMessageSchema()

    data_str = """{
    "type": "AccountUpdate",
    "creditor_id": 1000000000000000,
    "debtor_id": 2000000000000000,
    "creation_date": "2021-01-30",
    "last_change_ts": "2022-01-01T00:00:01Z",
    "last_change_seqnum": 1,
    "principal": 12340000000000,
    "interest": -1e6,
    "interest_rate": -7.7,
    "last_interest_rate_change_ts": "2022-01-01T00:00:02Z",
    "last_config_ts": "2022-01-01T00:00:03Z",
    "last_config_seqnum": -1,
    "negligible_amount": 3.14,
    "config_flags": 64,
    "config_data": "test config data",
    "account_id": "test account",
    "debtor_info_iri": "https://example.com/",
    "debtor_info_content_type": "text/plain",
    "debtor_info_sha256": "64*f",
    "last_transfer_number": 0,
    "last_transfer_committed_at": "2022-01-01T00:00:04Z",
    "demurrage_rate": -6.28,
    "commit_period": 86400,
    "transfer_note_max_bytes": 100,
    "ts": "2022-01-01T00:00:00Z",
    "ttl": 100000,
    "unknown": "ignored"
    }"""
    data_str = data_str.replace("64*f", 64 * "f")
    data = s.loads(data_str)

    assert data["type"] == "AccountUpdate"
    assert data["creditor_id"] == 1000000000000000
    assert data["debtor_id"] == 2000000000000000
    assert data["creation_date"] == date(2021, 1, 30)
    assert data["last_change_ts"] == datetime.fromisoformat(
        "2022-01-01T00:00:01+00:00"
    )
    assert data["last_change_seqnum"] == 1
    assert data["principal"] == 12340000000000
    assert data["interest"] == -1e6
    assert data["interest_rate"] == -7.7
    assert data["last_interest_rate_change_ts"] == datetime.fromisoformat(
        "2022-01-01T00:00:02+00:00"
    )
    assert data["last_config_ts"] == datetime.fromisoformat(
        "2022-01-01T00:00:03+00:00"
    )
    assert data["last_config_seqnum"] == -1
    assert data["negligible_amount"] == 3.14
    assert data["config_flags"] == 64
    assert data["config_data"] == "test config data"
    assert data["account_id"] == "test account"
    assert data["debtor_info_iri"] == "https://example.com/"
    assert data["debtor_info_content_type"] == "text/plain"
    assert data["debtor_info_sha256"] == 32 * "ff"
    assert data["last_transfer_number"] == 0
    assert data["last_transfer_committed_at"] == datetime.fromisoformat(
        "2022-01-01T00:00:04+00:00"
    )
    assert data["demurrage_rate"] == -6.28
    assert data["commit_period"] == 86400
    assert data["transfer_note_max_bytes"] == 100
    assert data["ts"] == datetime.fromisoformat("2022-01-01T00:00:00+00:00")
    assert data["ttl"] == 100000
    assert "unknown" not in data

    wrong_config_data = data.copy()
    wrong_config_data["config_data"] = 1500 * "Щ"
    wrong_config_data = s.dumps(wrong_config_data)
    with pytest.raises(
        ValidationError, match="The length of config_data exceeds 2000 bytes"
    ):
        s.loads(wrong_config_data)

    wrong_account_id = data.copy()
    wrong_account_id["account_id"] = "Кирилица"
    wrong_account_id = s.dumps(wrong_account_id)
    with pytest.raises(
        ValidationError,
        match="The account_id field contains non-ASCII characters",
    ):
        s.loads(wrong_account_id)

    wrong_content_type = data.copy()
    wrong_content_type["debtor_info_content_type"] = "Кирилица"
    wrong_content_type = s.dumps(wrong_content_type)
    with pytest.raises(
        ValidationError,
        match=(
            "The debtor_info_content_type field contains non-ASCII characters"
        ),
    ):
        s.loads(wrong_content_type)

    try:
        s.loads("{}")
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(
            m == ["Missing data for required field."]
            for m in e.messages.values()
        )


def test_account_transfer():
    s = ps.AccountTransferMessageSchema()

    data = s.loads(
        """{
    "type": "AccountTransfer",
    "creditor_id": -1000000000000000,
    "debtor_id": -2000000000000000,
    "creation_date": "2021-01-30",
    "transfer_number": 333333333333333,
    "coordinator_type": "direct",
    "sender": "test sender",
    "recipient": "test recipient",
    "acquired_amount": 1230000000000,
    "transfer_note": "test note",
    "transfer_note_format": "",
    "committed_at": "2022-01-01T00:00:01Z",
    "principal": -100000000000000,
    "ts": "2022-01-01T00:00:00Z",
    "previous_transfer_number": 333333333333332,
    "unknown": "ignored"
    }"""
    )

    assert data["type"] == "AccountTransfer"
    assert data["creditor_id"] == -1000000000000000
    assert data["debtor_id"] == -2000000000000000
    assert data["creation_date"] == date(2021, 1, 30)
    assert data["transfer_number"] == 333333333333333
    assert type(data["transfer_number"]) is int
    assert data["coordinator_type"] == "direct"
    assert data["sender"] == "test sender"
    assert data["recipient"] == "test recipient"
    assert data["acquired_amount"] == 1230000000000
    assert type(data["acquired_amount"]) is int
    assert data["transfer_note"] == "test note"
    assert data["transfer_note_format"] == ""
    assert data["committed_at"] == datetime.fromisoformat(
        "2022-01-01T00:00:01+00:00"
    )
    assert data["principal"] == -100000000000000
    assert type(data["principal"]) is int
    assert data["ts"] == datetime.fromisoformat("2022-01-01T00:00:00+00:00")
    assert data["previous_transfer_number"] == 333333333333332
    assert "unknown" not in data

    wrong_acquired_amount = data.copy()
    wrong_acquired_amount["acquired_amount"] = 0
    wrong_acquired_amount = s.dumps(wrong_acquired_amount)
    with pytest.raises(
        ValidationError,
        match="The acquired_amount field is zero, which is not allowed",
    ):
        s.loads(wrong_acquired_amount)

    wrong_transfer_note = data.copy()
    wrong_transfer_note["transfer_note"] = 350 * "Щ"
    wrong_transfer_note = s.dumps(wrong_transfer_note)
    with pytest.raises(
        ValidationError, match="The length of transfer_note exceeds 500 bytes"
    ):
        s.loads(wrong_transfer_note)

    wrong_sender = data.copy()
    wrong_sender["sender"] = "Кирилица"
    wrong_sender = s.dumps(wrong_sender)
    with pytest.raises(
        ValidationError, match="The sender field contains non-ASCII characters"
    ):
        s.loads(wrong_sender)

    wrong_recipient = data.copy()
    wrong_recipient["recipient"] = "Кирилица"
    wrong_recipient = s.dumps(wrong_recipient)
    with pytest.raises(
        ValidationError,
        match="The recipient field contains non-ASCII characters",
    ):
        s.loads(wrong_recipient)

    wrong_transfer_number = data.copy()
    wrong_transfer_number["transfer_number"] = 333333333333332
    wrong_transfer_number = s.dumps(wrong_transfer_number)
    with pytest.raises(
        ValidationError,
        match="transfer_number must be greater than previous_transfer_number",
    ):
        s.loads(wrong_transfer_number)

    try:
        s.loads("{}")
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(
            m == ["Missing data for required field."]
            for m in e.messages.values()
        )
