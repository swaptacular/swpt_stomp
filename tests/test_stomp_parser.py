import pytest
from swpt_stomp.stomp_parser import StompParser


def test_regexps():
    from swpt_stomp.stomp_parser import _HEAD_RE, _HEARTBEAT_RE

    assert _HEAD_RE.match(b"""123""") is None

    m = _HEAD_RE.match(b"""CONNECT\n\n""")
    assert len(m.groups()) == 3
    assert m[1] == b"CONNECT"
    assert m[2] == b""
    assert m[3] == b"\n"
    assert m.end() == 9

    m = _HEAD_RE.match(b"""CONNECT\nh1:v1\nh2:v2\n\n""")
    assert m[1] == b"CONNECT"
    assert m[2] == b"h1:v1\nh2:v2\n"
    assert m[3] == b"\n"

    m = _HEAD_RE.match(b"""CONNECT\r\nh1:v1\r\nh2:v2\r\n\r\n""")
    assert m[1] == b"CONNECT"
    assert m[2] == b"h1:v1\r\nh2:v2\r\n"
    assert m[3] == b"\n"

    m = _HEAD_RE.match(bytearray(b"""SEND\nkey:value\n\nboby\0"""))
    assert m[1] == b"SEND"
    assert m[2] == b"key:value\n"
    assert m[3] == b"\n"

    # Not obeying the protocol:
    assert _HEAD_RE.match(b"""CONNxxx""") is None
    assert _HEAD_RE.match(b"""CONNECT\nWRONGHEADER\n\n""") is None
    assert _HEAD_RE.match(b"""CONNECT\naa:bb\n:""") is None
    assert _HEAD_RE.match(b"""CONNECT\naa:bb\n:""") is None

    # Incomplete head:
    assert _HEAD_RE.match(b"")[3] is None
    assert _HEAD_RE.match(b"CONNECT")[3] is None
    assert _HEAD_RE.match(b"CONNECT\r")[3] is None
    assert _HEAD_RE.match(b"CONNECT\r\n")[3] is None
    assert _HEAD_RE.match(b"CONNECT\n")[3] is None
    assert _HEAD_RE.match(b"CONNECT\r\na")[3] is None
    assert _HEAD_RE.match(b"CONNECT\na:b")[3] is None
    assert _HEAD_RE.match(b"CONNECT\na:b\r")[3] is None
    assert _HEAD_RE.match(b"CONNECT\na:b\r\n")[3] is None
    assert _HEAD_RE.match(b"CONNECT\na:b\n")[3] is None
    assert _HEAD_RE.match(b"CONNECT\na:b\r\naa")[3] is None
    assert _HEAD_RE.match(b"CONNECT\na:b\r\naa:bb\r\n")[3] is None
    assert _HEAD_RE.match(b"CONNECT\na:b\naa:bb\n")[3] is None
    assert _HEAD_RE.match(b"CONNECT\na:b\naa:bb\n\r")[3] is None
    assert _HEAD_RE.match(b"CONNECT\na:b\naa:bb\n\r\n")[3] == b'\n'
    assert _HEAD_RE.match(b"CONNECT\na:b\naa:bb\n\n")[3] == b'\n'

    m = _HEAD_RE.match(b"""CONNECT\nh1:v1\nh2:v2""")
    assert m[1] == b"CONNECT"
    assert m[2] == b"h1:v1\n"
    assert m[3] is None

    assert _HEARTBEAT_RE.match(b'123') is None
    assert _HEARTBEAT_RE.match(b'\n')
    assert _HEARTBEAT_RE.match(b'\n\n')
    m = _HEARTBEAT_RE.match(b'\r\n\r\n')
    assert m.end() == 4


def test_parse_headers():
    from swpt_stomp.stomp_parser import _HEAD_RE, _parse_headers, ProtocolError

    m = _HEAD_RE.match(b"""CONNECT\nkey\\n\\c:value\\r\\\n\n""")
    headers = _parse_headers(m[2])
    assert headers['key\n:'] == 'value\r\\'
    assert m[3] == b"\n"

    m = _HEAD_RE.match(b"""CONNECT\nkey\\t:value\n\n""")
    with pytest.raises(ProtocolError):
        _parse_headers(m[2])


def test_add_all_bytes_at_once():
    p = StompParser()
    assert not p.has_frame()
    p.add_bytes(b"SEND\nkey:value\n\nboby\0")
    assert p.has_frame()
