import pytest
from swpt_stomp.stomp_parser import HEAD_RE, HEARTBEAT_RE, parse_headers, ProtocolError


def test_regexps():
    assert HEAD_RE.match(b"""123""") is None

    m = HEAD_RE.match(b"""CONNECT\n\n""")
    assert m[1] == b"CONNECT"
    assert m[2] == b""
    assert m.end() == 9

    m = HEAD_RE.match(b"""CONNECT\nh1:v1\nh2:v2\n\n""")
    assert m[1] == b"CONNECT"
    assert m[2] == b"h1:v1\nh2:v2\n"

    m = HEAD_RE.match(b"""CONNECT\nkey\\n\\c:value\\r\\\n\n""")
    headers = parse_headers(m[2])
    assert headers['key\n:'] == 'value\r\\'

    m = HEAD_RE.match(b"""CONNECT\nkey\\t:value\n\n""")
    with pytest.raises(ProtocolError):
        parse_headers(m[2])

    assert HEAD_RE.match(b"""CONNECT\nWRONGHEADER\n\n""") is None

    m = HEAD_RE.match(b"""CONNECT\r\nh1:v1\r\nh2:v2\r\n\r\n""")
    assert m[1] == b"CONNECT"
    assert m[2] == b"h1:v1\r\nh2:v2\r\n"

    assert HEARTBEAT_RE.match(b'123') is None
    assert HEARTBEAT_RE.match(b'\n')
    assert HEARTBEAT_RE.match(b'\n\n')
    m = HEARTBEAT_RE.match(b'\r\n\r\n')
    assert m.end() == 4
