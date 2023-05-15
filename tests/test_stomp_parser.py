import pytest
from swpt_stomp.stomp_parser import StompParser, EmptyQueue, ProtocolError


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

    m = _HEAD_RE.match(bytearray(b"""SEND\nkey:value\n\nbody\0"""))
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

    m = _HEAD_RE.match(b"""CONNECT\naa:bb\r\naaaa:bbbb\r\n\r\n""")
    headers = _parse_headers(m[2])
    assert headers['aa'] == 'bb'
    assert headers['aaaa'] == 'bbbb'
    assert m[3] == b"\n"

    m = _HEAD_RE.match(b"""CONNECT\nkey\\n\\c:value\\r\\\n\n""")
    headers = _parse_headers(m[2])
    assert headers['key\n:'] == 'value\r\\'
    assert m[3] == b"\n"

    m = _HEAD_RE.match(b"""CONNECT\nkey\\t:value\n\n""")
    with pytest.raises(ProtocolError):
        _parse_headers(m[2])


def test_add_all_bytes_at_once():
    p = StompParser()
    assert len(p.frames) == 0
    assert p.has_frame() is False
    p.add_bytes(b"SEND\nkey:value\n\nbody\0")
    assert len(p.frames) == 1
    assert p.has_frame() is True
    frame = p.pop_frame()
    assert frame.command == 'SEND'
    assert len(frame.headers) == 1
    assert frame.headers['key'] == 'value'
    assert frame.body == bytearray(b'body')
    assert len(p.frames) == 0
    assert p.has_frame() is False

    with pytest.raises(EmptyQueue):
        p.pop_frame()
    assert len(p.frames) == 0
    assert p.has_frame() is False


def test_add_bytes_one_by_one():
    message = b"SEND\nkey:value\n\nbody\0"
    p = StompParser()
    for i in message:
        assert not p.has_frame()
        p.add_bytes(bytes([i]))
    assert len(p.frames) == 1
    frame = p.frames[0]
    assert frame.command == 'SEND'
    assert len(frame.headers) == 1
    assert frame.headers['key'] == 'value'
    assert frame.body == bytearray(b'body')

    p.add_bytes(b"SEND\nkey:value\n\nbody\0")
    assert len(p.frames) == 2


def test_heartbeats():
    heartbeats = b"\n\n\r\n"
    message = b"SEND\nkey:value\n\nbody\0"
    p = StompParser()
    p.add_bytes(heartbeats)
    p.add_bytes(heartbeats)
    assert len(p.frames) == 0
    p.add_bytes(message)
    assert len(p.frames) == 1
    p.add_bytes(heartbeats)
    p.add_bytes(heartbeats)
    assert len(p.frames) == 1
    p.add_bytes(message)
    assert len(p.frames) == 2
    p.add_bytes(message)
    assert len(p.frames) == 3
    p.add_bytes(heartbeats)
    assert len(p.frames) == 3


def test_reset():
    p = StompParser()
    p.add_bytes(b"SEND\nkey:value\n\nbody\0")
    assert len(p.frames) == 1
    p.reset()
    assert len(p.frames) == 0


def test_protocol_error():
    p = StompParser()
    with pytest.raises(ProtocolError):
        p.add_bytes(b"error")


def test_content_length_too_big():
    p = StompParser()
    p.add_bytes(b"SEND\ncontent-length:4\n\nbody\0")

    with pytest.raises(ProtocolError):
        p.add_bytes(b"SEND\ncontent-length:400000\n\nbody\0")


def test_body_is_too_large():
    from swpt_stomp.stomp_parser import BODY_MAX_LENGTH

    p = StompParser()
    body_ok = BODY_MAX_LENGTH * b"x"
    p.add_bytes(b"SEND\n\n" + body_ok + b"\0")
    assert p.pop_frame().body == body_ok

    with pytest.raises(ProtocolError):
        p.add_bytes(b"SEND\n\n" + body_ok + b'x' + b"\0")

    p.reset()
    p.add_bytes(b"SEND\n\n" + body_ok)
    with pytest.raises(ProtocolError):
        p.add_bytes(b'x')


def test_body_containing_null():
    p = StompParser()
    p.add_bytes(b"SEND\ncontent-length:18\n\nThere is a \0 byte!\0")
    assert p.pop_frame().body == b'There is a \0 byte!'

    # Smaller content-length, still OK:
    p.add_bytes(b"SEND\ncontent-length:17\n\nThere is a \0 byte!\0")
    assert p.pop_frame().body == b'There is a \0 byte!'

    # Smaller content-length, still not OK:
    with pytest.raises(ProtocolError):
        p.add_bytes(b"SEND\ncontent-length:7\n\nThere is a \0 byte!\0")
    assert p.pop_frame().body == b'There is a '
