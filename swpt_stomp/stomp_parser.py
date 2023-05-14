from __future__ import annotations
from dataclasses import dataclass
from collections import deque
import re

HEARTBEAT_RE = re.compile(
    rb"""\A(?:\r?\n)+       # empty lines""",
    re.VERBOSE)

HEAD_RE = re.compile(
    rb"""\A(?:\Z|
    ([A-Z]{1,50})(?:\Z|                                       # command
    \r?(?:\Z|                                                 # optional \r
    \n((?:[^\n\r:]{1,500}:[^\n\r:]{0,500}\r?\n){0,50})(?:\Z|  # header lines
      # an incomplete header line, or a closing line:
      (?:
        # an incomplete header line:
        [^\n\r:]{1,500}(?::[^\n\r:]{0,500}\r?)?\Z
      ) | (?:
        # a closing line:
        \r?(?:\Z|(\n))
      )
    ))))""",
    re.VERBOSE)

HEADER_ESCAPE_RE = re.compile(rb'\\.')
HEADER_ESCAPE_CHARS = {
    rb'\r': b'\r',
    rb'\n': b'\n',
    rb'\c': b':',
    rb'\\': b'\\',
}

BODY_MAX_LENGTH = 50_000


class ProtocolError(Exception):
    """Protocol error"""


def substitute_header_escape_chars(s: bytes) -> bytes:
    # \r, \n, \c, \\ in headers should be substituted accordingly. Other
    # escape symbols are not allowed.
    try:
        return HEADER_ESCAPE_RE.sub(lambda m: HEADER_ESCAPE_CHARS[m[0]], s)
    except KeyError:
        raise ProtocolError('invalid header')


def parse_headers(s: bytes) -> dict[str, str]:
    headers = {}
    lines = s.split(b'\n')
    for line in lines:
        if line.endswith(b'\r'):
            line = line[:-1]
        if not line:
            break
        k_bytes, v_bytes = line.split(b':')
        k = substitute_header_escape_chars(k_bytes).decode('utf8')
        v = substitute_header_escape_chars(v_bytes).decode('utf8')
        if k not in headers:
            headers[k] = v
    return headers


@dataclass
class StompFrame:
    command: str
    headers: dict[str, str]
    body: bytearray


class StompParser:
    _data: bytearray
    _current_pos: int
    _command: str
    _headers: dict[str, str]
    _body_end: int
    _frames: deque[StompFrame]

    def __init__(self):
        self._data = bytearray()
        self._current_pos = 0
        self._command = ''
        self._headers = {}
        self._body_end = 0
        self._frames = deque()

    def add_bytes(self, data: bytes) -> bool:
        self._data.extend(data)

        work_done = False
        while self._parse():
            work_done = True

        n = self._current_pos
        if n > 0:
            del self._data[:n]
            self._body_end -= n
            self._current_pos = 0

        return work_done

    def _parse(self) -> None:
        if self._command:
            return self._parse_body()
        else:
            self._parse_heartbeats()
            return self._parse_command()

    def _parse_heartbeats(self) -> None:
        m = HEARTBEAT_RE.match(self._data, self._current_pos)
        if m:
            self._current_pos = m.end()

    def _parse_command(self) -> bool:
        m = HEAD_RE.match(self._data, self._current_pos)
        if m is None:
            raise ProtocolError('invalid frame')

        if len(m.groups()) < 3:
            return False  # The head seems valid, but incomplete.

        self._current_pos = m.end()
        self._command = m[1].decode('ascii')
        self._headers = parse_headers(m[2])

        try:
            n = int(self._headers['content-length'])
        except (KeyError, ValueError):
            n = 0
        if n > BODY_MAX_LENGTH:
            raise ProtocolError('content-length is too large')

        self._body_end = self._current_pos + n
        return True

    def _parse_body(self) -> bool:
        data = self._data
        n = len(data)
        body_end = self._body_end
        if n > body_end:
            first_illegal_index = self._current_pos + BODY_MAX_LENGTH + 1
            stop = data.find(0, body_end, first_illegal_index)
            if (stop == -1):
                if n >= first_illegal_index:
                    raise ProtocolError('the body is too large')
                self._body_end = n
            else:
                self._frames.append(StompFrame(
                    command=self._command,
                    headers=self._headers,
                    body=data[self._current_pos:stop],
                ))
                self._current_pos = stop + 1  # Skip the frame-terminating NULL.
                self._command = ''
                return True

        return False
