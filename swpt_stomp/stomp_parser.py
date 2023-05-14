from __future__ import annotations
from dataclasses import dataclass
import re

HEARTBEAT_RE = re.compile(
    rb"""^(?:\r?\n)+                                       # empty lines""",
    re.DOTALL | re.VERBOSE)

HEAD_RE = re.compile(
    rb"""^
         ([A-Z]{1,20})\r?\n                                # command
         ((?:[^\n\r:]{1,500}:[^\n\r:]{0,500}\r?\n){0,50})  # header lines
         \r?\n                                             # empty line""",
    re.DOTALL | re.VERBOSE)

HEADER_ESCAPE_RE = re.compile(rb'\\.')
HEADER_ESCAPE_CHARS = {
    rb'\r': b'\r',
    rb'\n': b'\n',
    rb'\c': b':',
    rb'\\': b'\\',
}

BODY_RE = re.compile(
    rb"""^
         ([^\x00]{0,50000})                                # optional body
         \x00                                              # NULL""",
    re.DOTALL | re.VERBOSE)


class ProtocolError(Exception):
    """Protocol error"""


def substitute_header_escape_chars(s: bytes) -> bytes:
    # \r, \n, \c, \\ in headers should be substituted accordingly. Other
    # escape symbols are not allowed.
    try:
        return HEADER_ESCAPE_RE.sub(lambda m: HEADER_ESCAPE_CHARS[m[0]], s)
    except KeyError:
        raise ProtocolError()


def parse_headers(s: str) -> dict[str, str]:
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
    body: bytes


class StompParser:
    def __init__(self):
        self.data = bytearray()
        self.command = ''
        self.parsed_heartbeat = False
        self.headers = {}
        self.body_start = 0
        self.body_end = 0
        self.frames = []

    def read(self, data: bytes) -> bool:
        self.data.extend(data)
        while True:
            received_heartbeat, work_done = self._parse()
            if not work_done:
                break

        return received_heartbeat or work_done

    def _parse(self) -> tuple[bool, bool]:
        if self.command:
            received_heartbeat = False
            work_done = self._parse_body()
        else:
            received_heartbeat = self._parse_heartbeat()
            work_done = self._parse_command()

        return received_heartbeat, work_done

    def _parse_heartbeat(self) -> bool:
        assert not self.command
        m = HEARTBEAT_RE.match(self.data)
        if m:
            self.data = self.data[m.end():]
            return True

        return False

    def _parse_command(self) -> bool:
        assert not self.command
        m = HEAD_RE.match(self.data)
        if m:
            self.body_start = m.end()
            self.command = m[1].decode('utf8')
            self.headers = parse_headers(m[2])
            try:
                n = int(self.headers['content-length'])
            except (KeyError, ValueError):
                n = 0
            self.body_end = self.body_start + n
            return True

        return False

    def _parse_body(self) -> bool:
        assert self.command
        data = self.data
        if len(data) > self.body_end and (m := BODY_RE.match(data, self.body_start)):
            self.frames.append(StompFrame(
                command=self.command,
                headers=self.headers,
                body=m[1],
            ))
            self.data = data[m.end():]
            self.command = ''
            return True

        return False
