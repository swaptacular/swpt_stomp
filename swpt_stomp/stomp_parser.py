from dataclasses import dataclass, field
from typing import Iterator
from collections import deque
import re

_HEARTBEAT_RE = re.compile(rb"""(?:\r?\n)+    # empty lines""", re.VERBOSE)

_HEAD_RE = re.compile(
    rb"""(?:\Z|
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
    re.VERBOSE,
)

_HEADER_SEPARATOR_RE = re.compile(rb"\r?\n|:")
_HEADER_UNESCAPE_RE = re.compile(rb"\\.")
_HEADER_UNESCAPE_CHARS = {
    rb"\r": b"\r",
    rb"\n": b"\n",
    rb"\c": b":",
    rb"\\": b"\\",
}
_HEADER_ESCAPE_TABLE = str.maketrans(
    {
        ord("\r"): r"\r",
        ord("\n"): r"\n",
        ord(":"): r"\c",
        ord("\\"): r"\\",
    }
)


def _substitute_header_escape_chars(s: bytes) -> bytes:
    # \r, \n, \c, \\ in headers should be substituted accordingly. Other
    # escape symbols are not allowed.
    try:
        return _HEADER_UNESCAPE_RE.sub(
            lambda m: _HEADER_UNESCAPE_CHARS[m[0]], s
        )
    except KeyError:
        raise ProtocolError("Invalid header.")


def _parse_headers(s: bytes) -> dict[str, str]:
    if len(s) > HEADER_MAX_LENGTH:
        raise ProtocolError("The frame header is too big.")

    headers = {}
    parts = _HEADER_SEPARATOR_RE.split(s)
    try:
        for i in range(0, len(parts) - 1, 2):
            key = _substitute_header_escape_chars(parts[i]).decode("utf8")
            value = _substitute_header_escape_chars(parts[i + 1]).decode(
                "utf8"
            )
            if key not in headers:
                headers[key] = value
    except UnicodeDecodeError:
        raise ProtocolError("UTF-8 decode error.")

    return headers


HEADER_MAX_LENGTH = 4_096
BODY_MAX_LENGTH = 8_192


class ProtocolError(Exception):
    """STOMP 1.2 protocol error."""

    def __init__(self, message: str):
        super().__init__(message)

    @property
    def message(self) -> str:
        return self.args[0]


class EmptyQueue(Exception):
    """There are no available frames in the queue."""


@dataclass
class StompFrame:
    """STOMP 1.2 Protocol Frame."""

    command: str
    headers: dict[str, str] = field(default_factory=dict)
    body: bytearray = field(default_factory=bytearray)

    def __bytes__(self) -> bytes:
        t = _HEADER_ESCAPE_TABLE
        body = self.body
        headers = self.headers
        header_lines = [
            f"{k.translate(t)}:{v.translate(t)}\n" for k, v in headers.items()
        ]
        if body and "content-length" not in headers:
            header_lines.append(f"content-length:{len(body)}\n")

        headers_bytes = "".join(header_lines).encode("utf8")
        command_bytes = self.command.encode("ascii")
        return b"%s\n%s\n%s\0" % (command_bytes, headers_bytes, body)


class StompParser:
    """STOMP version 1.2 parser."""

    frames: deque[StompFrame]

    def __init__(self):
        self.reset()

    def reset(self) -> None:
        """Reset the parser, parsed frames will be lost."""
        self._data = bytearray()
        self._current_pos = 0
        self._command = ""
        self._headers: dict[str, str] = {}
        self._body_end = 0
        self.frames: deque[StompFrame] = deque()

    def add_bytes(self, data: bytes) -> None:
        """Feed bytes to the parser.

        If, as a result of feeding the given `data` to the parser, one or
        more frames have been completed, after this method returns, the
        frames will be available via the `parser.pop_frame` method.

        This method will raise a `ProtocolError` exception, if the incoming
        byte-stream does not obey the STOMP 1.2 protocol specification.
        """
        self._data.extend(data)
        while self._parse_block():
            pass

        n = self._current_pos
        if n > 0:
            del self._data[:n]
            self._body_end -= n
            self._current_pos = 0

    def has_frame(self) -> bool:
        """Return whether at least one frame is available for reading."""
        return bool(self.frames)

    def pop_frame(self) -> StompFrame:
        """Return a parsed frame, and remove it from the queue.

        Raises `EmptyQueue` error if there are no available frames.
        """
        try:
            return self.frames.popleft()
        except IndexError:
            raise EmptyQueue()

    def __iter__(self) -> Iterator[StompFrame]:
        """Iterate over available frames, removing them from the queue."""
        while True:
            try:
                yield self.pop_frame()
            except EmptyQueue:
                break

    def _parse_block(self) -> bool:
        if self._command:
            return self._parse_body()
        else:
            self._parse_heartbeats()
            return self._parse_command()

    def _parse_heartbeats(self) -> None:
        data = self._data
        current_pos = self._current_pos
        if len(data) > current_pos:
            m = _HEARTBEAT_RE.match(data, current_pos)
            if m:
                self._current_pos = m.end()

    def _parse_command(self) -> bool:
        data = self._data
        current_pos = self._current_pos
        if len(data) == current_pos:
            return False  # There is nothing to match.

        m = _HEAD_RE.match(data, current_pos)
        if m is None:
            raise ProtocolError("Invalid frame.")

        if m[3] is None:
            return False  # The head seems valid, but incomplete.

        self._current_pos = current_pos = m.end()
        self._command = m[1].decode("ascii")
        self._headers = _parse_headers(m[2])

        try:
            n = int(self._headers["content-length"])
        except (KeyError, ValueError):
            n = 0
        if n > BODY_MAX_LENGTH:
            raise ProtocolError("Content-length is too large.")

        self._body_end = current_pos + n
        return True

    def _parse_body(self) -> bool:
        data = self._data
        n = len(data)
        body_end = self._body_end
        if n > body_end:
            first_illegal_index = self._current_pos + BODY_MAX_LENGTH + 1
            stop = data.find(0, body_end, first_illegal_index)
            if stop == -1:
                if n >= first_illegal_index:
                    raise ProtocolError("The body is too large.")
                self._body_end = n
            else:
                self.frames.append(
                    StompFrame(
                        command=self._command,
                        headers=self._headers,
                        body=data[self._current_pos : stop],
                    )
                )
                self._current_pos = (
                    stop + 1
                )  # Skip the frame-terminating NULL.
                self._command = ""
                return True

        return False
