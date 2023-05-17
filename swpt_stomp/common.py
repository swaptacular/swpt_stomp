from __future__ import annotations
from dataclasses import dataclass


@dataclass
class Message:
    id: str
    content_type: str
    body: bytearray
