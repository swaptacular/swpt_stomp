import re
from enum import Enum
from dataclasses import dataclass
from typing import NamedTuple, Optional

_ONLY_DIGITS_RE = re.compile(r"[0-9]+$")
_DN_PART_RE = re.compile(r"(?!-)[a-z0-9-]{1,63}(?<!-)$", re.IGNORECASE)


class NodeType(Enum):
    AA = 1  # Accounting Authority
    CA = 2  # Creditors Agent
    DA = 3  # Debtors Agent


class Subnet(NamedTuple):
    subnet: int
    subnet_mask: int

    @classmethod
    def parse(cls, s):
        """Parse from a hexadecimal string."""
        n = 4 * len(s)
        if n > 64:
            raise ValueError(f'invalid subnet: {s}')

        try:
            subnet = (int(s, base=16) << (64 - n)) if n > 0 else 0
            if not 0 <= subnet < 0xffffffffffffffff:
                raise ValueError
        except ValueError:
            raise ValueError(f'invalid subnet: {s}')

        return Subnet(subnet, (-1 << (64 - n)) & 0xffffffffffffffff)


@dataclass
class NodeData:
    """Basic data about the owner of the node.
    """
    __slots__ = (
        'node_type',
        'node_id',
        'root_cert',
        'subnet',
    )

    node_type: NodeType
    node_id: str
    root_cert: bytes
    subnet: Optional[Subnet]


@dataclass
class PeerData:
    """Basic data about a peer of the owner of the node.
    """
    __slots__ = (
        'node_type',
        'node_id',
        'servers',
        'stomp_host',
        'stomp_destination',
        'root_cert',
        'peer_cert',
        'sub_cert',
        'subnet',
    )

    node_type: NodeType
    node_id: str
    servers: list[tuple[str, int]]
    stomp_host: Optional[str]
    stomp_destination: Optional[str]
    root_cert: bytes
    peer_cert: bytes
    sub_cert: Optional[bytes]
    subnet: Optional[Subnet]


def _parse_node_type(s: str) -> NodeType:
    if s == "Accounting Authorities":
        return NodeType.AA
    elif s == "Creditors Agents":
        return NodeType.CA
    elif s == "Debtors Agents":
        return NodeType.DA
    else:
        raise ValueError(f'invalid node type: {s}')


def _parse_servers(s: str) -> list[tuple[str, int]]:
    servers = []
    for server in s.split(maxsplit=10000):
        try:
            host, port_str = server.split(':', maxsplit=1)
        except ValueError:
            raise ValueError(f'invalid server: {s}')

        if not _is_valid_hostname(host):
            raise ValueError(f'invalid host: {host}')

        try:
            port = int(port_str)
            if not 1 <= port <= 65535:
                raise ValueError
        except ValueError:
            raise ValueError(f'invalid port: {port_str}')

        servers.append((host, port))

    return servers


def _is_valid_hostname(hostname):
    if hostname[-1] == ".":
        # strip exactly one dot from the right, if present
        hostname = hostname[:-1]

    if len(hostname) > 253:
        return False

    labels = hostname.split(".")

    # the TLD must be not all-numeric
    if _ONLY_DIGITS_RE.match(labels[-1]):
        return False

    return all(_DN_PART_RE.match(label) for label in labels)
