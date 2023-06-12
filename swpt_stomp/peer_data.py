import re
import os
import os.path
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict
from functools import partial
from abc import ABC, abstractmethod
from enum import Enum
from dataclasses import dataclass
from typing import NamedTuple, Optional

##############################################################################
# Used environment variables:
#
# * `EXECUTOR_THREADS` specifies the number of threads for the default
#   `ThreadPoolExecutor`.
#
# * `MAX_CACHED_PEERS` specifies the maximum number of cached peer data
#   records.
#
# * `PEERS_CACHE_SECONDS` specifies the number seconds during which cached
#   peer data records will be considered fresh.
##############################################################################

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


class DatabaseError(Exception):
    """Thrown by `NodePeersDatabase` instances, to indicate an error."""


class NodePeersDatabase(ABC):
    """A database containing information for the node and its peers."""

    def __init__(self) -> None:
        self.__node_data: Optional[NodeData] = None
        self.__peers: OrderedDict[str, _PeerCacheRecord] = OrderedDict()
        self.__max_cached_peers = max(1, int(
            os.environ.get('MAX_CACHED_PEERS', '1000')))
        self.__peers_cache_seconds = float(
            os.environ.get('PEERS_CACHE_SECONDS', '600'))

    def __get_stored_peer_data(self, node_id: str) -> Optional[PeerData]:
        if peer_record := self.__peers.get(node_id, None):
            cutoff_time = time.time() - self.__peers_cache_seconds
            if peer_record.time >= cutoff_time:
                return peer_record.peer_data

        return None

    def __store_peer_data(self, peer_data: PeerData) -> None:
        peers = self.__peers
        while len(peers) >= self.__max_cached_peers:
            peers.popitem()

        peers[peer_data.node_id] = _PeerCacheRecord(
            peer_data=peer_data,
            time=time.time(),
        )

    async def get_node_data(self) -> NodeData:
        """Return data for owner of the node."""

        if self.__node_data is None:
            try:
                self.__node_data = await self._get_node_data()
            except Exception as e:
                raise DatabaseError from e

        return self.__node_data

    async def get_peer_data(self, node_id: str) -> Optional[PeerData]:
        """Return data for peer with the given node ID."""

        peer_data = self.__get_stored_peer_data(node_id)
        if peer_data is None:
            try:
                peer_data = await self._get_peer_data(node_id)
            except Exception as e:
                raise DatabaseError from e
            if peer_data:
                self.__store_peer_data(peer_data)

        return peer_data

    @abstractmethod
    async def _get_peer_data(self, node_id: str) -> Optional[PeerData]:
        raise NotImplementedError  # pragma: nocover

    @abstractmethod
    async def _get_node_data(self) -> NodeData:
        raise NotImplementedError  # pragma: nocover


def get_database_instance(url: str) -> NodePeersDatabase:
    """Return an instance of a node-info database.

    The location of the database is determined by the passed `url`
    parameter. Currently, only the "file://" scheme is supported for the
    URL, and it must refer to a local directory.

    For example:
    >>> db = get_database_instance('file:///path/to/the/database/')
    """
    if url.startswith('file:///'):
        return _LocalDirectory(url)

    raise ValueError(f'invalid database URL: {url}')


@dataclass
class _PeerCacheRecord:
    slots = ('peer_data', 'time')
    peer_data: PeerData
    time: float


class _LocalDirectory(NodePeersDatabase):
    def __init__(self, url: str):
        super().__init__()

        assert url.startswith('file:///')
        self._root_dir: str = os.path.normpath(url[7:])
        self._loop = asyncio.get_event_loop()

        default_threads = str(5 * (os.cpu_count() or 1))
        threads = int(os.environ.get('EXECUTOR_THREADS', default_threads))
        self._executor = ThreadPoolExecutor(max_workers=threads)

    def _fetch_file(self, filepath: str) -> bytes:
        abspath = os.path.join(self._root_dir, filepath)
        with open(abspath, 'br') as f:
            return f.read()

    def _get_peer_dir(self, node_id: str) -> Optional[str]:
        abspath = os.path.join(self._root_dir, f'peers/{node_id}')
        return abspath if os.path.isdir(abspath) else None

    async def _read_file(self, filepath: str) -> bytes:
        return await self._loop.run_in_executor(
            self._executor,
            partial(self._fetch_file, filepath),
        )

    async def _read_text_file(self, filepath: str) -> str:
        octets = await self._read_file(filepath)
        return octets.decode('utf8')

    async def _read_pem_file(self, filepath: str) -> bytes:
        return await self._read_file(filepath)

    async def _read_oneline(self, filepath: str) -> str:
        content = await self._read_text_file(filepath)
        if content.endswith('\r\n'):
            return content[:-2]
        elif content.endswith('\n'):
            return content[:-1]

        return content

    async def _read_subnet_file(self, filepath: str) -> Optional[Subnet]:
        try:
            s = await self._read_oneline(filepath)
        except FileNotFoundError:
            return None

        return Subnet.parse(s)

    async def _get_node_data(self) -> NodeData:
        root_cert = await self._read_pem_file('root-ca.crt')
        node_id = await self._read_oneline('db/nodeid')
        node_type_str = await self._read_oneline('db/nodetype')
        try:
            node_type = _parse_node_type(node_type_str)
        except ValueError as e:
            raise DatabaseError from e

        if node_type == NodeType.AA:
            subnet = None
        elif node_type == NodeType.CA:
            subnet = await self._read_subnet_file('creditors-subnet.txt')
        else:
            assert node_type == NodeType.DA
            subnet = await self._read_subnet_file('debtors-subnet.txt')

        return NodeData(
            node_type=node_type,
            node_id=node_id,
            root_cert=root_cert,
            subnet=subnet,
        )

    async def _get_peer_data(self, node_id: str) -> Optional[PeerData]:
        onwer_node_data = await self.get_node_data()
        onwer_node_id = onwer_node_data.node_id
        onwer_node_type = onwer_node_data.node_type

        pdir = await self._loop.run_in_executor(
            self._executor,
            partial(self._get_peer_dir, node_id),
        )
        if pdir is None:
            return None

        root_cert = await self._read_pem_file(f'{pdir}/root-ca.crt')
        peer_cert = await self._read_pem_file(f'{pdir}/peercert.crt')
        try:
            sub_cert = await self._read_pem_file(f'{pdir}/sub-ca.crt')
        except FileNotFoundError:
            sub_cert = None

        node_type_str = await self._read_oneline(f'{pdir}/nodetype.txt')
        node_type = _parse_node_type(node_type_str)

        s = await self._read_text_file(f'{pdir}/nodeinfo/servers.txt')
        servers = _parse_servers_file(s)

        try:
            s = await self._read_text_file(f'{pdir}/nodeinfo/stomp.txt')
            stomp_host, stomp_destination = _parse_stomp_file(s, onwer_node_id)
        except FileNotFoundError:
            stomp_host, stomp_destination = None, None

        if onwer_node_type == NodeType.AA:
            subnet_file = f'{pdir}/subnet.txt'
            subnet = await self._read_subnet_file(subnet_file)
            if subnet is None:
                raise Exception(f'missing file: {subnet_file}')
        elif onwer_node_type == NodeType.CA:
            subnet = await self._read_subnet_file(f'{pdir}/masq-subnet.txt')
        else:
            assert onwer_node_type == NodeType.DA
            subnet = onwer_node_data.subnet

        return PeerData(
            node_type=node_type,
            node_id=node_id,
            servers=servers,
            stomp_host=stomp_host,
            stomp_destination=stomp_destination,
            root_cert=root_cert,
            peer_cert=peer_cert,
            sub_cert=sub_cert,
            subnet=subnet,
        )


def _parse_node_type(s: str) -> NodeType:
    if s == "Accounting Authorities":
        return NodeType.AA
    elif s == "Creditors Agents":
        return NodeType.CA
    elif s == "Debtors Agents":
        return NodeType.DA
    else:
        raise ValueError(f'invalid node type: {s}')


def _parse_servers_file(s: str) -> list[tuple[str, int]]:
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


def _parse_stomp_file(s: str, node_id: str) -> tuple[str, str]:
    """Return a (stomp_host, stomp_destination) tuple."""

    # TODO: Implement properly.
    return ('/', '/exchange/smp')


def _is_valid_hostname(hostname):
    if hostname[-1] == ".":
        # strip exactly one dot from the right, if present
        hostname = hostname[:-1]

    if len(hostname) > 253:
        return False

    labels = hostname.split(".")
    return all(_DN_PART_RE.match(label) for label in labels)
