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

_DN_PART_RE = re.compile(r"(?!-)[a-z0-9-]{1,63}(?<!-)$", re.IGNORECASE)

_STOMP_FILE_RE = re.compile(
    r"""
    ([^\r\n]*)(?:\r?\n)     # host
    ([^\r\n]*)(?:\r?\n)*\Z  # destination""",
    re.VERBOSE)

_PEM_CERTIFICATE_RE = re.compile(
    rb"""
    ^-----BEGIN\ CERTIFICATE-----$  # begin marker
    [^-]+                           # certificate content
    ^-----END\ CERTIFICATE-----$    # end marker
    (?:\r?\n)?                      # optional new line
    """,
    re.VERBOSE | re.DOTALL | re.MULTILINE)


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
    stomp_host: str
    stomp_destination: str
    root_cert: bytes
    peer_cert: bytes
    sub_cert: bytes
    subnet: Subnet


class DatabaseError(Exception):
    """Thrown by `NodePeersDatabase` instances, to indicate an error."""


class NodePeersDatabase(ABC):
    """A database containing information for the node and its peers."""

    def __init__(
            self,
            *,
            max_cached_peers: Optional[int] = None,
            peers_cache_seconds: Optional[float] = None,
    ):
        if max_cached_peers is None:
            max_cached_peers = int(
                os.environ.get('MAX_CACHED_PEERS', '5000'))

        if peers_cache_seconds is None:
            peers_cache_seconds = float(
                os.environ.get('PEERS_CACHE_SECONDS', '600'))

        assert max_cached_peers >= 1
        assert peers_cache_seconds >= 0.0
        self.__node_data: Optional[NodeData] = None
        self.__peers: OrderedDict[str, _PeerCacheRecord] = OrderedDict()
        self.__max_cached_peers = max_cached_peers
        self.__peers_cache_seconds = peers_cache_seconds

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
            except Exception as e:  # pragma: nocover
                raise DatabaseError from e

        return self.__node_data

    async def get_peer_data(self, node_id: str) -> Optional[PeerData]:
        """Return data for peer with the given node ID."""

        peer_data = self.__get_stored_peer_data(node_id)
        if peer_data is None:
            try:
                peer_data = await self._get_peer_data(node_id)
            except Exception as e:  # pragma: nocover
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


def get_database_instance(
        url: str,
        *,
        max_cached_peers: Optional[int] = None,
        peers_cache_seconds: Optional[float] = None,
) -> NodePeersDatabase:
    """Return an instance of a node-info database.

    The location of the database is determined by the passed `url`
    parameter. Currently, only the "file://" scheme is supported for the
    URL, and it must refer to a local directory.

    For example:
    >>> db = get_database_instance('file:///path/to/the/database/')

    When values for `max_cached_peers` or `peers_cache_seconds` are not
    passed, the values of "MAX_CACHED_PEERS" and "PEERS_CACHE_SECONDS"
    environment variables will be used.

    The "FILE_READ_THREADS" environment variables specifies the number of
    threads for the `ThreadPoolExecutor`, which is used for reading local
    files asynchronously.
    """
    if url.startswith('file:///'):
        return _LocalDirectory(
            url,
            max_cached_peers=max_cached_peers,
            peers_cache_seconds=peers_cache_seconds,
        )

    raise ValueError(f'invalid database URL: {url}')


@dataclass
class _PeerCacheRecord:
    slots = ('peer_data', 'time')
    peer_data: PeerData
    time: float


class _LocalDirectory(NodePeersDatabase):
    def __init__(
            self,
            url: str,
            *,
            max_cached_peers: Optional[int] = None,
            peers_cache_seconds: Optional[float] = None,
    ):
        super().__init__(
            max_cached_peers=max_cached_peers,
            peers_cache_seconds=peers_cache_seconds,
        )
        assert url.startswith('file:///')
        self._root_dir: str = os.path.normpath(url[7:])
        self._loop = asyncio.get_event_loop()

        default_threads = str(5 * (os.cpu_count() or 1))
        threads = int(os.environ.get('FILE_READ_THREADS', default_threads))
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

    async def _read_cert_file(self, filepath: str) -> bytes:
        content = await self._read_file(filepath)

        # PEM files may contain comments. Remove them. If the PEM file
        # contains multiple certificates, return only the first one.
        m = _PEM_CERTIFICATE_RE.search(content)
        if m is None:  # pragma: nocover
            raise RuntimeError('invalid certificate file')

        return m[0]

    async def _read_oneline(self, filepath: str) -> str:
        content = await self._read_text_file(filepath)
        if content.endswith('\r\n'):
            return content[:-2]
        elif content.endswith('\n'):
            return content[:-1]

        return content

    async def _read_subnet_file(self, filepath: str) -> Subnet:
        s = await self._read_oneline(filepath)
        return Subnet.parse(s)

    async def _get_node_data(self) -> NodeData:
        root_cert = await self._read_cert_file('root-ca.crt')
        node_id = await self._read_oneline('db/nodeid')
        node_type_str = await self._read_oneline('db/nodetype')
        node_type = _parse_node_type(node_type_str)

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
        dir = await self._loop.run_in_executor(
            self._executor,
            partial(self._get_peer_dir, node_id),
        )
        if dir is None:
            return None

        try:
            sub_cert = await self._read_cert_file(f'{dir}/sub-ca.crt')
        except FileNotFoundError:  # pragma: nocover
            return None

        root_cert = await self._read_cert_file(f'{dir}/root-ca.crt')
        peer_cert = await self._read_cert_file(f'{dir}/peercert.crt')

        node_type_str = await self._read_oneline(f'{dir}/nodetype.txt')
        node_type = _parse_node_type(node_type_str)

        servers_str = await self._read_text_file(f'{dir}/nodeinfo/servers.txt')
        servers = _parse_servers_file(servers_str)

        onwer_node_data = await self.get_node_data()
        onwer_node_id = onwer_node_data.node_id
        onwer_node_type = onwer_node_data.node_type

        try:
            stomp_str = await self._read_text_file(f'{dir}/nodeinfo/stomp.txt')
            stomp_host, stomp_destination = _parse_stomp_file(
                stomp_str, node_id=onwer_node_id)
        except FileNotFoundError:
            stomp_host = '/'
            stomp_destination = '/exchange/smp'

        if onwer_node_type == NodeType.AA:
            subnet = await self._read_subnet_file(f'{dir}/subnet.txt')
        elif onwer_node_type == NodeType.CA:
            subnet = await self._read_subnet_file(f'{dir}/masq-subnet.txt')
        else:
            assert onwer_node_type == NodeType.DA
            assert onwer_node_data.subnet is not None
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


def _parse_stomp_file(s: str, *, node_id: str) -> tuple[str, str]:
    """Return a (stomp_host, stomp_destination) tuple."""

    if m := _STOMP_FILE_RE.match(s):
        stomp_host = m[1].replace('${NODE_ID}', node_id)
        stomp_destination = m[2].replace('${NODE_ID}', node_id)
        return stomp_host, stomp_destination

    raise RuntimeError('invalid stomp.txt file')


def _is_valid_hostname(hostname):
    if hostname[-1] == ".":
        # strip exactly one dot from the right, if present
        hostname = hostname[:-1]

    if len(hostname) > 253:
        return False

    labels = hostname.split(".")
    return all(_DN_PART_RE.match(label) for label in labels)
