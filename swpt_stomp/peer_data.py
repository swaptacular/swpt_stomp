import re
import os
import os.path
import asyncio
import time
import tomli
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict
from functools import partial
from abc import ABC, abstractmethod
from enum import Enum
from dataclasses import dataclass
from typing import NamedTuple, Optional

_MIN_INT64 = -1 << 63
_MAX_INT64 = (1 << 63) - 1
_DN_PART_RE = re.compile(r"(?!-)[a-z0-9-]{1,63}(?<!-)$", re.IGNORECASE)

_PEM_CERTIFICATE_RE = re.compile(
    rb"""
    ^-----BEGIN\ CERTIFICATE-----$  # begin marker
    [^-]+                           # certificate content
    ^-----END\ CERTIFICATE-----$    # end marker
    (?:\r?\n)?                      # optional new line
    """,
    re.VERBOSE | re.DOTALL | re.MULTILINE)

MAX_CACHED_PEERS = int(os.environ.get('MAX_CACHED_PEERS', '5000'))
PEERS_CACHE_SECONDS = float(os.environ.get('PEERS_CACHE_SECONDS', '600'))
FILE_READ_THREADS = int(os.environ.get(
    'FILE_READ_THREADS',
    str(5 * (os.cpu_count() or 1)),
))


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

    def match(self, n: int) -> bool:
        """Return True if `n` is in the subnet.
        """
        return (_MIN_INT64 <= n <= _MAX_INT64
                and n & self.subnet_mask == self.subnet)


@dataclass
class StompConfig:
    """Data parsed from the stomp.toml file.
    """
    __slots__ = (
        'servers',
        'host',
        'destination',
        'login',
        'passcode',
        'accepted_content_types',
    )
    servers: list[tuple[str, int]]
    host: str
    destination: str
    login: Optional[str]
    passcode: Optional[str]
    accepted_content_types: list[str]


@dataclass
class NodeData:
    """Basic data about the owner of the node.
    """
    __slots__ = (
        'node_type',
        'node_id',
        'root_cert',
        'creditors_subnet',
        'debtors_subnet',
    )
    node_type: NodeType
    node_id: str
    root_cert: bytes
    creditors_subnet: Subnet
    debtors_subnet: Subnet


@dataclass
class PeerData:
    """Basic data about a peer of the owner of the node.
    """
    __slots__ = (
        'node_type',
        'node_id',
        'stomp_config',
        'root_cert',
        'sub_cert',
        'creditors_subnet',
        'debtors_subnet',
    )
    node_type: NodeType
    node_id: str
    stomp_config: StompConfig
    root_cert: bytes
    sub_cert: bytes
    creditors_subnet: Subnet
    debtors_subnet: Subnet


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
            max_cached_peers = MAX_CACHED_PEERS
        if peers_cache_seconds is None:
            peers_cache_seconds = PEERS_CACHE_SECONDS

        assert max_cached_peers >= 1
        assert peers_cache_seconds == peers_cache_seconds  # not a NaN
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
        *,
        url: Optional[str] = None,
        max_cached_peers: Optional[int] = None,
        peers_cache_seconds: Optional[float] = None,
        file_read_threads: Optional[int] = None,
        **kwargs,
) -> NodePeersDatabase:
    """Return an instance of a node-info database.

    The location of the database is determined by the `url` parameter.
    Currently, only the "file://" scheme is supported for the URL, and it
    must refer to a local directory.

    For example:
    >>> db = get_database_instance(url='file:///path/to/the/database/')

    When values for `max_cached_peers`, `peers_cache_seconds`, and
    `peers_cache_seconds` are not passed, the values of "MAX_CACHED_PEERS",
    "PEERS_CACHE_SECONDS", and "FILE_READ_THREADS" environment variables
    will be used. When not set, reasonable default values will be used.

    NOTE: The `file_read_threads` parameter specifies the number of threads
    for the `ThreadPoolExecutor`, which will be used for reading local files
    asynchronously.
    """
    if url and url.startswith('file:///'):
        return _LocalDirectory(
            url,
            max_cached_peers=max_cached_peers,
            peers_cache_seconds=peers_cache_seconds,
            file_read_threads=file_read_threads,
        )

    raise ValueError(f'invalid database URL: {url}')


@dataclass
class _PeerCacheRecord:
    slots = ('peer_data', 'time')
    peer_data: PeerData
    time: float


_ZERO_SUBNET = Subnet.parse(8 * '00')
_UNIVERSAL_SUBNET = Subnet.parse('')


class _LocalDirectory(NodePeersDatabase):
    def __init__(
            self,
            url: str,
            *,
            max_cached_peers: Optional[int] = None,
            peers_cache_seconds: Optional[float] = None,
            file_read_threads: Optional[int] = None,
    ):
        super().__init__(
            max_cached_peers=max_cached_peers,
            peers_cache_seconds=peers_cache_seconds,
        )
        assert url.startswith('file:///')
        self._root_dir: str = os.path.normpath(url[7:])
        self._loop = asyncio.get_event_loop()

        if file_read_threads is None:
            file_read_threads = FILE_READ_THREADS

        assert file_read_threads >= 1
        self._executor = ThreadPoolExecutor(max_workers=file_read_threads)

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
            creditors_subnet = _UNIVERSAL_SUBNET
            debtors_subnet = Subnet.parse(node_id)
        elif node_type == NodeType.CA:
            creditors_subnet = await self._read_subnet_file(
                'creditors-subnet.txt')
            debtors_subnet = _UNIVERSAL_SUBNET
        else:
            assert node_type == NodeType.DA
            creditors_subnet = _ZERO_SUBNET
            debtors_subnet = await self._read_subnet_file(
                'debtors-subnet.txt')

        return NodeData(
            node_type=node_type,
            node_id=node_id,
            root_cert=root_cert,
            creditors_subnet=creditors_subnet,
            debtors_subnet=debtors_subnet,
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

        try:
            # Peers that do not have a file with the name "ACTIVE" in their
            # corresponding directories will be ignored. The existence of
            # this file signals that all necessary objects related to the
            # peer (configuration files, RabbitMQ queues, exchanges,
            # bindings etc.) have been created.
            await self._read_file(f'{dir}/ACTIVE')
        except FileNotFoundError:  # pragma: nocover
            return None

        root_cert = await self._read_cert_file(f'{dir}/root-ca.crt')

        node_type_str = await self._read_oneline(f'{dir}/nodetype.txt')
        node_type = _parse_node_type(node_type_str)

        owner_node_data = await self.get_node_data()
        owner_node_id = owner_node_data.node_id
        owner_node_type = owner_node_data.node_type

        stomp_config = _parse_stomp_toml(
            await self._read_text_file(f'{dir}/nodeinfo/stomp.toml'),
            node_id=owner_node_id,
        )

        if owner_node_type == NodeType.AA:
            subnet = await self._read_subnet_file(f'{dir}/subnet.txt')
            if node_type == NodeType.CA:
                creditors_subnet = subnet
                debtors_subnet = owner_node_data.debtors_subnet
            elif node_type == NodeType.DA:
                creditors_subnet = _ZERO_SUBNET
                debtors_subnet = subnet
            else:  # pragma: nocover
                raise ValueError('invalid peer type')
        elif owner_node_type == NodeType.CA:
            if node_type == NodeType.AA:
                creditors_subnet = await self._read_subnet_file(
                    f'{dir}/masq-subnet.txt')
                mask = owner_node_data.creditors_subnet.subnet_mask
                if creditors_subnet.subnet_mask != mask:  # pragma: nocover
                    raise ValueError(
                        f'invalid sunbnet mask in {dir}/masq-subnet.txt')
                debtors_subnet = Subnet.parse(node_id)
            else:  # pragma: nocover
                raise ValueError('invalid peer type')
        else:
            assert owner_node_type == NodeType.DA
            if node_type == NodeType.AA:
                creditors_subnet = _ZERO_SUBNET
                debtors_subnet = owner_node_data.debtors_subnet
            else:  # pragma: nocover
                raise ValueError('invalid peer type')

        return PeerData(
            node_type=node_type,
            node_id=node_id,
            stomp_config=stomp_config,
            root_cert=root_cert,
            sub_cert=sub_cert,
            creditors_subnet=creditors_subnet,
            debtors_subnet=debtors_subnet,
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


def _parse_servers_list(servers: list[object]) -> list[tuple[str, int]]:
    if len(servers) == 0:
        raise ValueError('empty server list')

    parsed_servers = []
    for server in servers:
        try:
            if not isinstance(server, str):
                raise TypeError
            host, port_str = server.split(':', maxsplit=1)
        except (TypeError, ValueError):
            raise ValueError(f'invalid server: {server}')

        if not _is_valid_hostname(host):
            raise ValueError(f'invalid host: {host}')

        try:
            port = int(port_str)
            if not 1 <= port <= 65535:
                raise ValueError
        except ValueError:
            raise ValueError(f'invalid port: {port_str}')

        parsed_servers.append((host, port))

    return parsed_servers


def _parse_stomp_toml(s: str, *, node_id: str) -> StompConfig:
    data = tomli.loads(s)

    servers: object = data.get('servers')
    if not isinstance(servers, list):
        raise ValueError('invalid servers value')
    parsed_servers = _parse_servers_list(servers)

    host: object = data.get('host')
    if not isinstance(host, str):
        raise ValueError('invalid host value')
    host = host.replace('${NODE_ID}', node_id)

    destination: object = data.get('destination')
    if not isinstance(destination, str):
        raise ValueError('invalid destination value')
    destination = destination.replace('${NODE_ID}', node_id)

    login: object = data.get('login')
    if login is not None:
        if not isinstance(login, str):
            raise ValueError('invalid login value')
        login = login.replace('${NODE_ID}', node_id)

    passcode: object = data.get('passcode')
    if not (passcode is None or isinstance(passcode, str)):
        raise ValueError('invalid passcode value')

    accepted_content_types: object = data.get('accepted-content-types', [])
    if not (isinstance(accepted_content_types, list)
            and all(isinstance(x, str) for x in accepted_content_types)):
        raise ValueError('invalid accepted-content-types')
    accepted_content_types.append('application/json')

    return StompConfig(
        servers=parsed_servers,
        host=host,
        destination=destination,
        login=login,
        passcode=passcode,
        accepted_content_types=accepted_content_types,
    )


def _is_valid_hostname(hostname):
    if hostname[-1] == ".":
        # strip exactly one dot from the right, if present
        hostname = hostname[:-1]

    if len(hostname) > 253:
        return False

    labels = hostname.split(".")
    return all(_DN_PART_RE.match(label) for label in labels)
