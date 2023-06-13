import pytest
from swpt_stomp.peer_data import Subnet, NodeType, get_database_instance


def test_parse_subnet():
    sn = Subnet.parse('0a')
    assert sn.subnet == 0x0a00000000000000
    assert sn.subnet_mask == 0xff00000000000000

    sn = Subnet.parse('')
    assert sn.subnet == 0x0000000000000000
    assert sn.subnet_mask == 0x0000000000000000

    sn = Subnet.parse('1234567890abcdef')
    assert sn.subnet == 0x1234567890abcdef
    assert sn.subnet_mask == 0xffffffffffffffff

    with pytest.raises(ValueError):
        Subnet.parse('-02')  # negative

    with pytest.raises(ValueError):
        Subnet.parse('0a0000000000000013431545')  # too long


def test_parse_node_type():
    from swpt_stomp.peer_data import _parse_node_type

    assert _parse_node_type('Accounting Authorities') == NodeType.AA
    assert _parse_node_type('Creditors Agents') == NodeType.CA
    assert _parse_node_type('Debtors Agents') == NodeType.DA

    with pytest.raises(ValueError):
        _parse_node_type('something else')


def test_parse_servers_file():
    from swpt_stomp.peer_data import _parse_servers_file

    assert _parse_servers_file('server1.example.com.:1234') == [
        ('server1.example.com.', 1234)
    ]

    assert _parse_servers_file(
        'server1.example.com:1234\n'
        'server2.example.com:2345\n'
        '1.2.3.4:2345\n'
    ) == [
        ('server1.example.com', 1234),
        ('server2.example.com', 2345),
        ('1.2.3.4', 2345),
    ]

    # Invalid port.
    with pytest.raises(ValueError):
        _parse_servers_file('server1.example.com:123456')

    with pytest.raises(ValueError):
        _parse_servers_file('server1.example.com:-1234')

    with pytest.raises(ValueError):
        _parse_servers_file('server1.example.com:0')

    with pytest.raises(ValueError):
        _parse_servers_file('server1.example.com:INVALID')

    with pytest.raises(ValueError):
        _parse_servers_file('server1.example.com:')

    with pytest.raises(ValueError):
        _parse_servers_file('server1.example.com')

    # Invalid symbols in host.
    with pytest.raises(ValueError):
        _parse_servers_file('24[s]3q5:1234')

    # The host is too long.
    with pytest.raises(ValueError):
        _parse_servers_file(50 * 'abcdefgh.' + 'com:1234')


def test_parse_stomp_file():
    from swpt_stomp.peer_data import _parse_stomp_file

    for s in [
        'host\ndestination',
        'host\ndestination\n',
        'host\ndestination\n\n\n',
        'host\r\ndestination',
        'host\r\ndestination\r\n',
        'host\ndestination\r\n\n\n',
    ]:
        assert _parse_stomp_file(
            s, node_id='1234') == ('host', 'destination')

    assert _parse_stomp_file(
        '/${NODE_ID}\n/exchange/${NODE_ID}/smp',
        node_id='1234'
    ) == ('/1234', '/exchange/1234/smp')

    with pytest.raises(Exception):
        _parse_stomp_file('INVALID', node_id='1234')

    with pytest.raises(Exception):
        _parse_stomp_file('host\ndestination\nMORE', node_id='1234')


@pytest.mark.asyncio
async def test_get_node_data(datadir):
    with pytest.raises(ValueError):
        get_database_instance('http://example.com/db')

    db = get_database_instance('file:///home//user/./db/')
    assert db._root_dir == '/home/user/db'

    db = get_database_instance(f'file://{datadir["CA"]}')
    data = await db.get_node_data()
    assert data.node_id == '5921983fe0e6eb987aeedca54ad3c708'
    assert data.node_type == NodeType.CA
    assert data.subnet == Subnet.parse('000001')
    assert b'-----BEGIN CERTIFICATE-----' in data.root_cert

    db = get_database_instance(f'file://{datadir["DA"]}')
    data = await db.get_node_data()
    assert data.node_id == '060791aeca7637fa3357dfc0299fb4c5'
    assert data.node_type == NodeType.DA
    assert data.subnet == Subnet.parse('1234abcd00')
    assert b'-----BEGIN CERTIFICATE-----' in data.root_cert


@pytest.mark.asyncio
async def test_get_ca_peer_data(datadir):
    db = get_database_instance(f'file://{datadir["CA"]}')

    assert await db.get_peer_data('INVALID') is None

    data = await db.get_peer_data('1234abcd')
    assert data.node_id == '1234abcd'
    assert data.node_type == NodeType.AA

    data2 = await db.get_peer_data('1234abcd')
    assert data is data2


@pytest.mark.asyncio
async def test_get_aa_peer_data(datadir):
    db = get_database_instance(f'file://{datadir["AA"]}')

    assert await db.get_peer_data('INVALID') is None

    data = await db.get_peer_data('5921983fe0e6eb987aeedca54ad3c708')
    assert data.node_id == '5921983fe0e6eb987aeedca54ad3c708'
    assert data.node_type == NodeType.CA


@pytest.mark.asyncio
async def test_get_da_peer_data(datadir):
    db = get_database_instance(f'file://{datadir["DA"]}')

    assert await db.get_peer_data('INVALID') is None

    data = await db.get_peer_data('1234abcd')
    assert data.node_id == '1234abcd'
    assert data.node_type == NodeType.AA
