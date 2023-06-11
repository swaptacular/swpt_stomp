import pytest
from swpt_stomp.peer_data import Subnet, NodeType, NodePeersDatabase


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


def test_parse_servers():
    from swpt_stomp.peer_data import _parse_servers

    assert _parse_servers('server1.example.com.:1234') == [
        ('server1.example.com.', 1234)
    ]

    assert _parse_servers(
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
        _parse_servers('server1.example.com:123456')

    with pytest.raises(ValueError):
        _parse_servers('server1.example.com:-1234')

    with pytest.raises(ValueError):
        _parse_servers('server1.example.com:0')

    with pytest.raises(ValueError):
        _parse_servers('server1.example.com:INVALID')

    with pytest.raises(ValueError):
        _parse_servers('server1.example.com:')

    with pytest.raises(ValueError):
        _parse_servers('server1.example.com')

    # Invalid symbols in host.
    with pytest.raises(ValueError):
        _parse_servers('24[s]3q5:1234')

    # The host is too long.
    with pytest.raises(ValueError):
        _parse_servers(50 * 'abcdefgh.' + 'com:1234')


def test_db_basics():
    with pytest.raises(ValueError):
        NodePeersDatabase('http://example.com/db')

    db = NodePeersDatabase('file:///home//user/./db/')
    assert db.url == 'file:///home//user/./db/'
    assert db._root_dir == '/home/user/db'
