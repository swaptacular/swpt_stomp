from click.testing import CliRunner
from swpt_stomp.client import client
from swpt_stomp.server import server
from swpt_stomp.configure_queue import configure_queue


def test_client_cli(datadir):
    runner = CliRunner()
    result = runner.invoke(client, [
        '5921983fe0e6eb987aeedca54ad3c708',
        'test_client',
        f'--server-cert={datadir["AA"]}/server.crt',
        f'--server-key={datadir["AA"]}/server.key',
        f'--nodedata-url=file://{datadir["AA"]}',
    ])
    assert result.exit_code == 1
    assert isinstance(result.exception, ConnectionRefusedError)


def test_server_cli(datadir):
    runner = CliRunner()
    result = runner.invoke(server, [
        f'--server-cert={datadir["AA"]}/server.crt',
        f'--server-key={datadir["AA"]}/INVALID.key',
        f'--nodedata-url=file://{datadir["AA"]}',
        '--log-format=json',
    ])
    assert result.exit_code == 1
    assert isinstance(result.exception, FileNotFoundError)


def test_configure_queue(datadir):
    runner = CliRunner()

    for i in range(2):
        result = runner.invoke(configure_queue, [
            '5921983fe0e6eb987aeedca54ad3c708',
            'aa.5921983fe0e6eb987aeedca54ad3c708',
            f'--nodedata-url=file://{datadir["AA"]}',
        ])
        assert result.exit_code == 0

        result = runner.invoke(configure_queue, [
            '060791aeca7637fa3357dfc0299fb4c5',
            'aa.060791aeca7637fa3357dfc0299fb4c5',
            f'--nodedata-url=file://{datadir["AA"]}',
        ])
        assert result.exit_code == 0

        result = runner.invoke(configure_queue, [
            '1234abcd',
            'ca.1234abcd',
            f'--nodedata-url=file://{datadir["CA"]}',
        ])
        assert result.exit_code == 0

        result = runner.invoke(configure_queue, [
            '1234abcd',
            'da.1234abcd',
            f'--nodedata-url=file://{datadir["DA"]}',
        ])
        assert result.exit_code == 0

    result = runner.invoke(configure_queue, [
        'INVALID',
        'q.INVALID',
        f'--nodedata-url=file://{datadir["DA"]}',
    ])
    assert result.exit_code == 1
