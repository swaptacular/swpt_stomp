from click.testing import CliRunner
from swpt_stomp.client import client
from swpt_stomp.server import server


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
