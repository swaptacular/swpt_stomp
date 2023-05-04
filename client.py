import asyncio
import ssl

CERT_FILE = '/home/evgeni/test_ca/00000001.crt'
CA_FILE = '/home/evgeni/src/swpt_ca_scripts/root-ca.crt'
KEY_FILE = '/home/evgeni/test_ca/server.key'
HOST='127.0.0.1'
PORT=5050
SSL_HANDSHAKE_TIMEOUT=5.0


class HelloProtocol(asyncio.Protocol):
    def __init__(self, on_con_lost):
        self.on_con_lost = on_con_lost

    def connection_made(self, transport):
        message = "hello"
        peercert = transport.get_extra_info('peercert')
        transport.write(message.encode())
        print('Certificate serialNumber: {}'.format(get_cert_common_name(peercert)))
        print('Data sent: {!r}'.format(message))

    def data_received(self, data):
        print('Data received: {!r}'.format(data.decode()))

    def connection_lost(self, exc):
        print('The server closed the connection')
        print('Stop the event loop')
        self.on_con_lost.set_result(True)


def get_cert_common_name(peercert):
    try:
        subject = peercert['subject']
        for rdns in subject:  # traverse all relative distinguished names
            key, value = rdns[0]
            if key == 'serialNumber':
                return value
    except (TypeError, KeyError, ValueError):
        pass
    return None


def create_ssl_context():
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.check_hostname = False
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
    # ssl_context.verify_flags = ssl.VERIFY_CRL_CHECK_CHAIN
    ssl_context.load_verify_locations(cafile=CA_FILE)
    ssl_context.load_cert_chain(certfile=CERT_FILE, keyfile=KEY_FILE)
    return ssl_context


async def connect():
    ssl_context = create_ssl_context()
    print(ssl_context.get_ca_certs())
    loop = asyncio.get_running_loop()
    on_con_lost = loop.create_future()    
    transport, protocol = await loop.create_connection(
        lambda: HelloProtocol(on_con_lost),
        host=HOST,
        port=PORT,
        ssl=ssl_context,
        ssl_handshake_timeout=SSL_HANDSHAKE_TIMEOUT,
    )
    try:
        await on_con_lost
    finally:
        transport.close()    


asyncio.run(connect())
