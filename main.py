import asyncio
import ssl

CERT_FILE = '/home/evgeni/src/swpt_ca_scripts/server.crt'
CA_FILE = '/home/evgeni/src/swpt_ca_scripts/root-ca.crt'
KEY_FILE = '/home/evgeni/src/swpt_ca_scripts/server.key'
HOST=None
PORT=5050
BACKLOG=100
SSL_HANDSHAKE_TIMEOUT=5.0


class EchoProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        peercert = transport.get_extra_info('peercert')
        print('Connection from {}'.format(peername))
        print('Certificate serialNumber: {}'.format(get_cert_common_name(peercert)))
        self.transport = transport

    def data_received(self, data):
        message = data.decode()
        print('Data received: {!r}'.format(message))

        print('Send: {!r}'.format(message))
        self.transport.write(data)

        print('Close the client socket')
        self.transport.close()


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
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
    # ssl_context.verify_flags = ssl.VERIFY_CRL_CHECK_CHAIN
    ssl_context.load_verify_locations(cafile=CA_FILE)
    ssl_context.load_cert_chain(certfile=CERT_FILE, keyfile=KEY_FILE)
    return ssl_context


async def serve():
    ssl_context = create_ssl_context()
    print(ssl_context.get_ca_certs())
    loop = asyncio.get_running_loop()
    server = await loop.create_server(
        EchoProtocol,
        host=HOST,
        port=PORT,
        backlog=BACKLOG,
        ssl=ssl_context,
        ssl_handshake_timeout=SSL_HANDSHAKE_TIMEOUT,
    )
    async with server:
        await server.serve_forever()


    
# if __name__ == '__main__':
#     asyncio.run(serve())

asyncio.run(serve())

