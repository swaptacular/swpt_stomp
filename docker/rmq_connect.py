import sys
import asyncio
import aio_pika

if __name__ == '__main__':
    asyncio.run(aio_pika.connect(sys.argv[1]))
