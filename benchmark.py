#!/usr/bin/env python3.5
from typing import Any, Dict
import asyncio
import json
import sys
import random


async def read_incoming_data(reader):
    if reader.at_eof():
        raise StopAsyncIteration()
    while not reader.at_eof():
        await reader.readline()


async def benchmark(host, port, max_orders=0, sleep_time=0):
    reader, writer = await asyncio.open_connection(host, port)
    incoming = asyncio.ensure_future(read_incoming_data(reader))

    num_orders = 0
    try:
        while num_orders < max_orders or max_orders == 0:
            num_orders += 1
            order_id = random.randint(0, 10**6)
            side = random.choice(['BUY', 'SELL'])
            price = int(random.gauss(100, 10))
            quantity = int(random.gauss(100, 10))
            await send_message(writer, {
                'message': 'createOrder',
                'orderId': order_id,
                'side': side,
                'price': str(price),
                'quantity': quantity,
            })
            if reader.at_eof():
                break
        await asyncio.sleep(sleep_time)
    except KeyboardInterrupt:
        print("Ctrl-C pressed, aborting.")
    finally:
        writer.close()
        incoming.cancel()
        print("Orders opened: ", num_orders)


async def network_benchmark(host, port, max_orders=0, sleep_time=0):
    reader, writer = await asyncio.open_connection(host, port)
    incoming = asyncio.ensure_future(read_incoming_data(reader))

    num_orders = 0
    side = "BUY"
    try:
        while num_orders < max_orders or max_orders == 0:
            num_orders += 1
            order_id = random.randint(0, 10**6)
            side = "SELL" if side == "BUY" else "BUY"
            price = 101 if side == "BUY" else 100
            quantity = 10
            await send_message(writer, {
                'message': 'createOrder',
                'orderId': order_id,
                'side': side,
                'price': str(price),
                'quantity': quantity,
            })
        await asyncio.sleep(sleep_time)
    except KeyboardInterrupt:
        print("Ctrl-C pressed, aborting.")
    finally:
        writer.close()
        incoming.cancel()
        print("Orders opened: ", num_orders)


async def send_message(writer: asyncio.StreamWriter, msg: Dict[str, Any]) -> None:
    """
    Encode and send a message to the server.
    """
    data = json.dumps(msg)
    writer.write(data.encode('utf-8'))
    writer.write(b'\n')
    await writer.drain()


async def main():
    assert len(sys.argv) == 3 or len(sys.argv) == 4, \
        'Usage: client-participant.py PrivateChannelHostname PrivateChannelPort [net]'
    host = sys.argv[1]
    port = int(sys.argv[2])
    if len(sys.argv) == 4 and sys.argv[3] == 'net':
        await network_benchmark(host, port)
    else:
        await benchmark(host, port)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
