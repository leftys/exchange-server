#!/usr/bin/env python3.5
#
# Simulate a simple market participant.
#
# Usage: client-participant.py PrivateChannelHostname PrivateChannelPort
#
# http://codingchallenge.wood.cz/


from typing import Any, Dict
import asyncio
import json
import sys
import random


async def benchmark(host, port, max_orders=0, sleep_time=0):
    reader, writer = await asyncio.open_connection(host, port)
    loop = asyncio.get_event_loop()

    # Log incoming data
    # bgLogger = asyncio.ensure_future(readIncomingData(reader))

    num_orders = 0
    try:
        while (num_orders < max_orders or max_orders == 0):
            num_orders += 1
            order_id = random.randint(0,10**6)
            side = random.choice(['BUY','SELL'])
            price = int(random.gauss(100,10))
            quantity = int(random.gauss(100,10))
            await sendMessage(writer, {
                'message': 'createOrder',
                'orderId': order_id,
                'side': side,
                'price': price,
                'quantity': quantity,
            })
        await asyncio.sleep(sleep_time)
    except KeyboardInterrupt:
        print("Ctrl-C pressed, aborting.")
    finally:
        # bgLogger.cancel()
        writer.close()
        print("Orders opened: ",num_orders)


# async def readIncomingData(reader: asyncio.StreamReader) -> None:
#    async for line in utils.LineReader(reader):
#        pass


async def sendMessage(writer: asyncio.StreamWriter, msg: Dict[str, Any]) -> None:
    ''' Encode and send a message to the server. '''
    data = json.dumps(msg)
    #print('\n<{!s} sending {!r}>\n'.format(datetime.datetime.now(), data))
    writer.write(data.encode('utf-8') + b'\n')
    await writer.drain()

async def main():
    # Connect to the server
    assert len(sys.argv) == 3, 'Usage: client-participant.py PrivateChannelHostname PrivateChannelPort'
    host = sys.argv[1]
    port = int(sys.argv[2])
    await benchmark(host, port)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
