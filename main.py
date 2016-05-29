#!/usr/bin/env python3.5
#
# Simulate a simple market exchange server.

import asyncio
import signal
from server import Server
from exchange import Exchange


def _stop_server(signame: str, server: Server, loop: asyncio.AbstractEventLoop) -> None:
    print("Received signal %s: exiting." % signame)
    loop.stop()

def main():
    loop = asyncio.get_event_loop()
    print("Starting server.")

    # create Exchange
    exchange = Exchange()

    # create a TCP server and starts listening
    #todo: rename?
    server = Server('localhost', 7001, exchange)
    server.start(loop)

    # abort on Ctrl+C or TERM signal
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame), _stop_server, signame, server, loop)

    try:
        loop.run_forever()
    finally:
        loop.close()


if __name__ == '__main__':
    main()
