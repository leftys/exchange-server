#!/usr/bin/env python3.5
#
# Simulate a simple market exchange server.

import asyncio
import signal
from server import Server
from exchange import Exchange


def _stop_server(signame: str, server: Server, loop: asyncio.AbstractEventLoop) -> None:
    print("Received signal %s: exiting." % signame)
    server.stop(loop)
    loop.stop()


# def wakeup(loop):
    # Call again
#    loop.call_later(0.1, wakeup, loop)


def main():
    loop = asyncio.get_event_loop()
    # loop.call_later(0.1, wakeup, loop)
    print("Starting server.")

    # create Exchange
    exchange = Exchange()

    # create a TCP server and starts listening
    server = Server('localhost', 7001, exchange)
    server.start(loop)

    # abort on Ctrl+C or TERM signal
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame), _stop_server, signame, server, loop)

    try:
        loop.run_forever()
    finally:
        exchange.print_stats()
        server.stop(loop)
        loop.close()


if __name__ == '__main__':
    main()
