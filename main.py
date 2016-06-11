#!/usr/bin/env python3.5
#
# Simple market exchange server.

import asyncio
import signal

from server import OrderServer, DatastreamServer
from exchange import Exchange


def _stop_server(signame: str, loop: asyncio.AbstractEventLoop) -> None:
    print("Received signal %s: exiting." % signame)
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

    # create TCP servers and start listening
    order_server = OrderServer("localhost", 7001, exchange)
    datastream_server = DatastreamServer("localhost", 7002, exchange)
    exchange.set_callbacks(order_server.fill_order_report, datastream_server.send_datastream_report)
    try:
        order_server.start(loop)
        datastream_server.start(loop)
    except OSError as ex:
        exit("Cannot bind address. Is another server already started?\nFull message: %s" % ex.strerror)

    # abort on Ctrl+C or TERM signal
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame), _stop_server, signame, loop)

    try:
        loop.run_forever()
    finally:
        exchange.print_stats()
        order_server.stop(loop)
        loop.close()


if __name__ == '__main__':
    main()
