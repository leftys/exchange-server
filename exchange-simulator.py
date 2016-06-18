#!/usr/bin/env python3.5
#
# Simple market exchange server.

import asyncio
import signal
import argparse

from exchange.server import OrderServer, DatastreamServer
from exchange.exchange import Exchange


def _stop_server(signame: str, loop: asyncio.AbstractEventLoop) -> None:
    """Stop upon receiving signal signame"""
    print("Received signal %s: exiting." % signame)
    loop.stop()


def _stats_wakeup(loop, exchange):
    """Prints stats every second"""
    exchange.print_stats()
    loop.call_later(1, _stats_wakeup, loop, exchange)


def main():
    loop = asyncio.get_event_loop()

    # Evaluate cmdline args
    parser = argparse.ArgumentParser("Stock exchange simulation server")
    parser.add_argument("--order-port", type=int, default=7001, help="Port of order/private channel")
    parser.add_argument("--datastream-port", type=int, default=7002, help="Port of datastream/public channel")
    parser.add_argument("--print-stats", action='store_true', help="Print statistics of open orders")
    args = parser.parse_args()

    # create Exchange
    exchange = Exchange()

    # create TCP servers and start listening
    order_server = OrderServer("localhost", args.order_port, exchange)
    datastream_server = DatastreamServer("localhost", args.datastream_port, exchange)
    exchange.set_callbacks(order_server.fill_order_report, datastream_server.send_datastream_report)
    try:
        order_server.start(loop)
        datastream_server.start(loop)
    except OSError as ex:
        exit("Cannot bind address. Is another server already started?\nFull message: %s" % ex.strerror)

    # abort on Ctrl+C or TERM signal
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame), _stop_server, signame, loop)

    # Print stats every second if requested
    if args.print_stats:
        loop.call_soon(_stats_wakeup, loop, exchange)

    print("Stock exchange simulation server started.")
    try:
        loop.run_forever()
    finally:
        if args.print_stats:
            exchange.print_stats()
        order_server.stop(loop)
        loop.close()


if __name__ == '__main__':
    main()
