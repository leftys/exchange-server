from unittest import TestCase
import exchange
import asyncio
from server import OrderServer, DatastreamServer
import benchmark


class TestServer(TestCase):
    def test_order_benchmark(self):
        loop = asyncio.get_event_loop()
        e = exchange.Exchange()
        order_server = OrderServer("localhost", 7001, e)
        datastream_server = DatastreamServer("localhost", 7002, e)
        e.set_callbacks(order_server.fill_order_report, datastream_server.send_datastream_report)
        order_server.start(loop)
        datastream_server.start(loop)
        loop.run_until_complete(asyncio.wait([
            benchmark.benchmark("localhost", 7001, 1000, 0.01)
        ]))
        order_server.stop(loop)
        datastream_server.stop(loop)
        e.print_stats()
        self.assertEqual(e.stats["opened"], 1000)
        self.assertGreater(e.stats["traded"], 500)


    def test_order_network_benchmark(self):
        loop = asyncio.get_event_loop()
        e = exchange.Exchange()
        order_server = OrderServer("localhost", 7001, e)
        datastream_server = DatastreamServer("localhost", 7002, e)
        e.set_callbacks(order_server.fill_order_report, datastream_server.send_datastream_report)
        order_server.start(loop)
        datastream_server.start(loop)
        loop.run_until_complete(asyncio.wait([
            benchmark.network_benchmark("localhost", 7001, 1000, 0.01)
        ]))
        order_server.stop(loop)
        datastream_server.stop(loop)
        e.print_stats()
        self.assertEqual(e.stats["opened"], 1000)
        self.assertGreater(e.stats["traded"], 500)