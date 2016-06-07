from unittest import TestCase
import exchange
import asyncio
from server import OrderServer, DatastreamServer
import benchmark


class TestExchange(TestCase):
    def test_get_clientid(self):
        e = exchange.Exchange()
        id1 = e.get_clientid()
        id2 = e.get_clientid()
        self.assertLess(id1, id2)

    def test_open_order(self):
        loop = asyncio.get_event_loop()
        e = exchange.Exchange()
        tasks = [
            e.open_order("123", 0, "BUY", 150, 200),
            e.open_order("234", 1, "SELL", 149, 100),
        ]
        loop.run_until_complete(asyncio.wait(tasks))
        self.assertTrue(len(e.book._ask) == 0, "Ask table not resolved")
        self.assertTrue(len(e.book._bid) == 1, "Bid order deleted")

    def test_cancel_order(self):
        loop = asyncio.get_event_loop()
        e = exchange.Exchange()
        loop.run_until_complete(e.open_order("123", 0, "BUY", 150, 200))
        loop.run_until_complete(e.cancel_order(0, "123"))
        self.assertEqual(len(e.book._bid), 0, "Order was not canceled")

    def test_set_callbacks(self):
        loop = asyncio.get_event_loop()
        e = exchange.Exchange()
        self.fill_report = []
        self.datastream_report = []
        e.set_callbacks(self.fill_callback, self.datastream_callback)
        tasks = [
            e.open_order("223", 0, "BUY", 150, 200),
            e.open_order("334", 1, "SELL", 149, 100),
        ]
        loop.run_until_complete(asyncio.wait(tasks))
        self.assertEqual(len(self.fill_report), 2, "Fill callback failed")
        self.assertEqual(self.fill_report[0][2], 150, "Order matched at wrong price")
        self.assertGreaterEqual(len(self.datastream_report), 3, "Datastream callback failed")
        # todo: more tests. report order may vary!

    async def fill_callback(self, *args):
        print("Fill callback received:", args)
        self.fill_report.append(args)

    async def datastream_callback(self, *args):
        print("Datastream callback received:", args)
        self.datastream_report.append(args)

    def test_benchmark(self):
        loop = asyncio.get_event_loop()
        e = exchange.Exchange()
        order_server = OrderServer("localhost", 7001, e)
        datastream_server = DatastreamServer("localhost", 7002, e)
        e.set_callbacks(order_server.fill_order_report, datastream_server.send_datastream_report)
        order_server.start(loop)
        datastream_server.start(loop)
        loop.run_until_complete(asyncio.wait([
            benchmark.benchmark("localhost",7001,1000,0.01)
        ]))
        order_server.stop(loop)
        e.print_stats()
        self.assertEqual(e.stats["opened"], 1000)
        self.assertGreater(e.stats["traded"], 500)
