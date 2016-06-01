from unittest import TestCase
import exchange
import asyncio


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
        loop.run_until_complete(e.cancel_order("123"))
        self.assertEqual(len(e.book._bid), 0, "Order was not canceled")

    def test_set_callbacks(self):
        loop = asyncio.get_event_loop()
        e = exchange.Exchange()
        self.fill_report = []
        self.datastream_report = []
        e.set_callbacks(self.fill_callback, self.datastream_callback)
        tasks = [
            e.open_order("123", 0, "BUY", 150, 200),
            e.open_order("234", 1, "SELL", 149, 100),
        ]
        loop.run_until_complete(asyncio.wait(tasks))
        self.assertEqual(len(self.fill_report), 2, "Fill callback failed")
        self.assertEqual(self.fill_report[0][2], 150, "Order matched at wrong price")
        self.assertEqual(len(self.datastream_report), 3, "Datastream callback failed")
        self.assertEqual(self.datastream_report[2][3], 150, "Trade published with wrong price")

    async def fill_callback(self, *args):
        self.fill_report.append(args)

    async def datastream_callback(self, *args):
        self.datastream_report.append(args)
