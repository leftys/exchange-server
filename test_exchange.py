from unittest import TestCase
import exchange
import asyncio
from decimal import Decimal, getcontext


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
            e.open_order("123", 0, "BUY", Decimal(150), 200),
            e.open_order("234", 1, "SELL", Decimal(149), 100),
        ]
        loop.run_until_complete(asyncio.wait(tasks))
        self.assertTrue(len(e.book._ask) == 0, "Ask table not resolved")
        self.assertTrue(len(e.book._bid) == 1, "Bid order deleted")

    def test_decimal(self):
        loop = asyncio.get_event_loop()
        e = exchange.Exchange()
        tasks = [
            e.open_order("123", 0, "BUY", Decimal(1.000001), 200),
            e.open_order("124", 0, "BUY", Decimal(1.000003), 200),
            e.open_order("234", 1, "SELL", Decimal(1.000002), 400),
        ]
        loop.run_until_complete(asyncio.wait(tasks))
        self.assertEqual(len(e.book._ask), 1)
        self.assertEqual(len(e.book._bid), 1)

    def test_cancel_order(self):
        loop = asyncio.get_event_loop()
        e = exchange.Exchange()
        loop.run_until_complete(e.open_order("123", 0, "BUY", Decimal(150), 200))
        loop.run_until_complete(e.cancel_order(0, "123"))
        self.assertEqual(len(e.book._bid), 0, "Order was not canceled")

    def test_set_callbacks(self):
        loop = asyncio.get_event_loop()
        e = exchange.Exchange()
        fill_report = []
        datastream_report = []

        async def fill_callback(*args):
            print("Fill callback received:", args)
            fill_report.append(args)

        async def datastream_callback(*args):
            print("Datastream callback received:", args)
            datastream_report.append(args)

        e.set_callbacks(fill_callback, datastream_callback)
        tasks = [
            e.open_order("223", 0, "BUY", Decimal(150), 200),
            e.open_order("334", 1, "SELL", Decimal(149), 100),
        ]
        loop.run_until_complete(asyncio.wait(tasks))
        self.assertEqual(len(fill_report), 2, "Fill callback failed")
        self.assertEqual(fill_report[0][2], 150, "Order matched at wrong price")
        self.assertGreaterEqual(len(datastream_report), 3, "Datastream callback failed")
        # todo: more tests. report order may vary!
