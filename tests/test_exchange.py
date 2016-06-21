from unittest import TestCase
import asyncio
from decimal import Decimal

from exchange import exchange


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

        self.assertTrue(len(e.book._bid) == 1, "Bid order missing")
        self.assertTrue(len(e.book._ask) == 0, "Ask table not cleaned")
        self.assertEqual(e.book._bid[0].price, Decimal(150), "Bid order has wrong price")
        self.assertEqual(e.book._bid[0].qty, 100, "Bid order has wrong qty")

    def test_sell_order_price_not_changed(self):
        loop = asyncio.get_event_loop()
        e = exchange.Exchange()
        tasks = [
            e.open_order("123", 0, "BUY", Decimal(150), 100),
            e.open_order("124", 1, "SELL", Decimal(149), 200),
        ]
        loop.run_until_complete(asyncio.wait(tasks))

        self.assertEqual(e.book._ask[0].price, Decimal(149), "Ask order price changed after filling")
        self.assertEqual(e.book._ask[0].qty, 100, "Sell order has wrong price")

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

    def _execute_and_get_reports(self, exchange_obj, tasks):
        loop = asyncio.get_event_loop()
        fill_report = []
        datastream_report = []

        async def fill_callback(*args):
            print("Fill callback received:", args)
            fill_report.append(args)

        async def datastream_callback(*args):
            print("Datastream callback received:", args)
            datastream_report.append(args)

        exchange_obj.set_callbacks(fill_callback, datastream_callback)

        for t in tasks:  # Run tasks one by one to ensure reports order
            loop.run_until_complete(asyncio.wait([t]))

        return fill_report, datastream_report

    def test_fill_callback(self):
        e = exchange.Exchange()
        tasks = [
            e.open_order("223", 0, "BUY", Decimal(150), 200),
            e.open_order("334", 1, "SELL", Decimal(149), 100),
        ]

        fill_report, datastream_report = self._execute_and_get_reports(e, tasks)

        self.assertIn((1, '334', Decimal('150'), 100), fill_report)
        self.assertIn((0, '223', Decimal('150'), 100), fill_report)
        self.assertEqual(len(fill_report), 2, "Fill callback returned redundant items.")

    def test_datastream_callback(self):
        e = exchange.Exchange()
        tasks = [
            e.open_order("223", 0, "BUY", Decimal(150), 200),
            e.open_order("334", 1, "SELL", Decimal(149), 100),
        ]

        (fill_report, datastream_report) = self._execute_and_get_reports(e, tasks)

        price_and_qty = [x[3:] for x in datastream_report]  # Ignore type, side and datetime
        self.assertListEqual(price_and_qty,
                         [(Decimal('150'), 200),
                         (Decimal('150'), 100),
                         (Decimal('150'), 100)])

    # Future work: implement more tests. Not all funcionality and error cases are covered.