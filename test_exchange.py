from unittest import TestCase
import exchange
import asyncio
from decimal import Decimal


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
        print(e.book._bid)
        print(e.book._ask)
        self.assertTrue(len(e.book._ask) == 0, "Ask table not cleaned")
        self.assertTrue(len(e.book._bid) == 1, "Incomplete bid order deleted")

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

    def _open_orders_and_get_reports(self, exchange_obj, tasks):
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

        # Run tasks one by one to ensure reports order
        for t in tasks:
            loop.run_until_complete(asyncio.wait([t]))

        return fill_report, datastream_report

    def test_fill_callback(self):
        e = exchange.Exchange()
        tasks = [
            e.open_order("223", 0, "BUY", Decimal(150), 200),
            e.open_order("334", 1, "SELL", Decimal(149), 100),
        ]

        fill_report, datastream_report = self._open_orders_and_get_reports(e, tasks)

        self.assertIn((1, '334', Decimal('150'), 100), fill_report)
        self.assertIn((0, '223', Decimal('150'), 100), fill_report)
        self.assertEqual(len(fill_report), 2, "Fill callback returned redundant items.")

    def test_datastream_callback(self):
        e = exchange.Exchange()
        tasks = [
            e.open_order("223", 0, "BUY", Decimal(150), 200),
            e.open_order("334", 1, "SELL", Decimal(149), 100),
        ]

        (fill_report, datastream_report) = self._open_orders_and_get_reports(e, tasks)

        datastream_report # Ignore type, side and datetime
        self.assertEqual(price_and_qty, )
        self.assertEqual(len(datastream_report), 3, "Datastream callback returned redundant items.")

        # [('orderbook', 'BUY', datetime.datetime(2016, 6, 18, 12, 41, 39, 986376), Decimal('150'), 200),
        # ('trade', None, datetime.datetime(2016, 6, 18, 12, 41, 39, 987852), Decimal('150'), 100),
        # ('orderbook', 'BUY', datetime.datetime(2016, 6, 18, 12, 41, 39, 986376), Decimal('150'), 100)]
        # todo: check reports content

    # todo: more tests. report order may vary!
