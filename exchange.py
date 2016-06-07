import book
from typing import Callable
import datetime

class Exchange:
    """
    Trading logic. This class provides opening and closing orders in asynchronous way provides callbacks on different
    events.
    """

    def __init__(self):
        self.next_clientid = 0
        self.book = book.Book()
        self.fill_callback = None
        self.datastream_callback = None
        self.stats = {"opened": 0, "traded": 0}
        # self.loop = loop

    def get_clientid(self) -> int:
        """
        Returns next available client id
        """
        id = self.next_clientid
        self.next_clientid += 1
        return id

    async def open_order(self, orderid: str, clientid: int, side: str, price: int, qty: int) -> None:
        """
        Opens new trading order.
        :param orderid: string id unique for a client
        :param clientid: number of client
        :param side: order side, "BUY" or "SELL"
        :param price: desired price of order
        :param qty: desired amount of equity
        :return: None
        """
        order = book.Order(orderid, clientid, side, price, qty)
        (order, filled) = self.book.open_order(order)
        self.stats["opened"] += 1
        order_was_traded = len(filled) > 0
        order_was_fully_traded = order.qty == 0
        if order_was_traded:
            self.stats["traded"] += 2
        if self.fill_callback and order_was_traded:
            for filled_order in [order] + filled:
                await self.fill_callback(filled_order.clientid, filled_order.id, filled_order.price_traded,
                                         filled_order.qty)
        if self.datastream_callback:
            if order_was_traded:
                # Notify about the conducted trade.
                await self.datastream_callback("trade", None, order.time, order.price_traded, qty - order.qty)

                # Notify about the changed rows of the limit order book.
                for changed_order in filled:  # We may have already notified about the first order.
                    await self.datastream_callback("orderbook", changed_order.side, changed_order.time,
                                                   changed_order.price, self.book.get_price_qty(changed_order.side,
                                                                                                changed_order.price))
            if not order_was_fully_traded:
                # Notify about the opened order only if it has not been fully traded and therefore remains in the book.
                # There are no more orders with the same price and side, as they would have get fulfilled already.
                qty_left = self.book.get_price_qty(order.side, order.price)
                await self.datastream_callback("orderbook", order.side, order.time, order.price, qty_left)

    async def cancel_order(self, clientid: str, orderid: str) -> None:
        """
        Removes order
        :param clientid: string id unique for a client
        :param orderid: number of client
        :return: None
        """
        order = self.book.remove_order(clientid, orderid)
        if self.datastream_callback:
            await self.datastream_callback("cancel", order.side, order.time, order.price, order.qty)

    def set_callbacks(self, fill: Callable[[int, int, int, int], None], datastream: Callable[[str, str, datetime.time,
                                                                                              int, int], None])-> None:
        """
        Sets callbacks i.e. functions, which will be called when an order is opened, closed or traded.
        """
        self.fill_callback = fill
        self.datastream_callback = datastream

    def print_stats(self):
        """
        Prints statistics of server utilization e.g. number of opened or traded orders.
        """
        print("Opened orders:", self.stats["opened"])
        print("Traded orders:", self.stats["traded"])
        print("Leftover SELL orders:", len(self.book._ask))
        print("Leftover BUY orders:", len(self.book._bid))