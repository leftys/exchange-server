#import heapq
from datetime import datetime
import copy


class Book:
    """
    Limit order book.
    Orders are managed through open_order and remove_order methods.
    """
    def __init__(self):
        self._bid = {}
        self._ask = {}
        self._order_by_id_idx = {}

    def _opposite(self, side: str):
        if side == "BUY":
            return "SELL"
        if side == "SELL":
            return "BUY"

    def _get_table(self, side: str):
        assert side in ["BUY", "SELL"]
        if side == "BUY":
            return self._bid
        elif side == "SELL":
            return self._ask

    def open_order(self, order):
        """
        :param order: Book.Order object
        :return: filled orders as a list of Book.Order objects.
        """
        table = self._get_table(order.side)
        try:
            orders = table[order.price]
            orders.append(order)
        except KeyError:
            table[order.price] = order
        self._order_by_id_idx[(order.clientid, order.id)] = (order.side, order.price)
        return self._try_match_order(order)

    def remove_order(self, clientid: str, orderid: str):
        """
        :param orderid: Order_id assigned by client during opening.
        :return: The removed order as Book.Order.
        """
        (side, price) = self._order_by_id_idx[(clientid, orderid)]
        orders = self._get_table(side)[price]
        for idx, order in enumerate(orders):
            if order.clientid == clientid and order.id == orderid:
                del orders[idx]
                del self._order_by_id_idx[(clientid, orderid)]
                assert orders == self._get_table(side)[price]
                return order

    def _try_match_order(self, opened_order):
        filled = []
        if len(self._bid) == 0 or len(self._ask) == 0:
            return opened_order, filled
        table = self._get_table(self._opposite(opened_order.side))

        while len(table) > 0 and not opened_order < table[0]: # We use our comparator defined below. todo: can we use > ?
            # It's a match
            qty = min(opened_order.qty, table[0].qty)
            price = opened_order.price if opened_order.side == "BUY" else table[0].price
            print("Matched orders at {0} ({1}x).".format(price, qty))

            # Opened order
            opened_order.qty -= qty
            opened_order.price_traded = price
            if opened_order.qty == 0:
                    table_opened_order = self._get_table(opened_order.side)
                    del table_opened_order[0]

            # Matched orders
            order_report = copy.copy(table[0])
            order_report.qty = qty
            order_report.price_traded = price
            filled.append(order_report)

        return opened_order, filled

    def get_price_qty(self, side: str, price: int):
        table = self._get_table(side)


class Order:
    """
    Buy/sell order
    """
    def __init__(self, id: str, clientid: int, side: str, price: int, qty: int):
        assert side in ["BUY", "SELL"]
        self.id = id
        self.clientid = clientid
        self.side = side
        self.price = price
        self.price_traded = None
        self.qty = qty
        self.time = datetime.now()

    def __lt__(self, other):
        assert self.side == other.side
        if self.side == "BUY":
            if self.price < other.price:
                return True
        else:
            if self.price > other.price:
                return True
        return self.time < other.time
