import heapq
from datetime import datetime
import copy


class Book:
    """
    Limit order book.
    Orders are managed through open_order and remove_order methods.
    """
    def __init__(self):
        self._bid = []
        self._ask = []
        self._order_by_price_idx = {}

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
        heapq.heappush(table, order)
        self._update_price_qty(order.side, order.price, order.qty)
        return self._try_match_order(order)

    def remove_order(self, clientid: str, orderid: str):
        """
        :param orderid: Order_id assigned by client during opening.
        :return: The removed order as Book.Order.
        """
        for side in ['BUY', 'SELL']:
            orders = self._get_table(side)
            for idx, order in enumerate(orders):
                if order.clientid == clientid and order.id == orderid:
                    del orders[idx]
                    self._update_price_qty(side, order.price, -order.qty)
                    return order

    def _try_match_order(self, opened_order):
        filled = []
        if len(self._bid) == 0 or len(self._ask) == 0:
            return opened_order, filled
        table = self._get_table(self._opposite(opened_order.side))

        while len(table) > 0 and not opened_order < table[0] and opened_order.qty > 0: # We use our comparator defined below. todo: can we use > ?
            # It's a match
            qty = min(opened_order.qty, table[0].qty)
            price = opened_order.price if opened_order.side == "BUY" else table[0].price
            print("Matched orders at {0} ({1}x).".format(price, qty))

            # Opened order
            opened_order.qty -= qty
            opened_order.price_traded = price
            self._update_price_qty(opened_order.side, opened_order.price, -qty)
            if opened_order.qty == 0:
                    heapq.heappop(self._get_table(opened_order.side))

            # Matched orders
            order_report = copy.copy(table[0])
            order_report.qty = qty
            order_report.price_traded = price
            filled.append(order_report)
            self._update_price_qty(order_report.side, order_report.price, -qty)
            if table[0].qty == qty:
                heapq.heappop(table)

        return opened_order, filled

    def get_price_qty(self, side: str, price: int):
        try:
            return self._order_by_price_idx[(side, price)]
        except KeyError:
            return 0

    def _update_price_qty(self, side: str, price: int, qty: int):
        try:
            self._order_by_price_idx[(side, price)] += qty
            if self._order_by_price_idx[(side, price)] == 0:
                del self._order_by_price_idx[(side, price)]
        except KeyError:
            self._order_by_price_idx[(side, price)] = qty


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
        if self.side == "BUY":
            if self.price < other.price:
                return True
        else:
            if self.price > other.price:
                return True
        return self.time < other.time
