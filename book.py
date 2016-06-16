import heapq
from typing import Tuple, List
from decimal import Decimal
import copy

from order import Order


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

    def open_order(self, order: Order):
        """
        :param order: Order object
        :return: filled orders as a list of Order objects.
        """
        table = self._get_table(order.side)
        assert len(self._ask) == 0 or len(self._bid) == 0 or self._ask[0].price >= self._bid[0].price
        heapq.heappush(table, order)
        self._update_price_qty(order.side, order.price, order.qty)
        return self._try_match_order(order)

    def remove_order(self, clientid: str, orderid: str) -> Order:
        """
        :param clientid Client id. (Has to be specified to resolve orderid conflicts between clients.)
        :param orderid: Order_id assigned by client during opening.
        :return: The removed order.
        """
        for side in ['BUY', 'SELL']:
            orders = self._get_table(side)
            for idx, order in enumerate(orders):
                if order.clientid == clientid and order.id == orderid:
                    if len(orders) > 1:
                        orders[idx] = orders.pop()
                        heapq.heapify(orders)
                    else:
                        del orders[idx]
                    self._update_price_qty(side, order.price, -order.qty)
                    return order
        raise KeyError

    def _try_match_order(self, opened_order: Order) -> Tuple[Order, List[Order]]:
        filled = []
        if len(self._bid) == 0 or len(self._ask) == 0:
            return opened_order, filled
        table = self._get_table(self._opposite(opened_order.side))

        while len(table) > 0 and self._matches(opened_order, table[0]) and opened_order.qty > 0:
            assert self._get_table(opened_order.side)[0].id == opened_order.id

            # It's a match
            qty = min(opened_order.qty, table[0].qty)
            assert qty != 0
            price = opened_order.price if opened_order.side == "BUY" else table[0].price

            # Opened order
            opened_order.qty -= qty
            opened_order.price_traded = price
            self._update_price_qty(opened_order.side, opened_order.price, -qty)
            if opened_order.qty == 0:
                assert heapq.heappop(self._get_table(opened_order.side)).id == opened_order.id

            # Matched orders
            order_report = copy.copy(table[0])
            order_report.qty = qty
            order_report.price_traded = price
            filled.append(order_report)
            table[0].qty -= qty
            self._update_price_qty(order_report.side, order_report.price, -qty)

            if table[0].qty == 0:
                assert table[0].id == heapq.heappop(table).id
            assert len(table) == 0 or table[0].qty > 0
            assert len(self._get_table(opened_order.side)) == 0 or self._get_table(opened_order.side)[0].qty > 0

        return opened_order, filled

    def get_price_qty(self, side: str, price: Decimal) -> int:
        """
        :param side: Order side, "BUY" or "SELL".
        :param price: Price too look-up.
        :return: qty left on the specified side of order book.
        """
        try:
            return self._order_by_price_idx[(side, price)]
        except KeyError:
            return 0

    def _update_price_qty(self, side: str, price: Decimal, qty: int) -> None:
        try:
            self._order_by_price_idx[(side, price)] += qty
            if self._order_by_price_idx[(side, price)] == 0:
                del self._order_by_price_idx[(side, price)]
        except KeyError:
            self._order_by_price_idx[(side, price)] = qty

    def _matches(self, order1: Order, order2: Order) -> bool:
        if order1.side == "BUY":
            return order1.price > order2.price
        else:
            return order1.price < order2.price
