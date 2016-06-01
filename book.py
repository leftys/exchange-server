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
        pass

    def open_order(self, order):
        """
        :param order: Book.Order object
        :return: filled orders as a list of Book.Order objects.
        """
        assert order.side in ["BUY", "SELL"]
        if order.side == "BUY":
            heap = self._bid
        elif order.side == "SELL":
            heap = self._ask
        heapq.heappush(heap, order)
        return self._try_match_order()

    def remove_order(self, orderid: str):
        """
        :param orderid: Order_id assigned by client during opening.
        :return: The removed order as Book.Order.
        """
        for heap in [self._bid, self._ask]:
            for idx, order in enumerate(heap):
                if order.id == orderid:
                    # Remove element from heap, runs in O(n)
                    heap[idx] = heap[-1]
                    heap.pop()
                    heapq.heapify(heap)
                    return order

    def _try_match_order(self):
        filled = []
        if len(self._bid) == 0 or len(self._ask) == 0:
            return filled
        bid = self._bid[0]
        ask = self._ask[0]

        while bid.price >= ask.price:
            qty = min(bid.qty, ask.qty)
            price = bid.price
            print("Matched orders at {0} ({1}x).".format(price, qty))
            for (order, heap) in [(bid, self._bid), (ask, self._ask)]:
                order.qty -= qty
                if order.qty == 0:
                    heapq.heappop(heap)

                order_report = copy.copy(order)
                order_report.qty = qty
                order_report.price = price
                filled.append(order_report)

            if len(self._bid) == 0 or len(self._ask) == 0:
                return filled
            bid = self._bid[0]
            ask = self._ask[0]
        return filled


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
