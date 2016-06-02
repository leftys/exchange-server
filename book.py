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
        self._bid_by_price_idx = {}
        self._ask_by_price_idx = {}
        pass

    def _opposite(self, side: str):
        if side == "BUY":
            return "SELL"
        if side == "SELL":
            return "BUY"

    def _get_heap(self, side: str):
        assert side in ["BUY", "SELL"]
        if side == "BUY":
            return self._bid
        elif side == "SELL":
            return self._ask

    def _get_index(self, side: str):
        assert side in ["BUY", "SELL"]
        if side == "BUY":
            return self._bid_by_price_idx
        elif side == "SELL":
            return self._ask_by_price_idx

    def open_order(self, order):
        """
        :param order: Book.Order object
        :return: filled orders as a list of Book.Order objects.
        """
        heap = self._get_heap(order.side)
        heapq.heappush(heap, order)
        self._update_price_index(order.side, order.price,order.qty)
        return self._try_match_order(order)

    def remove_order(self, orderid: str):
        """
        :param orderid: Order_id assigned by client during opening.
        :return: The removed order as Book.Order.
        """
        for side in ["BUY", "SELL"]:
            heap = self._get_heap(side)
            for idx, order in enumerate(heap):
                if order.id == orderid:
                    # Remove element from heap, runs in O(n)
                    heap[idx] = heap[-1]
                    heap.pop()
                    heapq.heapify(heap)
                    self._update_price_index(side,order.price,-order.qty)
                    return order

    def _try_match_order(self, opened_order):
        filled = []
        if len(self._bid) == 0 or len(self._ask) == 0:
            return opened_order, filled
        heap = self._get_heap(self._opposite(opened_order.side))

        while len(heap) > 0 and not opened_order < heap[0]: # We use our comparator defined below. todo: can we use > ?
            # It's a match
            qty = min(opened_order.qty, heap[0].qty)
            price = opened_order.price if opened_order.side == "BUY" else heap[0].price
            print("Matched orders at {0} ({1}x).".format(price, qty))

            # Opened order
            opened_order.qty -= qty
            opened_order.price_traded = price
            if opened_order.qty == 0:
                    heap_opened_order = self._get_heap(opened_order.side)
                    heapq.heappop(heap_opened_order)

            # Matched orders
            order_report = copy.copy(heap[0])
            order_report.qty = qty
            order_report.price_traded = price
            filled.append(order_report)

        return opened_order, filled

    def get_qty_at_price(self, side: str, price: int):
        prince_index = self._get_index(side)
        return prince_index[price]

    def _update_price_index(self, side:str, price: int, qty: int):
        price_index = self._get_index(side)
        if price in price_index:
            price_index[price] += qty
        else:
            price_index[price] = qty
        #print("Price indexes",self._bid_by_price_idx,self._ask_by_price_idx)
        assert price_index[price] >= 0


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
