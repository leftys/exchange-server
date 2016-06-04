import book


class Exchange:
    def __init__(self): # loop
        self.next_clientid = 0
        self.book = book.Book()
        self.fill_callback = None
        self.datastream_callback = None
        # self.loop = loop

    def get_clientid(self) -> int:
        id = self.next_clientid
        self.next_clientid += 1
        return id

    async def open_order(self, orderid: str, clientid: int, side: str, price: int, qty: int) -> None:
        order = book.Order(orderid, clientid, side, price, qty)
        (order, filled) = self.book.open_order(order)
        order_was_traded = len(filled) > 0
        order_was_fully_traded = order.qty == 0
        if self.fill_callback and order_was_traded:
            for filled_order in [order] + filled:
                await self.fill_callback(filled_order.clientid, filled_order.id, filled_order.price_traded, filled_order.qty)
        if self.datastream_callback:
            if order_was_traded:
                # Notify about the conducted trade.
                await self.datastream_callback("trade", None, order.time, order.price, order.qty)

                # Notify about the changed rows of the limit order book.
                last_price = -1
                for changed_order in filled: # We may have already notified about the first order.
                    await self.datastream_callback("orderbook", changed_order.side, changed_order.time, changed_order.price,
                                                   self.book.get_price_qty(changed_order.side, changed_order.price))
            if not order_was_fully_traded:
                # Notify about the opened order only if it has not been fully traded and therefore remains in the book.
                # There are no more orders with the same price and side, as they would have get fulfilled already.
                qty_left = self.book.get_price_qty(order.side, order.price)
                await self.datastream_callback("orderbook", order.side, order.time, order.price, qty_left)

    async def cancel_order(self, clientid: str, orderid: str) -> None:
        order = self.book.remove_order(clientid, orderid)
        if self.datastream_callback:
            await self.datastream_callback("cancel", order.side, order.time, order.price, order.qty)

    def set_callbacks(self, fill, datastream) -> None:
        self.fill_callback = fill
        self.datastream_callback = datastream