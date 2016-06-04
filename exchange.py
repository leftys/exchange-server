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
        order, filled = self.book.open_order(order)
        order_was_traded = len(filled) > 0
        order_was_fully_traded = order.qty == 0
        if self.fill_callback:
            for order in filled:
                await self.fill_callback(order.clientid, order.id, order.price_traded, order.qty)
        if self.datastream_callback:
            if not order_was_fully_traded: # todo: bug
                # Notify about the opened order only if it has not been fully traded and therefore remains in the book.
                # There are no more orders with the same price and side, as they would have get fulfilled already.
                await self.datastream_callback("orderbook", order.side, order.time, order.price, order.qty)
            if order_was_traded:
                # Notify about the conducted trade.
                order_result = filled[0]
                await self.datastream_callback("trade", None, order.time, order_result.price, order_result.qty)

                # Notify about the changed rows of the limit order book.
                last_price = -1
                for changed_order in filled: # We may have already notified about the first order.
                    if (not order_was_fully_traded or changed_order.side != side) and changed_order.price_original != last_price: # Prices are sorted as the matching is done in price order.
                        await self.datastream_callback("orderbook", changed_order.side, changed_order.time, changed_order.price_original,
                                                       self.book.get_qty_at_price(changed_order.side, changed_order.price_original))

    async def cancel_order(self, clientid: str, orderid: str) -> None:
        order = self.book.remove_order(clientid, orderid)
        if self.datastream_callback:
            await self.datastream_callback("cancel", order.side, order.time, order.price, order.qty)

    def set_callbacks(self, fill, datastream) -> None:
        self.fill_callback = fill
        self.datastream_callback = datastream