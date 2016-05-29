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
        filled = self.book.open_order(order)
        if self.fill_callback:
            for order in filled:
                await self.fill_callback(order.clientid, order.id, order.price, order.qty)
        if self.datastream_callback:
            await self.datastream_callback("orderbook", order.side, order.time, order.price, order.qty)
            if len(filled) > 0:
                matched = filled[0]
                await self.datastream_callback("trade", None, order.time, matched.price, matched.qty)

    async def cancel_order(self, orderid: str) -> None:
        order = self.book.remove_order(orderid)
        if self.datastream_callback:
            await self.datastream_callback("cancel", order.side, order.time, order.price, order.qty)


    def set_callbacks(self, fill, datastream) -> None:
        self.fill_callback = fill
        self.datastream_callback = datastream