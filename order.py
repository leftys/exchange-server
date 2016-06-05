from datetime import datetime


class Order:
    """
    Buy/sell order with defined "<" operator.
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

    def __lt__(self, other) -> bool:
        if self.price == other.price:
            return self.time < other.time
        if self.side == "BUY":
            return self.price > other.price
        else:
            return self.price < other.price

    def __repr__(self) -> str:
        return "%s: %d@%d" % (self.side, self.qty, self.price)
