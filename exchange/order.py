from datetime import datetime
from decimal import Decimal


class Order:
    """
    Buy/sell order with defined "<" operator.
    """
    def __init__(self, id: str, clientid: int, side: str, price: Decimal, qty: int):
        assert side in ["BUY", "SELL"], "Side has to be BUY or SELL"
        assert qty > 0, "Quantity has to be positive"
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
        return "%s: %d@%f" % (self.side, self.qty, self.price)
    