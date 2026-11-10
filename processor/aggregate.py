from dataclasses import dataclass
from datetime import datetime

def window_start(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return dt.replace(second=0, microsecond=0)

@dataclass
class Candle:
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    count: int = 0

    @classmethod
    def from_trade(cls, price: float, size: float) -> "Candle":
        return cls(price, price, price, price, size, 1)

    def add(self, price: float, size: float) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += size
        self.count += 1
