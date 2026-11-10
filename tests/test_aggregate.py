from aggregate import Candle, window_start


def test_window_start_truncates_to_minute():
    w = window_start("2026-11-09T12:34:56.789012Z")
    assert (w.minute, w.second, w.microsecond) == (34, 0, 0)


def test_candle_ohlcv():
    c = Candle.from_trade(100, 1)
    c.add(105, 2)
    c.add(98, 1)
    assert (c.open, c.high, c.low, c.close) == (100, 105, 98, 98)
    assert c.volume == 4
    assert c.count == 3


def test_single_trade_candle():
    c = Candle.from_trade(50, 2)
    assert c.open == c.high == c.low == c.close == 50
    assert c.count == 1
