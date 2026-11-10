import json, logging, os, time
from datetime import datetime, timedelta, timezone

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from aggregate import Candle, window_start

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("processor")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = os.getenv("TOPIC", "trades")
DLQ = os.getenv("DLQ_TOPIC", "trades.dlq")
DSN = os.getenv("PG_DSN", "postgresql://app:change-me@localhost:5432/market")
GRACE = timedelta(seconds=5)
FLUSH_EVERY = 10  # seconds

UPSERT = """
INSERT INTO candles (symbol, window_start, open, high, low, close, volume, trade_count)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (symbol, window_start) DO UPDATE SET
  open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
  close=EXCLUDED.close, volume=EXCLUDED.volume, trade_count=EXCLUDED.trade_count
"""

def flush_closed(windows, conn):
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=1) - GRACE
    closed = [k for k in windows if k[1] <= cutoff]
    if not closed:
        return
    rows = [(s, w, c.open, c.high, c.low, c.close, c.volume, c.count)
            for (s, w) in closed for c in [windows[(s, w)]]]
    with conn.cursor() as cur:
        cur.executemany(UPSERT, rows)
    conn.commit()
    for k in closed:
        del windows[k]
    log.info("flushed %d candles", len(closed))

def main():
    conn = psycopg2.connect(DSN)
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "candle-builder",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    dlq = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe([TOPIC])
    windows: dict = {}
    last_flush = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error(msg.error())
                else:
                    try:
                        e = json.loads(msg.value())
                        key = (e["symbol"], window_start(e["ts"]))
                        price, size = float(e["price"]), float(e["size"])
                        if key in windows:
                            windows[key].add(price, size)
                        else:
                            windows[key] = Candle.from_trade(price, size)
                    except Exception as ex:
                        log.warning("bad message -> DLQ: %s", ex)
                        dlq.produce(DLQ, value=msg.value(), headers=[("error", str(ex))])
                        dlq.poll(0)

            if time.time() - last_flush >= FLUSH_EVERY:
                flush_closed(windows, conn)
                try:
                    consumer.commit(asynchronous=False)
                except KafkaException as ex:
                    log.debug("nothing to commit: %s", ex)
                last_flush = time.time()
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()
