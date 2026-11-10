import asyncio, json, logging, os
import websockets
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("producer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = os.getenv("TOPIC", "trades")
PRODUCTS = os.getenv("PRODUCTS", "BTC-USD,ETH-USD,SOL-USD").split(",")
URL = "wss://ws-feed.exchange.coinbase.com"

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "enable.idempotence": True,
    "linger.ms": 50,
    "compression.type": "lz4",
})

def on_delivery(err, msg):
    if err:
        log.error("delivery failed: %s", err)

async def run():
    backoff = 1
    while True:
        try:
            async with websockets.connect(URL, ping_interval=20) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": PRODUCTS,
                    "channels": ["matches"],
                }))
                log.info("connected")
                backoff = 1
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("type") not in ("match", "last_match"):
                        continue
                    event = {
                        "symbol": msg["product_id"],
                        "price": float(msg["price"]),
                        "size": float(msg["size"]),
                        "ts": msg["time"],
                        "trade_id": msg["trade_id"],
                    }
                    producer.produce(TOPIC, key=event["symbol"],
                                     value=json.dumps(event), on_delivery=on_delivery)
                    producer.poll(0)
        except Exception as e:
            log.warning("connection lost: %s; retrying in %ss", e, backoff)
            producer.flush(5)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

if __name__ == "__main__":
    asyncio.run(run())
