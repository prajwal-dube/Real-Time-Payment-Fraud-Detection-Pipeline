"""Kafka -> Spark micro-batches -> per-card Redis state -> scored Kafka decisions."""
import json
import os
import time
import joblib
import redis
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
from features import enrich

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MODEL_PATH = os.getenv("MODEL_PATH", "/app/artifacts/model.joblib")
CHECKPOINT = os.getenv("CHECKPOINT", "/app/checkpoint")
pack = joblib.load(MODEL_PATH)  # Model is built locally in this image, never uploaded by API users.
model, threshold = pack["model"], pack["threshold"]
cache = redis.Redis.from_url(REDIS_URL, decode_responses=True)
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP.split(","),
                         value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode(),
                         acks="all", retries=5)


def score_event(event):
    event_id = event["eventId"]
    cached = cache.get("decision:" + event_id)
    if cached:
        return json.loads(cached)
    card = event["cardId"]
    saved = cache.get("card:" + card)
    features, state = enrich(event, json.loads(saved) if saved else None)
    probability = float(model.predict_proba([features])[0, 1])
    now = int(event["eventAt"])
    risk_times = state["risk_times"]
    if probability >= threshold:
        risk_times.append(now)
    active = now < state["blocked_until"]
    tripped = len(risk_times) >= 3
    if tripped:
        state["blocked_until"] = max(state["blocked_until"], now + 300_000)
    decision = "BLOCKED" if active or tripped or probability >= threshold else (
        "REVIEW" if probability >= .35 else "APPROVED")
    output = {
        "eventId": event_id, "cardId": card, "amount": event["amount"],
        "country": event["country"], "eventAt": now,
        "ingestedAt": event["ingestedAt"], "processedAt": int(time.time() * 1000),
        "probability": round(probability, 5), "decision": decision,
        "circuitBreaker": bool(active or tripped), "velocity5m": features[1],
        "distanceKm": round(features[2], 1),
    }
    # Atomic Redis commit enables deterministic replay of an already-scored event.
    # Kafka publish follows the commit; a replay republishes the cached decision.
    with cache.pipeline(transaction=True) as transaction:
        transaction.set("card:" + card, json.dumps(state), ex=7 * 24 * 3600)
        transaction.set("decision:" + event_id, json.dumps(output), ex=7 * 24 * 3600)
        transaction.execute()
    return output


def valid(event):
    return (event.get("eventId") and event.get("cardId") and
            isinstance(event.get("amount"), (int, float)) and 0 < event["amount"] <= 100_000 and
            isinstance(event.get("eventAt"), int) and isinstance(event.get("ingestedAt"), int) and
            isinstance(event.get("lat"), (int, float)) and -90 <= event["lat"] <= 90 and
            isinstance(event.get("lon"), (int, float)) and -180 <= event["lon"] <= 180 and
            isinstance(event.get("country"), str) and len(event["country"]) == 2)


def process_batch(batch, batch_id):
    for row in batch.orderBy("offset").toLocalIterator():
        event = row["parsed"].asDict() if row["parsed"] is not None else None
        if not event or not valid(event):
            producer.send("invalid-transactions", {"raw": row["raw"], "offset": row["offset"], "reason": "invalid schema"}).get(timeout=20)
            continue
        decision = score_event(event)
        producer.send("fraud-decisions", decision, key=event["cardId"].encode()).get(timeout=20)
    producer.flush()
    print(f"completed batch={batch_id}", flush=True)


def main():
    spark = SparkSession.builder.appName("fraud-pipeline").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    schema = StructType([
        StructField("eventId", StringType()), StructField("cardId", StringType()),
        StructField("amount", DoubleType()), StructField("eventAt", LongType()),
        StructField("ingestedAt", LongType()), StructField("country", StringType()),
        StructField("lat", DoubleType()), StructField("lon", DoubleType()),
        StructField("groundTruth", IntegerType()),
    ])
    source = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP) \
        .option("subscribe", "transactions").option("startingOffsets", "earliest").load()
    parsed = source.select(
        col("offset"), col("value").cast("string").alias("raw"),
        from_json(col("value").cast("string"), schema).alias("parsed"))
    query = parsed.writeStream.foreachBatch(process_batch).option("checkpointLocation", CHECKPOINT) \
        .trigger(processingTime="1 second").start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
