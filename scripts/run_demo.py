#!/usr/bin/env python3
"""Send synthetic events through HTTP/Kafka/Spark, then measure returned decisions."""
import argparse
import json
import sys
import time
import uuid
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "processor"))
from generate import generate


def metrics(labels, predictions):
    tp = sum(a == 1 and b == 1 for a, b in zip(labels, predictions))
    fp = sum(a == 0 and b == 1 for a, b in zip(labels, predictions))
    tn = sum(a == 0 and b == 0 for a, b in zip(labels, predictions))
    fn = sum(a == 1 and b == 0 for a, b in zip(labels, predictions))
    precision = tp / (tp + fp) if tp + fp else 0
    recall = tp / (tp + fn) if tp + fn else 0
    return {"tp": tp, "fp": fp, "tn": tn, "fn": fn,
            "precision": precision, "recall": recall,
            "f1": 2 * precision * recall / (precision + recall) if precision + recall else 0,
            "false_positive_rate": fp / (fp + tn) if fp + tn else 0}


def request(base, path, payload=None):
    req = Request(base + path, data=json.dumps(payload).encode() if payload else None,
                  headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=20) as response:
        return json.load(response)


def percentile(values, percentile_value):
    values = sorted(values)
    return values[max(0, (len(values) * percentile_value + 99) // 100 - 1)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://localhost:3000")
    parser.add_argument("--cards", type=int, default=2)
    parser.add_argument("--per-card", type=int, default=80)
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()
    if args.cards < 1 or args.per_card < 75 or args.cards * args.per_card > 1800:
        parser.error("Use 1+ cards, 75+ events per card, and <=1800 total events")
    run_id = uuid.uuid4().hex[:8]
    truth = {}
    sent_at = time.monotonic()
    for event in generate(seed=9845, cards=args.cards, per_card=args.per_card):
        event["cardId"] = f"tok-{run_id}-{event['cardId'].split('-')[1]}"
        answer = request(args.api, "/api/transactions", event)
        truth[answer["eventId"]] = event["groundTruth"]
    send_seconds = time.monotonic() - sent_at
    deadline = time.monotonic() + args.timeout
    decisions = {}
    while len(decisions) < len(truth) and time.monotonic() < deadline:
        for item in request(args.api, "/api/recent")["items"]:
            if item["eventId"] in truth:
                decisions[item["eventId"]] = item
        if len(decisions) < len(truth):
            time.sleep(2)
    paired = [(truth[key], decisions[key]) for key in truth if key in decisions]
    labels = [label for label, _ in paired]
    predictions = [int(row["decision"] == "BLOCKED") for _, row in paired]
    latency = [max(0, row["processedAt"] - row["ingestedAt"]) for _, row in paired]
    result = {
        "run_id": run_id, "synthetic_events_sent": len(truth), "decisions_observed": len(paired),
        "ingestion_seconds": round(send_seconds, 2),
        "ingestion_events_per_second": round(len(truth) / send_seconds, 2),
        "decision_counts": {state: sum(row["decision"] == state for _, row in paired)
                            for state in ("APPROVED", "REVIEW", "BLOCKED")},
        "classification": metrics(labels, predictions) if paired and len(set(labels)) == 2 else None,
        "ingestion_to_spark_decision_p50_ms": percentile(latency, 50) if latency else None,
        "ingestion_to_spark_decision_p95_ms": percentile(latency, 95) if latency else None,
        "circuit_breaker_events": sum(row["circuitBreaker"] for _, row in paired),
        "note": "Pipeline time excludes Socket.IO and browser render; synthetic eventAt is historical.",
    }
    output = Path("artifacts") / f"live-run-{run_id}.json"
    output.parent.mkdir(exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    print("Saved", output)
    if len(paired) != len(truth):
        raise SystemExit("Not all decisions arrived before timeout; inspect docker compose logs processor")


if __name__ == "__main__":
    main()
