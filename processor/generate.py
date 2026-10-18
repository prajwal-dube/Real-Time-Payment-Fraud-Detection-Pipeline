"""Deterministic synthetic card histories with injected velocity/travel anomalies."""
import argparse
import json
import random
from pathlib import Path

LOCATIONS = [("US", 40.71, -74.01), ("IN", 12.97, 77.59),
             ("GB", 51.51, -0.13), ("SG", 1.35, 103.82), ("DE", 52.52, 13.40)]


def generate(seed=2026, cards=120, per_card=90):
    rng = random.Random(seed)
    start = 1_700_000_000_000  # Fixed epoch keeps generated fixtures reproducible.
    events = []
    for card in range(cards):
        country, home_lat, home_lon = LOCATIONS[card % len(LOCATIONS)]
        clock = start + card * 1000
        for index in range(per_card):
            attack = index in range(36, 41) or index in range(70, 75)
            clock += rng.randint(2500, 8500) if attack else rng.randint(95_000, 240_000)
            if attack:
                remote = LOCATIONS[(card + 2) % len(LOCATIONS)]
                location = remote
                amount = round(rng.uniform(400, 1800), 2)
            else:
                location = (country, home_lat + rng.uniform(-.08, .08), home_lon + rng.uniform(-.08, .08))
                amount = round(rng.uniform(4, 110), 2)
                if rng.random() < .02:  # legitimate but unusual purchase
                    amount = round(rng.uniform(300, 700), 2)
            events.append({
                "eventId": f"sim-{seed}-{card:04d}-{index:04d}",
                "cardId": f"tok-{card:04d}", "eventAt": clock,
                "amount": amount, "country": location[0],
                "lat": round(location[1], 5), "lon": round(location[2], 5),
                "groundTruth": int(attack),
            })
    return sorted(events, key=lambda e: (e["eventAt"], e["eventId"]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--cards", type=int, default=120)
    parser.add_argument("--per-card", type=int, default=90)
    parser.add_argument("--output", default="artifacts/synthetic-events.jsonl")
    args = parser.parse_args()
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as output:
        for event in generate(args.seed, args.cards, args.per_card):
            output.write(json.dumps(event, separators=(",", ":")) + "\n")
    print(f"Wrote {args.cards * args.per_card} synthetic events to {path}")
