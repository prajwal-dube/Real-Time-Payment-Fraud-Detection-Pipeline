"""One feature definition for training and live inference. No label is read here."""
from math import asin, cos, radians, sin, sqrt

FEATURE_NAMES = [
    "log_amount", "velocity_5m", "distance_km", "speed_kmh",
    "country_changed", "amount_ratio",
]


def distance_km(a_lat, a_lon, b_lat, b_lon):
    radius = 6371.0
    dlat, dlon = radians(b_lat - a_lat), radians(b_lon - a_lon)
    h = sin(dlat / 2) ** 2 + cos(radians(a_lat)) * cos(radians(b_lat)) * sin(dlon / 2) ** 2
    return 2 * radius * asin(min(1.0, sqrt(h)))


def enrich(event, previous=None):
    """Return (features, new card state). Events for a card must be in timestamp order."""
    from math import log1p
    state = previous or {"history": [], "risk_times": [], "blocked_until": 0}
    now = int(event["eventAt"])
    history = [int(t) for t in state.get("history", []) if now - 300_000 <= int(t) <= now]
    recent_count = len(history) + 1
    last = state.get("last")
    distance = distance_km(last["lat"], last["lon"], event["lat"], event["lon"]) if last else 0.0
    elapsed_hours = max(1.0 / 3600, (now - last["eventAt"]) / 3_600_000) if last else 1.0
    speed = min(distance / elapsed_hours, 2000.0)
    ratio = event["amount"] / max(float(state.get("mean_amount", 50.0)), 1.0)
    features = [
        log1p(event["amount"]),
        min(recent_count, 50),
        min(distance, 20_000.0),
        speed,
        int(bool(last) and last["country"] != event["country"]),
        min(ratio, 200.0),
    ]
    old_count = int(state.get("count", 0))
    old_mean = float(state.get("mean_amount", 50.0))
    new_state = {
        "history": (history + [now])[-50:],
        "last": {"lat": event["lat"], "lon": event["lon"], "country": event["country"], "eventAt": now},
        "count": old_count + 1,
        "mean_amount": (old_mean * old_count + event["amount"]) / (old_count + 1),
        "risk_times": [int(t) for t in state.get("risk_times", []) if now - 120_000 <= int(t) <= now],
        "blocked_until": int(state.get("blocked_until", 0)),
    }
    return features, new_state
