"""Train on synthetic cards; select threshold on validation cards; report test cards once."""
import argparse
import json
from pathlib import Path
import joblib
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import confusion_matrix, f1_score, precision_score, recall_score
from features import FEATURE_NAMES, enrich
from generate import generate


def split(card_id):
    # Explicit card-disjoint split; each card has a fixed index in the synthetic generator.
    index = int(card_id.split("-")[1])
    return "train" if index % 20 < 14 else ("validation" if index % 20 < 17 else "test")


def metrics(labels, predicted):
    tn, fp, fn, tp = [int(x) for x in confusion_matrix(labels, predicted, labels=[0, 1]).ravel()]
    return {
        "tp": tp, "fp": fp, "tn": tn, "fn": fn,
        "precision": precision_score(labels, predicted, zero_division=0),
        "recall": recall_score(labels, predicted, zero_division=0),
        "f1": f1_score(labels, predicted, zero_division=0),
        "false_positive_rate": fp / (fp + tn) if fp + tn else 0,
    }


def run(cards, per_card, seed, output):
    by_split = {name: ([], []) for name in ("train", "validation", "test")}
    state = {}
    for event in generate(seed, cards, per_card):
        card = event["cardId"]
        features, state[card] = enrich(event, state.get(card))
        x, y = by_split[split(card)]
        x.append(features)
        y.append(event["groundTruth"])
    for name, (x, y) in by_split.items():
        if not x or len(set(y)) != 2:
            raise ValueError(f"{name} needs both classes; use at least 20 cards and 75 events/card")
    model = HistGradientBoostingClassifier(max_iter=80, max_leaf_nodes=15, learning_rate=.1, random_state=seed)
    model.fit(*by_split["train"])
    val_x, val_y = by_split["validation"]
    probabilities = model.predict_proba(val_x)[:, 1]
    candidates = []
    for i in range(5, 100):
        threshold = i / 100
        score = metrics(val_y, probabilities >= threshold)
        if score["false_positive_rate"] <= .005:
            candidates.append((score["f1"], threshold, score))
    if not candidates:
        threshold = .95
        constraint_met = False
    else:
        _, threshold, _ = max(candidates, key=lambda row: (row[0], row[1]))
        constraint_met = True
    test_x, test_y = by_split["test"]
    test_probabilities = model.predict_proba(test_x)[:, 1]
    report = {
        "source": "deterministic synthetic generator; not real payment data",
        "seed": seed, "cards": cards, "per_card": per_card,
        "split": {name: {"events": len(x), "fraud": sum(y)} for name, (x, y) in by_split.items()},
        "features": FEATURE_NAMES, "block_threshold": threshold,
        "validation_fpr_constraint_met": constraint_met,
        "validation": metrics(val_y, probabilities >= threshold),
        "test": metrics(test_y, test_probabilities >= threshold),
        "review_threshold": .35,
    }
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "threshold": threshold, "features": FEATURE_NAMES}, output / "model.joblib")
    (output / "evaluation.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cards", type=int, default=120)
    parser.add_argument("--per-card", type=int, default=90)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--output", default="artifacts")
    args = parser.parse_args()
    run(args.cards, args.per_card, args.seed, args.output)
