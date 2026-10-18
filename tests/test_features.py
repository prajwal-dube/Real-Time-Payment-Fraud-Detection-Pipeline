import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "processor"))
from features import enrich, distance_km
from generate import generate
from train import split


class FeaturesTest(unittest.TestCase):
    def event(self, timestamp, country="IN", lat=12.97, lon=77.59):
        return {"eventAt": timestamp, "amount": 50, "country": country, "lat": lat, "lon": lon}

    def test_velocity_and_geography_cross_batch_boundaries(self):
        first, state = enrich(self.event(1_000_000))
        second, state = enrich(self.event(1_005_000, "GB", 51.51, -.13), state)
        self.assertEqual(first[1], 1)
        self.assertEqual(second[1], 2)
        self.assertEqual(second[4], 1)
        self.assertGreater(second[2], 7000)
        later, _ = enrich(self.event(1_400_000), state)
        self.assertEqual(later[1], 1)

    def test_synthetic_generator_and_card_split(self):
        events = generate(seed=9, cards=20, per_card=80)
        self.assertEqual(len(events), 1600)
        self.assertEqual(sum(e["groundTruth"] for e in events), 200)
        self.assertEqual({split(f"tok-{i:04d}") for i in range(20)}, {"train", "validation", "test"})
        self.assertEqual(events, generate(seed=9, cards=20, per_card=80))


if __name__ == "__main__":
    unittest.main()
