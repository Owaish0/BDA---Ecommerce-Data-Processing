"""Small deterministic streaming algorithms for measured comparisons, not transaction filtering."""

import hashlib
import math
import random


def bucket(value, seed, width):
    digest = hashlib.blake2b((str(seed) + ":" + str(value)).encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big") % width


class CountMinSketch:
    def __init__(self, epsilon=0.01, failure_probability=0.01):
        if not 0 < epsilon < 1 or not 0 < failure_probability < 1:
            raise ValueError("Probabilities must be between zero and one")
        self.width = math.ceil(math.e / epsilon)
        self.depth = math.ceil(math.log(1 / failure_probability))
        self.rows = [[0] * self.width for _ in range(self.depth)]
        self.total = 0

    def add(self, value):
        for seed, row in enumerate(self.rows):
            row[bucket(value, seed, self.width)] += 1
        self.total += 1

    def estimate(self, value):
        return min(row[bucket(value, seed, self.width)] for seed, row in enumerate(self.rows))


class BloomFilter:
    def __init__(self, capacity, false_positive_rate=0.01):
        if capacity < 1 or not 0 < false_positive_rate < 1:
            raise ValueError("Invalid capacity/probability")
        self.size = math.ceil(-capacity * math.log(false_positive_rate) / (math.log(2) ** 2))
        self.hashes = max(1, round(self.size / capacity * math.log(2)))
        self.bits = bytearray((self.size + 7) // 8)

    def add(self, value):
        for seed in range(self.hashes):
            bit = bucket(value, seed, self.size)
            self.bits[bit // 8] |= 1 << (bit % 8)

    def contains(self, value):
        return all(
            self.bits[(bit := bucket(value, seed, self.size)) // 8] & (1 << (bit % 8))
            for seed in range(self.hashes)
        )


class Reservoir:
    def __init__(self, size=1000, seed=404):
        if size < 1:
            raise ValueError("Size must be positive")
        self.size = size
        self.random = random.Random(seed)
        self.seen = 0
        self.items = []

    def add(self, value):
        self.seen += 1
        if len(self.items) < self.size:
            self.items.append(value)
        else:
            index = self.random.randrange(self.seen)
            if index < self.size:
                self.items[index] = value
