from ecommerce.algorithms import BloomFilter, CountMinSketch, Reservoir


def test_sketch_bounds_for_observed_counts():
    sketch = CountMinSketch()
    for key, count in [("a", 71), ("b", 12), ("c", 5)]:
        for _ in range(count):
            sketch.add(key)
    assert sketch.total == 88
    assert sketch.estimate("a") >= 71
    assert sketch.estimate("b") >= 12
    assert sketch.estimate("c") >= 5


def test_bloom_no_false_negatives_and_absent_probe():
    bloom = BloomFilter(1000)
    for i in range(1000):
        bloom.add(i)
    assert all(bloom.contains(i) for i in range(1000))
    assert sum(bloom.contains(i) for i in range(1000, 2000)) < 50


def test_reservoir_bounded_reproducible_and_not_prefix():
    a, b = Reservoir(10), Reservoir(10)
    for i in range(1000):
        a.add(i)
        b.add(i)
    assert a.items == b.items
    assert len(a.items) == 10 and a.seen == 1000
    assert any(i >= 10 for i in a.items)
