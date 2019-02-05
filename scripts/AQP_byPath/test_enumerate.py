import pytest
from scripts.AQP_byPath.enumerate_graphs import get_redis
import json
from collections import defaultdict

def test_counts():
    r = get_redis()
    a = 'CHEBI:15365'
    b = 'MONDO:0005136'
    np = 3
    key = f'Paths({np},{a},{b})'
    value = r.get(key)
    paths = json.loads(value)
    print(paths[0])
    descs = [(p['te0'],p['ln0'],p['te1'],p['ln1'],p['te2'],p['n1id']) for p in paths]
    counts = defaultdict(int)
    for d in descs:
        counts[d] += 1
    ml = [ (counts[d],d) for d in counts]
    print(ml[:5])
    ml.sort()
    for m in ml[-10:]:
        print(m)


