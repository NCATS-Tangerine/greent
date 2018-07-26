import pytest
from crawler.crawl_util import glom

def test_simple():
    d = {}
    eqs = [(1,2), (2,3), (4,5)]
    glom(d,eqs)
    assert len(d) == 5
    assert d[1] == d[2] == d[3] == {1,2,3}
    assert d[4] == d[5] == {4,5}

def test_two_calls():
    d = {}
    eqs = [(1,2), (2,3), (4,5), (6,7)]
    oeqs = [(5,7)]
    glom(d,eqs)
    glom(d,oeqs)
    assert d[1]==d[2]==d[3]=={1,2,3}
    assert d[4]==d[5]==d[6]==d[7]=={4,5,6,7}

def test_sets():
    d = {}
    eqs = [{1,2}, set([2,3]), set([4,5]), set([6,7])]
    oeqs = [{5,7}]
    glom(d,eqs)
    glom(d,oeqs)
    assert d[1]==d[2]==d[3]=={1,2,3}
    assert d[4]==d[5]==d[6]==d[7]=={4,5,6,7}

def test_bigger_sets():
    d = {}
    eqs = [{1,2,3}, {4,5,6} ]
    glom(d,eqs)
    assert d[1]==d[2]==d[3]=={1,2,3}
    assert d[4]==d[5]==d[6]=={4,5,6}
    eqs = [{3,4,6,7} ]
    glom(d,eqs)
    assert d[1]==d[2]==d[3]==d[4]==d[5]==d[6]==d[7]=={1,2,3,4,5,6,7}
