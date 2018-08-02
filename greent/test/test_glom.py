import pytest
from crawler.crawl_util import glom
from greent.conftest import rosetta
from greent.graph_components import LabeledID

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

def test_load_diseases_and_phenotypes(rosetta):
    mondo_sets = build_sets(rosetta.core.mondo,['MONDO:0004979','MONDO:0004784','MONDO:0004765'])
    #hpo_sets = build_sets(rosetta.core.hpo,['HP:0002099'])
    dicts = {}
    glom(dicts,mondo_sets)
    print("*",dicts['MONDO:0004979'])
    print("*",dicts['MONDO:0004784'])
    print("*",dicts['MONDO:0004765'])
    assert dicts['MONDO:0004979'] != dicts['MONDO:0004784'] != dicts['MONDO:0004765']


def build_sets(o,mids):
    sets=[]
    for mid in mids:
        print( o.get_xrefs(mid) )
        dbx = set([x['id'] for x in o.get_xrefs(mid) if not x['id'].startswith('ICD')])
        print('-----:',dbx)
        dbx.add(mid)
        sets.append(dbx)
    return sets

