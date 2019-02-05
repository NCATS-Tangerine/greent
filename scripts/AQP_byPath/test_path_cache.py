import pytest
from scripts.AQP_byPath.cache_paths import convert_neo_result_to_dicts


def test_simplest():
    #This isn't really a neo4j result, but it has the same interface.
    result={'aid':'chebi:1','te0':'fakee0'}
    _,outdicts= convert_neo_result_to_dicts(result)
    assert len(outdicts) == 1
    od = outdicts[0]
    assert len(od) == 1
    assert od['te0'] == 'fakee0'

def test_simple_2hop():
    #This isn't really a neo4j result, but it has the same interface.
    result={'aid':'chebi:1','te0':'fakee0','ln0':['named_thing','gene'],'n0id':'hgnc:1','te1':'fakee1'}
    _,outdicts = convert_neo_result_to_dicts(result)
    assert len(outdicts) == 1
    od = outdicts[0]
    assert len(od) == 4
    assert od['te0'] == 'fakee0'
    assert od['te1'] == 'fakee1'
    assert od['ln0'] == 'gene'
    assert od['n0id'] == 'hgnc:1'

def test_2label_2hop():
    #This isn't really a neo4j result, but it has the same interface.
    result={'aid':'chebi:1','te0':'fakee0','ln0':['named_thing','genetic_condition','disease'],'n0id':'mondo:1','te1':'fakee1'}
    _,outdicts = convert_neo_result_to_dicts(result)
    assert len(outdicts) == 2
    ln0s = set()
    for od in outdicts:
        assert len(od) == 4
        assert od['te0'] == 'fakee0'
        assert od['te1'] == 'fakee1'
        ln0s.add(od['ln0'] )
        assert od['n0id'] == 'mondo:1'
    assert len(ln0s) == 2
    assert 'genetic_condition' in ln0s
    assert 'disease' in ln0s

def test_2label_3hop():
    #This isn't really a neo4j result, but it has the same interface.
    result={'aid':'chebi:1','te0':'fakee0',
            'ln0':['named_thing','genetic_condition','disease'],'n0id':'mondo:1','te1':'fakee1',
            'ln1':['named_thing','phenotypic_feature_or_disease','phenotypic_feature'],'n1id':'umls:1','te2':'fakee2'}
    _,outdicts = convert_neo_result_to_dicts(result)
    assert len(outdicts) == 4
    ln0s = set()
    for od in outdicts:
        assert len(od) == 7
        assert od['te0'] == 'fakee0'
        assert od['te1'] == 'fakee1'
        assert od['te2'] == 'fakee2'
        ln0s.add((od['ln0'],od['ln1']))
        assert od['n0id'] == 'mondo:1'
        assert od['n1id'] == 'umls:1'
    assert len(ln0s) == 4
    assert ('genetic_condition','phenotypic_feature') in ln0s
    assert ('genetic_condition','phenotypic_feature_or_disease') in ln0s
    assert ('disease','phenotypic_feature') in ln0s
    assert ('disease','phenotypic_feature_or_disease') in ln0s
