import pytest
from greent.graph_components import KNode
from greent import node_types
from greent.util import Text
from greent.conftest import rosetta


@pytest.fixture()
def kegg(rosetta):
    kegg = rosetta.core.kegg
    return kegg

def test_chem_to_enzyme_Trypanothione(kegg):
    l = KNode('KEGG.COMPOUND:C02090', name='trypanothione', type=node_types.CHEMICAL_SUBSTANCE)
    results = kegg.chemical_get_enzyme(l)
    ids = [node.id for edge,node in results]
    print(ids)
    assert('NCBIGene:6241' in ids)

def test_chem_to_eynzyme_tyrosine_tat(kegg):
    #Why don't I get the gene ASS1 when I look at degradation of L-aspartic acid?
    l = KNode('KEGG.COMPOUND:C00082', name='L-tyrosine', type = node_types.CHEMICAL_SUBSTANCE)
    results = kegg.chemical_get_enzyme(l)
    ids = [node.id for edge,node in results]
    for tid in ids:
        print(tid)
    assert('NCBIGene:6898' in ids)

def test_chem_to_chem(kegg,rosetta):
    codeine = KNode('CHEBI:16714',name='Codeine',type=node_types.CHEMICAL_SUBSTANCE)
    rosetta.synonymizer.synonymize(codeine)
    results = kegg.chemical_get_chemical(codeine)
    morphine = 'CHEBI:17303'
    codeine6glucoronide = 'CHEBI:80580'
    norcodeine = 'CHEBI:80579'
    ids = []
    for edge,node in results:
        assert edge.source_id == 'CHEBI:16714'
        rosetta.synonymizer.synonymize(node)
        ids.append(node.id)
    assert len(results) > 0
    for myid in ids:
        print(myid)
    assert morphine in ids
    assert codeine6glucoronide in ids
    assert norcodeine in ids

def test_chem_to_chem(kegg,rosetta):
    codeine = KNode('CHEBI:16714',name='Codeine',type=node_types.CHEMICAL_SUBSTANCE)
    rosetta.synonymizer.synonymize(codeine)
    results = kegg.chemical_get_chemical(codeine)
    morphine = 'CHEBI:17303'
    codeine6glucoronide = 'CHEBI:80580'
    norcodeine = 'CHEBI:80579'
    ids = []
    for edge,node in results:
        assert edge.source_id == 'CHEBI:16714'
        rosetta.synonymizer.synonymize(node)
        ids.append(node.id)
    assert len(results) > 0
    for myid in ids:
        print(myid)
    assert morphine in ids
    assert codeine6glucoronide in ids
    assert norcodeine in ids


def test_chem_to_chem_Glucosylceramide(kegg,rosetta):
    Glucosylceramide = KNode('KEGG.COMPOUND:C01190',name='Glucosylceramide',type=node_types.CHEMICAL_SUBSTANCE)
    rosetta.synonymizer.synonymize(Glucosylceramide)
    Acylsphingosine = 'KEGG.COMPOUND:C00195'
    results = kegg.chemical_get_chemical(Glucosylceramide)
    ids = []
    for edge,node in results:
        ids.append(node.id)
    assert len(results) > 0
    assert Acylsphingosine in ids

def test_chem_to_chem_caffiene(kegg,rosetta):
    caffiene = KNode('CHEBI:27732',name='Caffiene',type=node_types.CHEMICAL_SUBSTANCE)
    rosetta.synonymizer.synonymize(caffiene)
    results = kegg.chemical_get_chemical(caffiene)
    theobromine = 'KEGG.COMPOUND:C07480'
    ids = []
    for edge,node in results:
        if edge.source_id == 'CHEBI:27732':
            ids.append(node.id)
    #Really, it should be, but this reaction doesn't appear in KEGG (for humans)
    assert theobromine not in ids

def test_chem_to_reaction(kegg):
    hete = KNode('KEGG.COMPOUND:C04805', name="5-HETE", type=node_types.CHEMICAL_SUBSTANCE)
    results = kegg.chemical_get_reaction(hete)
    assert len(results)  == 1
    assert results[0] == 'rn:R07034'

def test_rxn_to_chem(kegg):
    results = kegg.reaction_get_chemicals('rn:R07034')
    assert len(results) == 5

def test_get_reaction(kegg):
    reaction = kegg.get_reaction('rn:R07034')
    assert len(reaction['enzyme']) == 7
    assert len(reaction['reactants']) == 2
    assert 'C00051' in reaction['reactants']
    assert 'C05356' in reaction['reactants']
    assert len(reaction['products']) == 3
    assert 'C00001' in reaction['products']
    assert 'C00127' in reaction['products']
    assert 'C04805' in reaction['products']

def test_get_reaction_morphinetomorphine3gluc(kegg):
    reactions = kegg.get_reaction('rn:R08262')
    assert len(reactions) == 1
    reaction = reactions[0]
    assert 'enzyme' in reaction


def test_chem_to_enzyme_fail(kegg,rosetta):
    input = KNode('CHEBI:29073',name='CHEDMICAL', type=node_types.METABOLITE)
    rosetta.synonymizer.synonymize(input)
    results = kegg.chemical_get_enzyme(input)
    print(results)
    assert True

def test_chem_to_enzyme_failagain(kegg,rosetta):
    input = KNode('CHEBI:16856',name='CHEDMICAL', type=node_types.METABOLITE)
    rosetta.synonymizer.synonymize(input)
    results = kegg.chemical_get_enzyme(input)
    print(results)
    assert True

def test_chem_to_enzyme_nb(kegg,rosetta):
    input = KNode('CHEBI:1941',name='CHEDMICAL', type=node_types.METABOLITE)
    rosetta.synonymizer.synonymize(input)
    results = kegg.chemical_get_enzyme(input)
    print(results)
    assert True

# There is a problem with some genes coming back malformed
def test_chem_to_enzyme_nb(kegg,rosetta):
    input = KNode('KEGG.COMPOUND:C00319',name='Sphingosine', type=node_types.METABOLITE)
    results = kegg.chemical_get_enzyme(input)
    genes = set()
    for e,n in results:
        genes.add(n.id)
    for gene in genes:
        print(gene)
    print(len(gene))

def test_chem_to_gene_Glucosylceramide(kegg,rosetta):
    Glucosylceramide = KNode('KEGG.COMPOUND:C01190',name='Glucosylceramide',type=node_types.CHEMICAL_SUBSTANCE)
    rosetta.synonymizer.synonymize(Glucosylceramide)
    GBA = 'HGNC:4177'
    results = kegg.chemical_get_enzyme(Glucosylceramide)
    ids = []
    for edge,node in results:
        rosetta.synonymizer.synonymize(node)
        ids.append(node.id)
    assert len(results) > 0
    assert GBA in ids


def test_get_reaction(kegg):
    rn = 'rn:R09338'
    out = kegg.get_reaction(rn)
    assert True

def test_chem_to_eynzyme_ass1(kegg):
    #Why don't I get the gene ASS1 when I look at degradation of L-aspartic acid?
    l = KNode('KEGG.COMPOUND:C00049', name='L-aspartic acid', type = node_types.CHEMICAL_SUBSTANCE)
    results = kegg.chemical_get_enzyme(l)
    ids = [node.id for edge,node in results]
    print(ids)
    assert('NCBIGene:445' in ids)

def test_chem_to_enzyme(kegg):
    hete = KNode('KEGG.COMPOUND:C04805', name="5-HETE", type=node_types.CHEMICAL_SUBSTANCE)
    results = kegg.chemical_get_enzyme(hete)
    assert len(results) == 7
    ids = [ node.id for edge,node in results ]
    assert 'NCBIGene:2876' in ids
    assert 'NCBIGene:2877' in ids
    assert 'NCBIGene:2878' in ids
    assert 'NCBIGene:2880' in ids
    assert 'NCBIGene:2882' in ids
    assert 'NCBIGene:257202' in ids
    assert 'NCBIGene:493869' in ids

def test_chem_to_enzyme_codeine_to_cyp3a4(kegg):
    hete = KNode('KEGG.COMPOUND:C06174', name="Codeine", type=node_types.CHEMICAL_SUBSTANCE)
    results = kegg.chemical_get_enzyme(hete)
    ids = [ node.id for edge,node in results ]
    assert 'NCBIGene:1576' in ids

def test_chem_to_enzyme_norcodeine_to_cyp3a4(kegg):
    hete = KNode('KEGG.COMPOUND:C16576', name="Norcodeine", type=node_types.CHEMICAL_SUBSTANCE)
    results = kegg.chemical_get_enzyme(hete)
    ids = [ node.id for edge,node in results ]
    assert 'NCBIGene:1576' in ids

def test_chem_to_enzymes_morphine_and_morphine3gluc(kegg):
    morhpine = KNode('KEGG.COMPOUND:C01516', name="Morphine", type=node_types.CHEMICAL_SUBSTANCE)
    results = kegg.chemical_get_enzyme(morhpine)
    morphine_enzymes = set([ node.id for edge,node in results ])
    normorhpine = KNode('KEGG.COMPOUND:C16643', name="Morphine3Gluc", type=node_types.CHEMICAL_SUBSTANCE)
    results = kegg.chemical_get_enzyme(normorhpine)
    normorphine_enzymes = set([ node.id for edge,node in results ])
    shared = morphine_enzymes.intersection(normorphine_enzymes)
    assert len(shared) > 0


#def test_enzyme_to_chem(kegg):
#    enzyme = KNode('EC:1.11.1.9', name="who", type=node_types.GENE)
#    results = kegg.enzyme_get_chemicals(enzyme)
#    assert len(results) == 8
#
#def test_synonymized(kegg,rosetta):
#    enzyme = KNode('HGNC:2843',name='DGAT1',type=node_types.GENE)
#    rosetta.synonymizer.synonymize(enzyme)
#    results = kegg.enzyme_get_chemicals(enzyme)
#    assert len(results) > 0
