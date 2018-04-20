import logging
from greent.graph_components import KEdge
from greent.util import Text
from greent import node_types
from collections import defaultdict

def get_supporter(greent):
    return ChemotextSupport(greent)


class ChemotextSupport():

    def __init__(self,greent):
        self.greent = greent
        self.ctext = greent.chemotext
        self.identifier_to_label = defaultdict(list)

    def prepare(self,nodes):
        self.add_chemotext_terms( nodes )

    def add_chemotext_terms(self,nodes):
        """For each mesh term in a node, find out what chemotext calls that thing so we can query for it"""
        logging.getLogger('application').debug('{} nodes'.format(len(nodes) ))
        for node in nodes:
            logging.getLogger('application').debug('node: {}'.format(node.identifier) )
            mesh_identifiers = list( filter( lambda x: Text.get_curie(x)=='MESH', node.synonyms))
            for mesh_id in mesh_identifiers:
                logging.getLogger('application').debug('  mesh_id: {}'.format(mesh_id) )
                bare_id = Text.un_curie(mesh_id)
                cterm = self.ctext.get_chemotext_term_from_meshid( bare_id )
                if cterm is None:
                    logging.getLogger('application').warn("  Cannot find chemotext synonym for %s (%s) %s" % (bare_id,mesh_id,node.identifier))
                else:
                    logging.getLogger('application').debug('  node: {}, label: {}, chemotext: {}'.format(node.identifier, bare_id, cterm) )
                    self.identifier_to_label[node.identifier].append(cterm)

    def get_mesh_labels(self,node):
        logging.getLogger('application').debug('{} to {}'.format(node.identifier, self.identifier_to_label[node.identifier]))
        return self.identifier_to_label[ node.identifier ]

    def term_to_term(self,node_a,node_b,limit = 10000):
        """Given two terms, find articles in chemotext that connect them, and return as a KEdge.
        If nothing is found, return None"""
        logging.getLogger('application').debug('identifiers: {} to {}'.format(node_a.identifier, node_b.identifier))
        meshes_a = self.get_mesh_labels(node_a)
        meshes_b = self.get_mesh_labels(node_b)
        articles=[]
        from datetime import datetime
        start = datetime.now()
        for label_a in meshes_a:
            for label_b in meshes_b:
                response = self.ctext.query( query="MATCH (d:Term)-[r1]-(a:Art)-[r2]-(t:Term) WHERE d.name='%s' AND t.name='%s' RETURN a LIMIT %d" % (label_a, label_b, limit))
                for result in response['results']:
                    for data in result['data']:
                        articles += data['row']
        end = datetime.now()
        logging.getLogger('application').debug('chemotext: {} to {}: {} ({})'.format(meshes_a, meshes_b, len(articles), end-start))
        if len(articles) > 0:
            ke= KEdge( 'chemotext', 'term_to_term', { 'publications': articles }, is_support = True )
            ke.source_node = node_a
            ke.target_node = node_b
            return ke
        return None


def test():
    from greent.rosetta import Rosetta
    rosetta = Rosetta()
    gt = rosetta.core
    support = ChemotextSupport(gt)
    from greent.graph_components import KNode
    node = KNode('HP:0000964', node_type = node_types.PHENOTYPE, label='Eczema')
    node.mesh_identifiers.append( { 'curie': 'MeSH:D004485', 'label': 'Eczema' } )
    support.add_chemotext_terms( [node] )
    import json
    print( json.dumps( node.mesh_identifiers[0] ,indent=4) )

def test2():
    from greent.rosetta import Rosetta
    rosetta = Rosetta()
    gt = rosetta.core
    support = ChemotextSupport(gt)
    from greent.graph_components import KNode
    node_a = KNode('CTD:1,2-linoleoylphosphatidylcholine', node_type = node_types.DRUG, label='1,2-linoleoylphosphatidylcholine')
    node_b = KNode('CTD:Hydrogen Peroxide', node_type = node_types.DRUG, label='Hydrogen Peroxide')
    #node.mesh_identifiers.append( { 'curie': 'MeSH:D004485', 'label': 'Eczema' } )
    #support.add_chemotext_terms( [node] )
    #import json
    #print( json.dumps( node.mesh_identifiers[0] ,indent=4) )

if __name__ == '__main__':
    test()
