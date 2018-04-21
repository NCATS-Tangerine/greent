from greent.graph_components import KNode
from greent.rosetta import Rosetta
from greent.userquery import UserQuery
from builder.builder import KnowledgeGraph
from builder.lookup_utils import lookup_disease_by_name, lookup_drug_by_name, lookup_phenotype_by_name

class KnowledgeQuery:

    """ KnowledgeGraph query builder interface. """

    def type2nametype(self, node_type):
        name_type = 'NAME.DISEASE' if node_type == 'Disease' or node_type == 'Phenotype' or node_type == 'GeneticCondition'\
            else 'NAME.DRUG' if node_type == 'Substance'\
            else None
        if not name_type:
            raise ValueError('Unsupported named node type.')
        return name_type

    def create_query (self, start_name, start_type, end_name, end_type, intermediate, two_sided):
        start_name_type = self.type2nametype(start_type)
        start_name_node = KNode( '{}.{}'.format(start_name_type, start_name), start_name_type)
        query = UserQuery(ids[0], start_type, start_name_node)
        for transition in intermediate:
            query.add_transition(transition['type'].replace(' ', ''),
                                 min_path_length=transition['leadingEdge']['numNodesMin']+1,
                                 max_path_length=transition['leadingEdge']['numNodesMax']+1)
        if two_sided:
            end_name_type = self.type2nametype(end_type)
            end_name_node = KNode(f"{end_name_type}.{end_name}", end_name_type)
            query.add_transition(end_type, end_values = ids[-1])
            query.add_end_lookup_node(end_name_node)
        return query

    def query (self, query, query_id):
        # build knowledge graph
        kgraph = KnowledgeGraph(query, Rosetta ())
        
        # get construction/source graph
        sgraph = self._get_source_graph(kgraph)        
        kgraph.execute()
        kgraph.print_types()
        kgraph.prune()
        kgraph.enhance()
        kgraph.support(supports=['chemotext'])
        return kgraph

    def _get_source_graph(self, kgraph):
        # quickly grab the knowledge source graph
        cyphers = kgraph.userquery.generate_cypher()
        construction_graph = []
        for cypher in cyphers:
            nodes = []
            edges = []
            programs = kgraph.rosetta.type_graph.db.query(cypher, data_contents=True)
            for program in programs.rows:
                chain = program[0]

                # chain looks something like this:
                """[{'iri': 'http://identifiers.org/name/disease', 'name': 'NAME.DISEASE'},
                    {'op': 'tkba.name_to_doid', 'predicate': 'NAME_TO_ID', 'synonym': False, 'enabled': True},
                    {'iri': 'http://identifiers.org/doid', 'name': 'DOID'},
                    {'op': 'disease_ontology.doid_to_pharos', 'predicate': 'SYNONYM', 'synonym': True, 'enabled': True},
                    {'iri': 'http://pharos.nih.gov/identifier/', 'name': 'PHAROS'},
                    {'op': 'pharos.disease_get_gene', 'predicate': 'DISEASE_GENE', 'synonym': False, 'enabled': True},
                    {'iri': 'http://identifiers.org/hgnc', 'name': 'HGNC'},
                    {'op': 'biolink.gene_get_genetic_condition', 'predicate': 'GENE_TO_GENETIC_CONDITION', 'synonym': False, 'enabled': True},
                    {'iri': 'http://identifiers.org/doid/gentic_condition', 'name': 'DOID.GENETIC_CONDITION'}]"""

                nodes += [{'id':n['name'],
                           'name':n['name'],
                           'type':n['iri']} for n in chain[::2]]
                edges += [{'from':chain[i*2]['name'],
                           'to':chain[i*2+2]['name'],
                           'reference':e['op'].split('.')[0],
                           'function':e['op'].split('.')[1],
                           'type':e['predicate'],
                           'id':e['op'],
                           'publications':''} for i, e in enumerate(chain[1::2])]

            # unique nodes
            nodes = {n['id']:n for n in nodes}
            nodes = [nodes[k] for k in nodes]
            
            # unique edges
            edges = {e['id']:e for e in edges}
            edges = [edges[k] for k in edges]
            construction_graph += [{
                'nodes': nodes,
                'edges': edges
            }]
        def uniqueDictByField(d, k):
            return list({e[k]:e for e in d}.values())
        construction_graph = {
            'nodes': uniqueDictByField([n for g in construction_graph for n in g['nodes']], 'id'),
            'edges': uniqueDictByField([e for g in construction_graph for e in g['edges']], 'id')
        }
        return construction_graph
