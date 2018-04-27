from greent.graph_components import KNode
from greent.rosetta import Rosetta
from greent import node_types
from builder.userquery import UserQuery
from builder.buildmain import KnowledgeGraph
from builder.lookup_utils import lookup_disease_by_name, lookup_drug_by_name, lookup_phenotype_by_name
        
class KnowledgeQuery:

    """ KnowledgeGraph query builder interface. """

    def type2nametype(self, node_type):
        disease_ish = [
            node_types.DISEASE,
            node_types.PHENOTYPE,
            node_types.GENETIC_CONDITION
        ]
        name_type = 'NAME.DISEASE' if node_type in disease_ish \
        else 'NAME.DRUG' if node_type == node_types.DRUG \
        else None
        if not name_type:
            raise ValueError(f'Unsupported named node type: {node_type}')
        return name_type

    def create_query (self, start_name, start_type, end_name, end_type, intermediate, two_sided, end_values):
        start_node = KNode(f"{start_type}.{start_name}", start_type)
        query = UserQuery (start_name, start_type)
        for transition in intermediate:
            query.add_transition(transition['type'],
                                 min_path_length=transition['min_path_length'],
                                 max_path_length=transition['max_path_length'])

        if two_sided:
            end_name_type = self.type2nametype(end_type)
            end_name_node = KNode(f"{end_name_type}.{end_name}", end_name_type)
            query.add_transition(end_type, end_values = end_values)
            #query.add_end_lookup_node(end_name_node)
        return query

    def query (self, query, query_id, support, rosetta):
        # build knowledge graph
        kgraph = KnowledgeGraph(query, rosetta)
        
        # get construction/source graph
        #sgraph = self._get_source_graph(kgraph)        
        kgraph.execute()
        kgraph.print_types()
        kgraph.enhance()
        if support:
            kgraph.support(support_module_names=['builder.omnicorp'])
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
