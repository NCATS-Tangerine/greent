'''
Question definition
'''

# standard modules
import os
import sys
import json
import hashlib
import warnings
import logging
import time
from typing import NamedTuple

from builder.api.setup import swagger
from builder.util import FromDictMixin
from greent.util import LoggingUtil, Text

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

@swagger.definition('QNode')
class QNode(FromDictMixin):
    """
    Node Object
    ---
    schema:
        id: QNode
        required:
            - node_id
        properties:
            node_id:
                type: string
                required: true
            type:
                type: string
            curie:
                type: string
            name:
                type: string
            synonyms:
                type: array
                items:
                    $ref: '#/definitions/LabeledID'
    """
    def __init__(self, *args, **kwargs):
        self.node_id = None
        self.type = None
        self.curie = None
        self.name = None
        self.synonyms = []

        super().__init__(*args, **kwargs)

    @property
    def id(self):
        return self.node_id

    def __repr__(self):
        return f'{self.type} ({self.curie})'

    def concept_cypher_signature(self, id, known=False):
        if self.type and not known:
            return f"({id}:Concept {{name: '{self.type}'}})"
        elif not known:
            return f"({id}:Concept)"
        else:
            return f"({id})"

    def cypher_signature(self, id, known=False):
        if self.curie and self.type and not known:
            return f"({id}:{self.type} {{id: '{self.curie}'}})"
        elif self.type and not known:
            return f"({id}:{self.type})"
        else:
            return f"({id})"

    def load_attribute(self, key, value):
        if key == 'synonyms':
            return {LabeledID(**v) if isinstance(v, dict) else v for v in value}
        else:
            return super().load_attribute(key, value)

@swagger.definition('LabeledID')
class LabeledID(NamedTuple):
    """
    Labeled Thing Object
    ---
    schema:
        id: LabeledID
        required:
            - identifier
        properties:
            identifer:
                type: string
            label:
                type: string
    """
    identifier: str
    label: str = ''

    def __repr__(self):
        return f'({self.identifier},{self.label})'

    def __gt__(self, other):
        return self.identifier > other.identifier

@swagger.definition('QEdge')
class QEdge(FromDictMixin):
    """
    Edge Object
    ---
    schema:
        id: QEdge
        required:
            - edge_id
            - source_id
            - target_id
        properties:
            edge_id:
                type: string
            source_id:
                type: string
            target_id:
                type: string
            provided_by:
                type: string
            original_predicate:
                $ref: '#/definitions/LabeledID'
            standard_predicate:
                $ref: '#/definitions/LabeledID'
            publications:
                type: array
                items:
                    type: string
            min_length:
                type: integer
                default: 1
            max_length:
                type: integer
                default: 1
    """
    def __init__(self, *args, **kwargs):
        self.edge_id = None
        self.source_id = None
        self.target_id = None
        self.provided_by = None
        self.original_predicate = None
        self.standard_predicate = None
        self.type = None
        self.publications = []
        self.min_length = 1
        self.max_length = 1
        self.ctime = time.time() # creation time in seconds since the epoch, UTC
        # time.localtime() will turn this into a localized struct_time

        super().__init__(*args, **kwargs)

        if self.min_length > self.max_length:
            raise ValueError("An edge's minimum length should be less than or equal to its maximum length.")

    @property 
    def id(self):
        return self.edge_id

    def cypher_signature(self, id=None):
        # TODO: handle known predicates
        if id and not self.min_length == self.max_length == 1:
            return f"-[{id}*{self.min_length}..{self.max_length}]-"
        elif id:
            return f"-[{id}]-"
        elif not self.min_length == self.max_length == 1:
            return f"-[*{self.min_length}..{self.max_length}]-"
        else:
            return  "--"

    def load_attribute(self, key, value):
        if key == 'original_predicate' or key == 'standard_predicate':
            return LabeledID(**value) if isinstance(value, dict) else value
        else:
            return super().load_attribute(key, value)

@swagger.definition('Question')
class Question(FromDictMixin):
    """
    Question Object
    ---
    schema:
        id: Question
        required:
          - query_graph
        properties:
            query_graph:
                type: object
                required:
                  - nodes
                  - edges
                properties:
                    nodes:
                        type: array
                        items:
                            $ref: '#/definitions/QNode'
                    edges:
                        type: array
                        items:
                            $ref: '#/definitions/QEdge'
    """

    def __init__(self, *args, **kwargs):
        self.query_graph = {}

        super().__init__(*args, **kwargs)

    def load_attribute(self, key, value):
        if key == 'query_graph':
            return {
                'nodes': [QNode(n) for n in value['nodes']],
                'edges': [QEdge(e) for e in value['edges']]
            }
        else:
            return super().load_attribute(key, value)

    @property
    def cypher_signature(self):
        node_map = {n.id:n for n in self.query_graph['nodes']}
        links = []
        known_ids = set()
        for e in self.query_graph['edges']:
            source_signature = node_map[e.source_id].cypher_signature(
                f'{e.source_id}',
                known=e.source_id in known_ids
            )
            target_signature = node_map[e.target_id].cypher_signature(
                f'{e.target_id}',
                known=e.target_id in known_ids
            )
            links.append(f"{source_signature}{e.cypher_signature()}{target_signature}")
            known_ids.update([e.source_id, e.target_id])
        return links

    @property
    def concept_cypher_signature(self):
        node_map = {n.id:n for n in self.query_graph['nodes']}
        links = []
        known_ids = set()
        for idx, e in enumerate(self.query_graph['edges']):
            source_signature = node_map[e.source_id].concept_cypher_signature(
                f'{e.source_id}',
                known=e.source_id in known_ids
            )
            target_signature = node_map[e.target_id].concept_cypher_signature(
                f'{e.target_id}',
                known=e.target_id in known_ids
            )
            links.append(f"{source_signature}{e.cypher_signature(f'{e.id}')}{target_signature}")
            known_ids.update([e.source_id, e.target_id])
        return links

    def generate_concept_cypher(self):
        """Generate a cypher query to find paths through the concept-level map."""
        named_node_names = [n.id for n in self.query_graph['nodes'] if n.curie]
        node_names = [n.id for n in self.query_graph['nodes']]
        edge_names = [e.id for e in self.query_graph['edges']]
        cypherbuffer = [f"MATCH {s}" for s in self.concept_cypher_signature]
        node_list = f"""[{', '.join([f"'{n}'" for n in node_names])}]"""
        named_node_list = f"""[{', '.join([f"'{n}'" for n in named_node_names])}]"""
        edge_list = f"[{', '.join(edge_names)}]"
        edge_switches = [f"CASE startnode({e.id}) WHEN {e.source_id} THEN ['{e.source_id}','{e.target_id}'] ELSE ['{e.target_id}','{e.source_id}'] END AS {e.id}_pair" for e in self.query_graph['edges']]
        edge_pairs = [f"{e.id}_pair" for e in self.query_graph['edges']]
        cypherbuffer.append(f"WITH {', '.join(node_names + edge_names + edge_switches)}")
        cypherbuffer.append(f"WHERE robokop.traversable({node_list}, [{', '.join(edge_pairs)}], {named_node_list})")
        # This is to make sure that we don't get caught up in is_a and other funky relations.:
        cypherbuffer.append(f'AND ALL(r in {edge_list} WHERE EXISTS(r.op))')
        node_map = f"{{{', '.join([f'{n}:{n}' for n in node_names])}}}"
        edge_map = f"{{{', '.join([f'{e}:{e}{{.*, source:{e}_pair[0], target:{e}_pair[1]}}' for e in edge_names])}}}"
        cypherbuffer.append(f"RETURN {node_map} as nodes, {edge_map} as edges")
        return '\n'.join(cypherbuffer)

    def get_transitions(self, graph, query):
        """ Execute a cypher query and walk the results to build a set of transitions to execute.
        The query should be such that it returns a path (node0-relation0-node1-relation1-node2), and
        an array of the relation start nodes.  For the path above, start nodes like (node0,node1) would
        indicate a unidirectional path, while (node0,node2) would indicate an end-based path meeting in
        the middle.
        Each node in the path can be described with an arbitrary node index.  Note that this index does not
        have to correspond to the order of calling or any structural property of the graph.  It simply points
        to a particular node in the call map.
        Returns:
            nodes: A map from a node index to the concept.
            transitions: a map from a node index to an (operation, output index) pair
        """
        with graph.driver.session() as session:
            result = session.run(query)
        plans=[]
        for row in result:
            nodes = row['nodes']
            edges = row['edges']

            # extract transitions
            transitions = {node_id: {node_id: [] for node_id in nodes} for node_id in nodes}
            for e in edges:
                edge = edges[e]
                source_id = edge['source']
                target_id = edge['target']
                qedge = next(e2 for e2 in self.query_graph['edges'] if e2.id == e)
                qedge_type = qedge.type
                predicate = [Text.snakify(e2type) for e2type in qedge_type] if isinstance(qedge_type, list) and qedge_type else Text.snakify(qedge_type) if isinstance(qedge_type, str) else None
                trans = {
                    "op": edge['op'],
                    "link": edge['predicate'],
                    "predicate": predicate
                }
                transitions[source_id][target_id].append(trans)

            plans.append(transitions)
        return plans
    def get_edge_op_paths(self, graph):
        """
        Executes a cypher query and returns the result of operations bound to edges.
        Returns:
            A map of edge keys and their list of operations along their source node ids.
        """
        with graph.driver.session() as session:
            result = session.run(self.generate_concept_cypher())
        response = {}
        logger.debug(result)
        for row in result:
            edges = row['edges']
            print(len(edges))
            for e in edges:
                op = edges[e]['op']
                source = edges[e]['source']
                if e not in response:
                    response[e] = {
                        'input': source,
                        'op': [op]
                    }
                elif op not in response[e]['op']:
                    response[e]['op'].append(op)

        for e_id in response:
            edge = response[e_id]
            new_edge = []
            for op in edge['op']:
                new_edge.append({
                    'op': op,
                    'input': edge['input']
                })
            response[e_id] = new_edge
        return response

    def compile(self, rosetta, disconnected_graph = False):
        plan = None
        if not disconnected_graph:
            plans = self.get_transitions(rosetta.type_graph, self.generate_concept_cypher())
            
            # merge plans
            plan = {n.id: {n.id: [] for n in self.query_graph['nodes']} for n in self.query_graph['nodes']}
            for p in plans:
                for source_id in p:
                    for target_id in p[source_id]:
                        plan[source_id][target_id].extend(p[source_id][target_id])

            # remove duplicate transitions
            for source_id in plan:
                for target_id in plan:
                    plan[source_id][target_id] = {t['op']:t for t in plan[source_id][target_id]}.values()
        else:
            plan = self.get_transitions_disconnected(rosetta.type_graph)
        if not plan:
            raise RuntimeError('No viable programs.')

        from greent.program import Program
        program = Program(plan, self.query_graph, rosetta, 0)
        programs = [program]
    
        return programs

    def get_transitions_disconnected(self, graph):
        """
        Function adjusted for crawler works on the assumptions that quetion contains 
        unform types of  pairs of nodes which we don't have pair to pair connections.
        I.e (a)->(b) (c) -> (d) but no (b)->(c)
        """
        source_node = self.query_graph['nodes'][0].concept_cypher_signature('n0')
        target_node = self.query_graph['nodes'][1].concept_cypher_signature('n1')
        cypher =[f'MATCH {source_node}-[e]-> {target_node}']
        cypher += ['WHERE Exists(e.op) RETURN Collect(e) as edges']
        query = '\n'.join(cypher)
        result = ''
        with graph.driver.session() as session:
            result = session.run(query)
        edges = []
        for row in result:
            for edge in row['edges']:
                e = {
                        "op": edge['op'],
                        "link": edge['predicate'],
                        "predicate": Text.snakify(edge['type']) if edge['type'] else None
                    }
                edges.append(e)
        p = {}
        for edge in self.query_graph['edges']:
            p[edge.source_id] = {}
            p[edge.source_id][edge.target_id] = {e['op']: e for e in edges}.values()
        return p