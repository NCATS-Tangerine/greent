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

from builder.api.setup import swagger
from builder.util import FromDictMixin
from greent.util import LoggingUtil

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)

@swagger.definition('Node')
class Node(FromDictMixin):
    """
    Node Object
    ---
    schema:
        id: Node
        required:
            - id
        properties:
            id:
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
                    $ref: '#/definitions/LabeledThing'
    """
    def __init__(self, *args, **kwargs):
        self.id = None
        self.type = None
        self.curie = None
        self.name = None
        self.synonyms = []

        super().__init__(*args, **kwargs)

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
            return {LabeledThing(v) if isinstance(v, dict) else v for v in value}
        else:
            return super().load_attribute(key, value)

@swagger.definition('LabeledThing')
class LabeledThing(FromDictMixin):
    """
    Labeled Thing Object
    ---
    schema:
        id: LabeledThing
        required:
            - identifier
        properties:
            identifer:
                type: string
            label:
                type: string
    """
    def __init__(self, *args, **kwargs):
        self.identifier = None
        self.label = None

        super().__init__(*args, **kwargs)

    def __gt__(self, other):
        return self.identifier > other.identifier

@swagger.definition('Edge')
class Edge(FromDictMixin):
    """
    Edge Object
    ---
    schema:
        id: Edge
        required:
            - source_id
            - target_id
        properties:
            id:
                type: string
            source_id:
                type: string
            target_id:
                type: string
            provided_by:
                type: string
            original_predicate:
                $ref: '#/definitions/LabeledThing'
            standard_predicate:
                $ref: '#/definitions/LabeledThing'
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
        self.id = None
        self.source_id = None
        self.target_id = None
        self.provided_by = None
        self.original_predicate = None
        self.standard_predicate = None
        self.publications = []
        self.min_length = 1
        self.max_length = 1
        self.ctime = time.time() # creation time in seconds since the epoch, UTC
        # time.localtime() will turn this into a localized struct_time

        super().__init__(*args, **kwargs)

        if self.min_length > self.max_length:
            raise ValueError("An edge's minimum length should be less than or equal to its maximum length.")

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
            return LabeledThing(value) if isinstance(value, dict) else value
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
          - machine_question
        properties:
            machine_question:
                type: object
                required:
                  - nodes
                  - edges
                properties:
                    nodes:
                        type: array
                        items:
                            $ref: '#/definitions/Node'
                    edges:
                        type: array
                        items:
                            $ref: '#/definitions/Edge'
    """

    def __init__(self, *args, **kwargs):
        self.machine_question = {}

        super().__init__(*args, **kwargs)

    def load_attribute(self, key, value):
        if key == 'machine_question':
            return {
                'nodes': [Node(n) for n in value['nodes']],
                'edges': [Edge(e, id=idx) for idx, e in enumerate(value['edges'])]
            }
        else:
            return super().load_attribute(key, value)

    @property
    def cypher_signature(self):
        node_map = {n.id:n for n in self.machine_question['nodes']}
        links = []
        known_ids = set()
        for e in self.machine_question['edges']:
            source_signature = node_map[e.source_id].cypher_signature(
                f'n{e.source_id}',
                known=e.source_id in known_ids
            )
            target_signature = node_map[e.target_id].cypher_signature(
                f'n{e.target_id}',
                known=e.target_id in known_ids
            )
            links.append(f"{source_signature}{e.cypher_signature()}{target_signature}")
            known_ids.update([e.source_id, e.target_id])
        return links

    @property
    def concept_cypher_signature(self):
        node_map = {n.id:n for n in self.machine_question['nodes']}
        links = []
        known_ids = set()
        for idx, e in enumerate(self.machine_question['edges']):
            source_signature = node_map[e.source_id].concept_cypher_signature(
                f'n{e.source_id}',
                known=e.source_id in known_ids
            )
            target_signature = node_map[e.target_id].concept_cypher_signature(
                f'n{e.target_id}',
                known=e.target_id in known_ids
            )
            links.append(f"{source_signature}{e.cypher_signature(f'e{e.id}')}{target_signature}")
            known_ids.update([e.source_id, e.target_id])
        return links

    @property
    def named_nodes(self):
        return [n.id for n in self.machine_question['nodes'] if n.curie]

    def generate_concept_cypher(self):
        """Generate a cypher query to find paths through the concept-level map."""
        named_node_names = [f'n{n}' for n in self.named_nodes]
        node_names = [f'n{n.id}' for n in self.machine_question['nodes']]
        edge_names = [f"e{e.id}" for e in self.machine_question['edges']]
        cypherbuffer = [f"MATCH {s}" for s in self.concept_cypher_signature]
        node_list = f"""[{', '.join([f"'{n}'" for n in node_names])}]"""
        named_node_list = f"""[{', '.join([f"'{n}'" for n in named_node_names])}]"""
        edge_list = f"[{', '.join(edge_names)}]"
        edge_switches = [f"CASE startnode(e{e.id}) WHEN n{e.source_id} THEN ['n{e.source_id}','n{e.target_id}'] ELSE ['n{e.target_id}','n{e.source_id}'] END AS e{e.id}_pair" for e in self.machine_question['edges']]
        edge_pairs = [f"e{e.id}_pair" for e in self.machine_question['edges']]
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
            transitions = {int(k[1:]): {int(k[1:]): [] for k in nodes} for k in nodes}
            for e in edges:
                edge = edges[e]
                source_id = int(edge['source'][1:])
                target_id = int(edge['target'][1:])
                trans = {
                    "op": edge['op'],
                    "link": edge['predicate']
                }
                transitions[source_id][target_id].append(trans)
            
            plans.append(transitions)
        return plans

    def compile(self, rosetta):
        plans = self.get_transitions(rosetta.type_graph, self.generate_concept_cypher())

        # merge plans
        plan = {n.id: {n.id: [] for n in self.machine_question['nodes']} for n in self.machine_question['nodes']}
        for p in plans:
            for source_id in p:
                for target_id in p[source_id]:
                    plan[source_id][target_id].extend(p[source_id][target_id])

        # remove duplicate transitions
        for source_id in plan:
            for target_id in plan:
                plan[source_id][target_id] = {t['op']:t for t in plan[source_id][target_id]}.values()

        if not plan:
            raise RuntimeError('No viable programs.')

        from greent.program import Program
        program = Program(plan, self.machine_question['nodes'], rosetta, 0)
        programs = [program]
        
        return programs