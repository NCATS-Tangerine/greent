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

from greent.program import Program
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
            identifiers:
                type: array
                items:
                    type: string
                default: []
    """
    def __init__(self, *args, **kwargs):
        self.id = None
        self.type = None
        self.curie = None
        self.name = None

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
            source_id:
                type: string
            target_id:
                type: string
            min_length:
                type: integer
                default: 1
            max_length:
                type: integer
                default: 1
    """
    def __init__(self, *args, **kwargs):
        self.source_id = None
        self.target_id = None
        self.min_length = 1
        self.max_length = 1

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
                'edges': [Edge(e) for e in value['edges']]
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
            links.append(f"{source_signature}{e.cypher_signature(f'e{idx}')}{target_signature}")
            known_ids.update([e.source_id, e.target_id])
        return links

    @property
    def named_nodes(self):
        return [n.id for n in self.machine_question['nodes'] if n.curie]

    def generate_concept_cypher(self):
        """Generate a cypher query to find paths through the concept-level map."""
        links = self.concept_cypher_signature
        num_links = len(links)
        node_names = [f'n{n.id}' for n in self.machine_question['nodes']]
        edge_names = [f"e{idx}" for idx in range(num_links)]
        cypherbuffer = [f"MATCH {s}" for s in self.concept_cypher_signature]
        node_list = f"[{', '.join(node_names)}]"
        edge_list = f"[{', '.join(edge_names)}]"
        cypherbuffer.append(f'WHERE robokop.traversable({node_list}, {edge_list}, [{", ".join([f"n{idx}" for idx in self.named_nodes])}])')
        # This is to make sure that we don't get caught up in is_a and other funky relations.:
        cypherbuffer.append(f'AND ALL(r in {edge_list} WHERE EXISTS(r.op))')
        node_map = f"{{{', '.join([f'{n}:{n}' for n in node_names])}}}"
        edge_map = f"{{{', '.join([f'{e}:{e}' for e in edge_names])}}}"
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

            # convert keys 'n#' to #
            nodes = {int(k[1:]):nodes[k] for k in nodes}

            # map internal Neo4j id to our id
            id_map = {nodes[k].id:k for k in nodes}

            # extract transitions
            transitions = {k:[] for k in nodes}
            for e in edges:
                edge = edges[e]
                props = edge.properties
                source_id = id_map[edge.start]
                target_id = id_map[edge.end]
                trans = {
                    "op": props['op'],
                    "link": props['predicate'],
                    "target_id": target_id
                }
                transitions[source_id].append(trans)

            plans.append(transitions)
        return plans

    def compile(self, rosetta):
        plans = self.get_transitions(rosetta.type_graph, self.generate_concept_cypher())
        programs = []
        for i,plan in enumerate(plans):
            try:
                # Some programs are bogus (when you have input to a named node) 
                # it throws an exception then, and we ignore it.
                program = Program(plan, self.machine_question['nodes'], rosetta, i)
                programs.append(program)
            except Exception as err:
                logger.warn(f'WARN: {err}')
        if not programs:
            raise RuntimeError('No viable programs.')
        return programs