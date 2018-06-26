'''
Question definition
'''

# standard modules
import os
import sys
import json
import hashlib
import warnings

from builder.api.setup import swagger
from builder.util import FromDictMixin

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

    def concept_cypher_signature(self):
        if not self.min_length == self.max_length == 1:
            return f"-[*{self.min_length}..{self.max_length}]-"
        else:
            return  "--"

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
        for e in self.machine_question['edges']:
            source_signature = node_map[e.source_id].concept_cypher_signature(
                f'n{e.source_id}',
                known=e.source_id in known_ids
            )
            target_signature = node_map[e.target_id].concept_cypher_signature(
                f'n{e.target_id}',
                known=e.target_id in known_ids
            )
            links.append(f"{source_signature}{e.cypher_signature()}{target_signature}")
            known_ids.update([e.source_id, e.target_id])
        return links

    @property
    def named_nodes(self):
        return [n.id for n in self.machine_question['nodes'] if n.curie]

    def generate_concept_cypher(self):
        """Generate a cypher query to find paths through the concept-level map."""
        links = self.concept_cypher_signature
        num_links = len(links)
        path_names = [f"p{idx}" for idx in range(num_links)]
        cypherbuffer = [f"MATCH {p}={s}" for p, s in zip(path_names, self.concept_cypher_signature)]
        nodes = ' + '.join([f'nodes({p})' for p in path_names])
        relationships = ' + '.join([f'relationships({p})' for p in path_names])
        cypherbuffer.append(f'WHERE robokop.traversable({nodes}, {relationships}, [{", ".join([f"n{idx}" for idx in self.named_nodes])}])')
        # This is to make sure that we don't get caught up in is_a and other funky relations.:
        cypherbuffer.append(f'AND ALL(r in {relationships} WHERE EXISTS(r.op))')
        cypherbuffer.append(f"RETURN {', '.join(path_names)}")
        return '\n'.join(cypherbuffer)
