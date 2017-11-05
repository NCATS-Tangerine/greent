import json
import requests
from reasoner import node_types
from greent.service import Service

class HGNC(Service):

    """ Generic id translation service. Essentially a highly generic synonym finder. """
    def __init__(self, context): 
        super(HGNC, self).__init__("hgnc", context)

    def  get_name(self, node):
        """Given a node for an hgnc, return the name for that id"""
        if node.node_type != node_types.GENE:
            raise ValueError('Node must be a gene')
        identifier_parts = node.identifier.split(':')
        if not identifier_parts[0] == 'HGNC':
            raise ValueError('Node must represent and HGNC id.')
        hgnc_id = identifier_parts[1]
        headers = {'Accept':'application/json'}
        r = requests.get('%s/hgnc_id/%s' % (self.url, hgnc_id), headers= headers).json()
        symbol = r['response']['docs'][0]['symbol']
        return symbol 
