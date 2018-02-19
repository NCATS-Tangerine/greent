import json
import requests
from greent import node_types
from greent.graph_components import KNode, KEdge
from greent.service import Service


class HGNC(Service):

    """ Generic GENE id translation service. Essentially a highly generic synonym finder. """
    def __init__(self, context): 
        super(HGNC, self).__init__("hgnc", context)

    def  get_name(self, node):
        """Given a node for an hgnc, return the name for that id"""
        if node.node_type != node_types.GENE:
            raise ValueError('Node must be a gene')
        identifier_parts = node.identifier.split(':')
        if identifier_parts[0] == 'HGNC':
            query_string='hgnc_id'
        elif identifier_parts[0].upper() == 'NCBIGENE':
            query_string = 'entrez_id'
        else:
            raise ValueError('Node must represent an HGNC or NCBIGene id.')
        hgnc_id = identifier_parts[1]
        headers = {'Accept':'application/json'}
        r = requests.get('%s/%s/%s' % (self.url, query_string, hgnc_id), headers= headers).json()
        try:
            symbol = r['response']['docs'][0]['symbol']
        except:
            import json
            json.dumps(r,indent=2)
            symbol = hgnc_id
        return symbol 

    #todo, it would probably be straightforward to autogenerate these and have common logic for them
    def ncbigene_to_uniprotkb(self, node):
        """Given a node representing an NCBIGene (or Entrez) identifier, retrieve the UniProtKB identifier"""
        if node.node_type != node_types.GENE:
            raise ValueError('Node must be a gene')
        identifier_parts = node.identifier.split(':')
        if not identifier_parts[0].upper() == 'NCBIGENE':
            raise ValueError('Node must represent an NCBIGENE identifier.')
        hgnc_id = identifier_parts[1]
        headers = {'Accept':'application/json'}
        r = requests.get('{0}/entrez_id/{1}'.format(self.url, hgnc_id), headers= headers).json()
        try:
            uniprots = r['response']['docs'][0]['uniprot_ids']
            return  [  ( KEdge( 'hgnc', 'ncbigene_to_uniprotkb', is_synonym=True ),\
                         KNode( identifier='UNIPROTKB:{}'.format(uniprot), node_type = node_types.GENE )) \
                         for uniprot in uniprots ]
        except (IndexError, KeyError):
            #No results back
            return []

    def hgnc_to_uniprotkb(self, node):
        """Given a node representing an HGNC retrieve the UniProtKB identifier"""
        if node.node_type != node_types.GENE:
            raise ValueError('Node must be a gene')
        identifier_parts = node.identifier.split(':')
        if not identifier_parts[0].upper() == 'HGNC':
            raise ValueError('Node must represent an HGNC identifier.')
        hgnc_id = identifier_parts[1]
        headers = {'Accept':'application/json'}
        r = requests.get('{0}/hgnc_id/{1}'.format(self.url, hgnc_id), headers= headers).json()
        try:
            uniprots = r['response']['docs'][0]['uniprot_ids']
            return  [  ( KEdge( 'hgnc', 'ncbigene_to_uniprotkb', is_synonym=True ),\
                         KNode( identifier='UNIPROTKB:{}'.format(uniprot), node_type = node_types.GENE )) \
                         for uniprot in uniprots ]
        except (IndexError,KeyError):
            #No results back
            return []

def test():
    from greent.service import ServiceContext 
    hgnc = HGNC( ServiceContext.create_context() )
    input_knode = KNode( 'NCBIGENE:3815' , node_type = node_types.GENE )
    print( hgnc.ncbigene_to_uniprotkb( input_knode ) )

if __name__ == '__main__':
    test()

