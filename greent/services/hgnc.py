import requests
from greent import node_types
from greent.graph_components import KNode, KEdge
from greent.service import Service


#A map from identifiers.org namespaces (curie prefixes) to how HGNC describes these things
prefixes_to_hgnc = {
    'HGNC': 'hgnc_id',
    'NCBIGENE': 'entrez_id',
    'NCBIGene': 'entrez_id',
    'HGNC.SYMBOL': 'symbol',
    'OMIM': 'omim_id',
    #UNIPROTKB is not a identifiers.org prefix.  Uniprot is, and uniprot.isoform is.
    'UNIPROTKB': 'uniprot_ids',
    'ENSEMBL': 'ensembl_gene_id'
}

hgnc_to_prefixes = { v: k for k,v in prefixes_to_hgnc.items()}

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

    def get_synonyms(self, identifier):
        identifier_parts = identifier.split(':')
        prefix = identifier_parts[0]
        id = identifier_parts[1]
        query_type = prefixes_to_hgnc[prefix]
        headers = {'Accept':'application/json'}
        r = requests.get('%s/%s/%s' % (self.url, query_type, id), headers= headers).json()
        docs = r['response']['docs']
        synonyms = set()
        for doc in docs:
            for key in doc:
                if key in hgnc_to_prefixes:
                    values = doc[key]
                    prefix = hgnc_to_prefixes[key]
                    if not isinstance(values, list):
                        values = [values]
                    for value in values:
                        if ':' in value:
                            value = value.split(':')[-1]
                        synonym = f'{prefix}:{value}'
                        synonyms.add(synonym)
        return synonyms

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

