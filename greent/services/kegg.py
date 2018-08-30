import requests
import json
from greent.service import Service
from greent.util import LoggingUtil
from greent.util import Text
from greent.graph_components import KNode
from greent import node_types
from builder.question import LabeledID

logger = LoggingUtil.init_logging (__name__)

class KEGG(Service):
    """ Access HMDB via the beacon """

    def __init__(self, context):
        super(KEGG, self).__init__("kegg", context)

    def parse_raw_results(self, raw_results, results,prefix):
        rawlines = raw_results.text.split('\n')
        lines = list(filter(lambda y: len(y) == 2, [x.split('\t') for x in rawlines]))
        for line in lines:
            rn = line[1]
            if not rn.startswith(prefix):
                continue
            results.append(rn)

    def parse_chemicals(self, line):
        n = line.split()
        return set( filter( lambda x: x.startswith('C'), n))

    def chemical_get_reaction(self,chemnode):
        identifiers = chemnode.get_synonyms_by_prefix('KEGG.COMPOUND')
        results = []
        for cid in identifiers:
            url = f'{self.url}/link/reaction/{Text.un_curie(cid)}'
            raw_results = requests.get(url)
            self.parse_raw_results(raw_results, results,'rn')
        return results

    def enzyme_get_reaction(self,enzymenode):
        identifiers = enzymenode.get_synonyms_by_prefix('EC')
        results = []
        for cid in identifiers:
            url = f'{self.url}/link/reaction/{Text.un_curie(cid)}'
            raw_results = requests.get(url)
            self.parse_raw_results(raw_results, results,'rn')
        return results


    def reaction_get_chemicals(self,reaction_id):
        results=[]
        rid = reaction_id.split(':')[1]
        url = f'{self.url}/link/cpd/{rid}'
        raw_results = requests.get(url)
        self.parse_raw_results(raw_results, results, 'cpd')
        return results

    def get_reaction(self,reaction_id):
        url = f'{self.url}/get/{reaction_id}'
        reaction = {}
        raw_results = requests.get(url)
        for line in raw_results.text.split('\n'):
            if line.startswith('ENZYME'):
                parts = line.split()
                reaction['enzyme'] = f'EC:{parts[1]}'
            elif line.startswith('EQUATION'):
                parts = line[9:].split('=')
                left = parts[0]
                right = parts[1]
                reaction['reactants'] = self.parse_chemicals(left)
                reaction['products'] = self.parse_chemicals(right)
        return reaction

    def chemical_get_enzyme(self,chemnode):
        """To get an enzyme from chemicals, we first look up the reactions for the chemical.
        Then we pull the reaction which gives us (1) the enzyme and (2) whether the chemical
        is a reactant or a product."""
        reactions = self.chemical_get_reaction(chemnode)
        chemids = set([Text.un_curie(x) for x in chemnode.get_synonyms_by_prefix('KEGG.COMPOUND')])
        results = []
        for reaction_id in reactions:
            rxn = self.get_reaction(reaction_id)
            if 'enzyme' in rxn:
                enzyme = KNode(rxn['enzyme'], type=node_types.GENE)
                if len(chemids.intersection(rxn['reactants'])) > 0:
                    predicate = LabeledID('RO:0002449','negatively regulates, entity to entity')
                    input_identifier = chemids.intersection(rxn['reactants']).pop()
                elif len(chemids.intersection(rxn['products'])) > 0:
                    predicate = LabeledID('RO:0002450','positively regulates, entity to entity')
                    input_identifier = chemids.intersection(rxn['products']).pop()
                else:
                    logger.error(f"Mismatch between query and answer: {rxn} {chemids}")
                    continue
                edge = self.create_edge(enzyme, chemnode, f'kegg.chemical_get_enzyme',  input_identifier, predicate)
                results.append( (edge, enzyme))
        return results


    def add_chem_results(self,chem_ids, predicate, enzyme_node, input_identifier, results, rset):
        for chem_id in chem_ids:
            if chem_id not in rset:
                chem_node = KNode(f'KEGG.COMPOUND:{chem_id}', type=node_types.CHEMICAL_SUBSTANCE)
                edge = self.create_edge(enzyme_node, chem_node, f'kegg.enzyme_get_chemicals',  input_identifier, predicate)
                results.append( (edge, chem_node))
                rset.add(chem_id)


    def enzyme_get_chemicals(self,enzyme_node):
        """To get chemicals from an enzyme, we first look up the reactions for the enzyme.
        Then we pull the reaction which gives us (1) the chemicals and (2) whether the chemical
        is a reactant or a product."""
        reactions = self.enzyme_get_reaction(enzyme_node)
        enzyme_ids = set([Text.un_curie(x) for x in enzyme_node.get_synonyms_by_prefix('EC')])
        results = []
        reactset=set()
        prodset=set()
        for reaction_id in reactions:
            rxn = self.get_reaction(reaction_id)
            input_identifier = rxn['enzyme']
            self.add_chem_results(rxn['reactants'], LabeledID('RO:0002449','negatively regulates, entity to entity'),enzyme_node,input_identifier,results,reactset)
            self.add_chem_results(rxn['products'], LabeledID('RO:0002449','negatively regulates, entity to entity'),enzyme_node,input_identifier,results,prodset)
        return results


