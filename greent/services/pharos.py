import asyncio
import concurrent.futures
import requests
import json
import logging
import sys
import os
import traceback
import datetime
from collections import defaultdict
from collections import namedtuple
from csv import DictReader
from greent.util import Munge
from greent.util import Text
from greent.service import Service
from greent.util import LoggingUtil
from greent.async import AsyncUtil
from greent.async import Operation
from greent.graph_components import KEdge, KNode
from greent import node_types
from simplejson.scanner import JSONDecodeError

logger = LoggingUtil.init_logging(__name__, logging.DEBUG)


class Pharos(Service):
    def __init__(self, context):
        super(Pharos, self).__init__("pharos", context)

    def request(self, url):
        response = None
        try:
            response = requests.get(url).json()
        except:
            traceback.print_exc()
        return response

    def query(self, query):
        return self.request("{0}/{1}".format(self.url, query))

    def target_to_id(self, target):
        target_map = self.query(query="targets/{0}".format(Munge.gene(target)))
        return target_map["id"] if "id" in target_map else None

    def target_to_disease(self, target_sym):
        target_id = self.target_to_id(Munge.gene(target_sym))
        links = self.query(query="targets({0})/links".format(target_id))
        result = []
        for k in links:
            if k['kind'] == "ix.idg.models.Disease":
                for p in k['properties']:
                    # print (p)
                    if p['label'] == 'IDG Disease':
                        result.append(p['term'])
        return result

    def disease_map(self, disease_id):
        return self.query(query="diseases({0})".format(disease_id))

    def make_doid_id(self, obj):
        if not obj:
            return None
        result = obj
        if isinstance(obj, KNode):
            result = obj.identifier
        if isinstance(result, list):
            if len(result) == 1:
                result = result[0]
        if result:
            if isinstance(result, str):
                if result.startswith('DOID:'):
                    result = result.replace('DOID:', '')
        return result

    def translate(self, subject_node):
        """Convert a subject with a DOID or UMLS into a Pharos Disease ID"""
        # TODO: This relies on a pretty ridiculous caching of a map between pharos ids and doids.
        #      As Pharos improves, this will not be required, but for the moment I don't know a better way.
        pmap = defaultdict(list)
        print (f"-------------------- {subject_node}")
        pharos_id_filename = os.path.join(os.path.dirname(__file__), 'pharos.id.all.txt')
        with open(pharos_id_filename, 'r') as inf:
            rows = DictReader(inf, dialect='excel-tab')
            for row in rows:
                if row['DOID'] != '':
                    doidlist = row['DOID'].split(',')
                    for d in doidlist:
                        pmap[d.upper()].append(row['PharosID'])
        valid_identifiers = subject_node.get_synonyms_by_prefix('DOID')
        valid_identifiers.update(subject_node.get_synonyms_by_prefix('UMLS'))
        pharos_set = set()
        for vi in valid_identifiers:
            pharos_set.update(pmap[vi])
        pharos_list = list(pharos_set)
        return pharos_list

    def target_to_hgnc(self, target_id):
        """Convert a pharos target id into an HGNC ID.
        The call does not return the actual name for the gene, so we do not provide it.
        There are numerous other synonyms that we could also cache, but I don't see much benefit here. """
        result = None
        try:
            r = requests.get('https://pharos.nih.gov/idg/api/v1/targets(%s)/synonyms' % target_id)
            result = r.json()
            for synonym in result:
                if synonym['label'] == 'HGNC':
                    result = synonym['term']
        except:
            pass
        return result

    def drugname_string_to_pharos_info(self, drugname):
        """Exposed for use in name lookups without KNodes"""
        r = requests.get('https://pharos.nih.gov/idg/api/v1/ligands/search?q={}'.format(drugname)).json()
        return_results = set()
        foundany = False
        for contents in r['content']:
            foundany=True
            synonym_href = contents['_synonyms']['href']
            sres = requests.get(synonym_href).json()
            for syno in sres:
                if syno['href'].startswith('https://www.ebi.ac.uk/chembl/compound/inspect/'):
                    term = syno['href'].split('/')[-1]
                    identifier = 'CHEMBL:{}'.format(term)
                    return_results.add( (identifier, drugname) )
        if foundany and len(return_results) == 0:
            print('UHOH')
            print(drugname)
            exit()
        return list(return_results)

    def drugname_to_pharos(self, namenode):
        drugname = Text.un_curie(namenode.identifier)
        pharosids = self.drugname_string_to_pharos_info(drugname)
        results = []
        for pharosid, pharoslabel in pharosids:
            newnode = KNode(pharosid, node_types.DRUG, label=pharoslabel)
            newedge = KEdge('pharos', 'drugname_to_pharos', {})
            results.append((newedge, newnode))
        return results

    def drug_get_gene(self, subject):
        """ Get a gene from a drug. """
        resolved_edge_nodes = []
        for s in subject.synonyms:
            if Text.get_curie(s) == 'CHEMBL':
                pharosid = Text.un_curie(s)
                original_edge_nodes = []
                r = requests.get('https://pharos.nih.gov/idg/api/v1/ligands(%s)?view=full' % pharosid)
                result = r.json()
                actions = set()  # for testing
                for link in result['links']:
                    if link['kind'] == 'ix.idg.models.Target':
                        pharos_target_id = int(link['refid'])
                        edge_properties = {}
                        for prop in link['properties']:
                            if prop['label'] == 'Pharmalogical Action':  # !
                                actions.add(prop['term'])
                        pharos_edge = KEdge('pharos', 'drug_get_gene', {'properties': link['properties']})
                        # Pharos returns target ids in its own numbering system. Collect other names for it.
                        hgnc = self.target_to_hgnc(pharos_target_id)
                        if hgnc is not None:
                            hgnc_node = KNode(hgnc, node_types.GENE)
                            resolved_edge_nodes.append((pharos_edge, hgnc_node))
                        else:
                            logging.getLogger('application').warn('Did not get HGNC for pharosID %d' % pharos_target_id)
        return resolved_edge_nodes

    def disease_get_gene(self, subject):
        """ Get a gene from a pharos disease id. """
        pharos_ids = self.translate(subject)
        resolved_edge_nodes = []
        print (f"-------------------- {pharos_ids}")

        for pharosid in pharos_ids:
            logging.getLogger('application').debug("Identifier:" + subject.identifier)
            original_edge_nodes = []
            r = requests.get('https://pharos.nih.gov/idg/api/v1/diseases(%s)?view=full' % pharosid)
            result = r.json()
            for link in result['links']:
                if link['kind'] == 'ix.idg.models.Target':
                    pharos_target_id = int(link['refid'])
                    pharos_edge = KEdge('pharos', 'disease_get_gene', {'properties': link['properties']})
                    # Pharos returns target ids in its own numbering system. Collect other names for it.
                    hgnc = self.target_to_hgnc(pharos_target_id)
                    if hgnc is not None:
                        hgnc_node = KNode(hgnc, node_types.GENE)
                        resolved_edge_nodes.append((pharos_edge, hgnc_node))
                    else:
                        logging.getLogger('application').warn('Did not get HGNC for pharosID %d' % pharos_target_id)
        return resolved_edge_nodes


class AsyncPharos(Pharos):
    """ Prototype asynchronous requests. In general we plan to have asynchronous requests and
    caching to accelerate query responses. """

    def __init__(self, context):
        super(AsyncPharos, self).__init__(context)

    def disease_get_gene(self, subject):
        pharosids = subject.identifier
        original_edge_nodes = []

        def process_pharos_response(r):
            try:
                result = r.json()
                for link in result['links']:
                    if link['kind'] != 'ix.idg.models.Target':
                        logger.info('Pharos disease returning new kind: %s' % link['kind'])
                    else:
                        pharos_target_id = int(link['refid'])
                        pharos_edge = KEdge('pharos', 'disease_get_gene', {'properties': link['properties']})
                        original_edge_nodes.append((pharos_edge, pharos_target_id))
            except JSONDecodeError as e:
                pass  # logger.error ("got exception %s", e)

        AsyncUtil.execute_parallel_requests(
            urls=["https://pharos.nih.gov/idg/api/v1/diseases(%s)?view=full" % p for p in pharosids],
            response_processor=process_pharos_response)

        logger.debug("        Getting hgnc ids for pharos id: {}".format(pharosids))
        resolved_edge_nodes = []
        HGNCRequest = namedtuple('HGNCRequest', ['pharos_target_id', 'edge'])
        index = 0

        def process_hgnc_request(request):
            nonlocal index
            index += 1
            url = "https://pharos.nih.gov/idg/api/v1/targets(%s)/synonyms" % request.pharos_target_id
            if index < 3:
                logger.debug("      hgnc_url:  {0}".format(url))
            return (requests.get(url).json(), request.edge)

        def process_hgnc_response(response):
            result = response[0]
            edge = response[1]
            hgnc = None
            for synonym in result:
                if synonym['label'] == 'HGNC':
                    hgnc = synonym['term']
            if hgnc is not None:
                hgnc_node = KNode(hgnc, node_types.GENE)
                resolved_edge_nodes.append((edge, hgnc_node))

        AsyncUtil.execute_parallel_operations(
            operations=[Operation(process_hgnc_request, HGNCRequest(pharos_target_id, edge)) for edge, pharos_target_id
                        in original_edge_nodes],
            # [:1],
            response_processor=process_hgnc_response)

        return resolved_edge_nodes


# Poking around on the website there are about 10800 ( a few less )
def build_disease_translation():
    """Write to disk a table mapping Pharos disease ID to DOID (and other?) so we can reverse lookup"""
    with open('services/pharos.id.all.txt', 'w') as pfile:
        pfile.write('PharosID\tDOID\n')
        for pharosid in range(1, 10800):
            r = requests.get('https://pharos.nih.gov/idg/api/v1/diseases(%d)/synonyms' % pharosid).json()
            doids = []
            for synonym in r:
                # if synonym['label'] == 'DOID':
                doids.append(synonym['term'])
            if len(doids) > 1:
                import json
                exit()
            elif len(doids) == 0:
                doids.append('')
            pfile.write('%d\t%s\n' % (pharosid, doids[0]))



