import logging
import requests
from greent.service import Service
from greent.util import Text,LoggingUtil
from greent.graph_components import KNode,LabeledID
from greent import node_types
from datetime import datetime as dt
import time

logger = LoggingUtil.init_logging(__name__)

class QuickGo(Service):

    def __init__(self, context):
        super(QuickGo, self).__init__("quickgo", context)
        self.go = context.core.go

    def get_predicate(self, p_label):
        labels2identifiers={'occurs_in': 'BFO:0000066',
                'enables': 'RO:0002327',
                'involved_in': 'RO:0002331',
                'results_in_movement_of': 'RO:0002565',
                'results in developmental progression of':'RO:0002295',
                'results in development of':'RO:0002296',
                'results in formation of':'RO:0002297',
                'results in synthesis of':'RO:0002587',
                'results in assembly of':'RO:0002588',
                'results in morphogenesis of':'RO:0002298',
                'results in maturation of':'RO:0002299',
                'results in acquisition of features of':'RO:0002315',
                'results in growth of':'RO:0002343',
                'results in commitment to':'RO:0002348',
                'results in determination of':'RO:0002349',
                'results in structural organization of':'RO:0002355',
                'results in specification of':'RO:0002356',
                'results in developmental induction of':'RO:0002357',
                'results in ending of':'RO:0002552',
                'results in disappearance of':'RO:0002300',
                'results in developmental regression of':'RO:0002301',
                'results in closure of':'RO:0002585',
                }
        try:
            return LabeledID(labels2identifiers[p_label],p_label)
        except:
            logger.warn(p_label)
            return LabeledID(f'GO:{p_label}',p_label)

    def standardize_predicate(self, predicate, source, target):
        """Fall back to a catch-all if we can't find a specific mapping"""
        try:
            return super(QuickGo, self).standardize_predicate(predicate)
        except:
            #If the target is a molecular function, use RO:0002215 capable_of
            if self.go.is_biological_process(target):
                return super(QuickGo,self).standardize_predicate(LabeledID('RO:0002215','capable_of'))
            #If the target is a biological process, use RO:0002331 involved_in
            if self.go.is_molecular_function(target):
                return super(QuickGo,self).standardize_predicate(LabeledID('RO:0002331','involved_in'))
            #If the target is a cell, use 'occurs_in'
            if target.node_type == node_types.CELL:
                return super(QuickGo,self).standardize_predicate(self.get_predicate('occurs_in'))

    #TODO: share the retry logic in Service?
    def query(self,url):
        done = False
        num_tries = 0
        max_tries = 10
        wait_time = 5 # seconds
        while num_tries < max_tries:
            try:
                return requests.get(url).json()
            except:
                num_tries += 1
                time.sleep(wait_time)
        return None
 

    def page_calls(self,url):
        response = self.query(url)
        if 'results' not in response:
            return []
        allresults = response['results']
        total_pages = response['pageInfo']['total']
        for page in range(2,total_pages+1):
            url_page = url+f'&page={page}'
            response = self.query(url_page)
            if 'results' in response:
                allresults += response['results']
        return allresults

    def go_term_to_cell_xontology_relationships(self, go_node):
        #This call is not paged!
        url = "{0}/QuickGO/services/ontology/go/terms/GO:{1}/xontologyrelations".format (self.url, Text.un_curie(go_node.identifier))
        response = self.query(url)
        if 'results' not in response:
            return []
        results = []
        for r in response['results']:
            if 'xRelations' in r:
                for xrel in r['xRelations']:
                    if xrel['id'].startswith('CL:'):
                        predicate = self.get_predicate(xrel['relation'])
                        cell_node = KNode (xrel['id'], node_types.CELL, label = xrel['term']) 
                        edge = self.create_edge(go_node, cell_node,'quickgo.go_term_to_cell_xontology_relationships',go_node.identifier,predicate,url = url)
                        results.append( ( edge , cell_node))
        return results

    def go_term_to_cell_annotation_extensions(self,go_node):
        """This is playing a little fast and loose with the annotations.  Annotations relate a gene to a go term,
        and they can have an extension like occurs_in(celltype). Technically, that occurs_in only relates to that
        particular gene/go combination.  But it's the only way to traverse from neurotransmitter release to neurons 
        that is currently available"""
        url = '{0}/QuickGO/services/annotation/search?includeFields=goName&goId=GO:{1}&taxonId=9606&extension=occurs_in(CL)'.format( self.url, Text.un_curie(go_node.identifier)) 
        call_results = self.page_calls(url)
        cell_ids = set()
        results = []
        for r in call_results:
            for e in r['extensions']:
                for c in e['connectedXrefs']:
                    if c['db'] == 'CL':
                        if c['id'] not in cell_ids:
                            predicate = self.get_predicate(c['qualifier'])
                            #Bummer, don't get a name
                            cell_node = KNode( 'CL:{}'.format(c['id']), node_types.CELL )
                            edge = self.create_edge(go_node, cell_node, 'quickgo.go_term_to_cell_annotation_extensions',go_node.identifier,predicate,url = url)
                            results.append( (edge,cell_node ) )
                            cell_ids.add(c['id'])
        return results

    def cell_to_go_term_annotation_extensions(self,cell_node):
        url=f'{self.url}/QuickGO/services/annotation/search?includeFields=goName&aspect=biological_process,molecular_function&extension={cell_node.identifier}'
        call_results = self.page_calls(url)
        go_ids = set([ (r['goId'],r['goName']) for r in call_results ])
        predicate=self.get_predicate('occurs_in')
        #Don't get a name for the go term.. there is a goName field, but it's always NULL.
        nodes = [ KNode( idlabel[0] , node_types.PROCESS_OR_FUNCTION, label=idlabel[1] ) for idlabel in go_ids ]
        edges = [ self.create_edge(go_node, cell_node, 'quickgo.cell_to_go_term_annotation_extensions', cell_node.identifier, predicate, url = url) for go_node in nodes ] 
        return list(zip(edges,nodes))

    def go_term_to_gene_annotation_strict(self,node):
        go = node.identifier
        url = f'{self.url}/QuickGO/services/annotation/search?goId={go}&taxonId=9606&goUsage=exact&targetSet=referencegenome'
        call_results = self.page_calls(url)
        used = set()
        results = [] 
        for r in call_results:
            uniprotid = r["geneProductId"]
            if uniprotid not in used:
                used.add(uniprotid)
                predicate = self.get_predicate(r['qualifier'])
                gene_node = KNode( uniprotid, node_types.GENE ) 
                edge = self.create_edge(node, gene_node, 'quickgo.go_term_to_gene_annotation',node.identifier,predicate,url = url)
                results.append( (edge,gene_node ) )
        return results

    def go_term_to_gene_annotation(self,node):
        go = node.identifier
        url = f'{self.url}/QuickGO/services/annotation/search?goId={go}&taxonId=9606&goUsage=exact'
        #print(url)
        call_results = self.page_calls(url)
        used = set()
        results = [] 
        for r in call_results:
            uniprotid = r["geneProductId"]
            #quickgo returns all kinda stuff: rna central ids, complex portal ids.  These are good, but we can't do
            #anything with them, so for now, dump em.
            if uniprotid not in used and uniprotid.upper().startswith('UNIPROT'):
                used.add(uniprotid)
                predicate = self.get_predicate(r['qualifier'])
                gene_node = KNode( uniprotid, node_types.GENE ) 
                edge = self.create_edge(gene_node, node, 'quickgo.go_term_to_gene_annotation',node.identifier,predicate,url = url)
                results.append( (edge,gene_node ) )
        return results
