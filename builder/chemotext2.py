import json
import logging
from greent.graph_components import KEdge
from greent.service import ServiceContext
from greent import chemotext2
from greent import node_types
#import nltk

def get_supporter(greent):
    return Chemotext2Support(greent)

class Chemotext2Support():

    def __init__(self,greent):
        greent.chemotext2 = chemotext2.Chemotext2( ServiceContext.create_context() )
        self.chemotext2 = greent.chemotext2
        self.badwords = set(['disease','virus','infection','fever','syndrome','hemorrhagic','disorder',\
                    'gene','cell','system','tissue','non','positive','negative','receptor',\
                    'type','severe','perinatal','form','adult','onset','nonsyndromic','syndromic',\
                    'syndrome','infantile','juvenile','early','late','chronic','rare',\
                    'autosomal','dominant','recessive', 'congenital','hereditary','familial',\
                    'male','female','with','without','single','mutation','isolated','absence','group', \
                    'susceptibility','plus','essential','distal','and','during','continuous',\
                    'due','deficiency','extensive','large','small','pro','partial','complete','morbid', \
                    'central','distal','middle','deficit','defect','status','rhythm','like'])

    def prepare(self,nodes):
        pass
     
    def generate_phrases(self,phrase):
        """From a phrase, find the 1 or 2 word queries into chemotext"""
        #Adding the individual words when the phrase is longer than 1 has problems.  For instance
        # something like 'Ebola virus' vs 'neimann-pick disease' will end up doing queries
        # of 'virus' vs 'disease' which will give a big score, but is meaningless
        punctuation='()-,;./'
        for p in punctuation:
            phrase = ' '.join( phrase.split(p) )
        words = phrase.split() #on whitespace
        if len(words) == 1:
            return [ phrase ]
        goodwords = [ w for w in words if (len(w) > 2) and w.lower() not in self.badwords ]
        return goodwords


    def term_to_term(self,node_a,node_b,limit = 10000):
        """Given two terms, find articles in chemotext that connect them, and return as a KEdge.
        If nothing is found, return None"""
        logging.getLogger('application').debug('chemotext2: "{}" to "{}"'.format(node_a.name, node_b.name))
        phrases_a = self.generate_phrases(node_a.name)
        phrases_b = self.generate_phrases(node_b.name)
        maxr = -1
        besta = ''
        bestb = ''
        for p_a in phrases_a:
            for p_b in phrases_b:
                if p_a == p_b:
                    continue
                r = self.chemotext2.get_semantic_similarity( p_a, p_b )
                if r > maxr:
                    maxr = r
                    besta = p_a
                    bestb = p_b
                logging.getLogger('application').debug('  "{}"-"{}": {} ({})'.format(p_a, p_b, r, maxr) ) 
        logging.getLogger('application').debug(' "{}"-"{}": {}'.format(besta, bestb, maxr) ) 
        if maxr > -1:
            raise RuntimeError('The following KEdge constructor looks somewhat suspect.')
            ke= KEdge( 'chemotext2', 'term_to_term', { 'similarity':maxr, 'terms':[besta, bestb] }, is_support = True )
            ke.source_node = node_a
            ke.target_node = node_b
            return ke
        return None


if __name__ == '__main__':
    test()
