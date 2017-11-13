import gensim.models
import json
import logging
import os
import requests
import time
from greent.service import Service
from greent.service import ServiceContext
from greent.util import LoggingUtil
from pprint import pformat

logger = LoggingUtil.init_logging (__file__, level=logging.DEBUG)

class Chemotext2 (Service):
    """ RENCI used the word2vec machine learning algorithm to compute teh semantic similarity of terms in the public access
    subset of the PubMed Central full text journal article corpus. Among other things, word2vec mdoels let us interrogate
    the "distance" between two terms. For a full explanation of the significance of distance, see the literature on word 
    embedding models and the word2vec algorithm. For our purposes, it's a sophisticated view of cooccurrence. """
    def __init__(self, context): #url="https://www.ebi.ac.uk/spot/oxo/api/search?size=500"):
        super(Chemotext2, self).__init__("chemotext2", context)
        logger.debug ("Ensuring presence of word to vec pubmed central models: {0}".format (self.url))
        files = [
            "pmc-2016.w2v", "pmc-2016.w2v.syn0.npy", "pmc-2016.w2v.syn1neg.npy",
            "bigram-2016.w2v", "bigram-2016.w2v.syn0.npy", "bigram-2016.w2v.syn1neg.npy"
        ]
        for f in files:
            if os.path.exists (f):
                continue
            logger.debug ("  --downloading word embedding model component: {0}".format (f))
            url = "{0}/{1}".format (self.url, f)
            r = requests.get (url, stream=True)
            with open (f, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024): 
                    if chunk: # filter out keep-alive new chunks
                        f.write (chunk)
        logger.debug ("All files present. Loading model. This will take a while.")
        model_path = files [0]

        start = time.time ()
        self.model = gensim.models.Word2Vec.load (model_path)
        logger.debug ("  -- loaded w2v term model: {0} in {1} seconds.".format (
            model_path, time.time () - start ))

        start = time.time ()
#       self.bigram_model = gensim.models.Word2Vec.load (model_path)
        logger.debug ("  -- loaded w2v bigram model: {0} in {1} seconds.".format (
            model_path, time.time () - start ))
        #with open ("a", "w") as stream:            
        #    stream.write (pformat (self.bigram_model.vocab))

    def get_semantic_similarity (self, term_a, term_b):
        """ Find semantic similarity of these terms as represented by a word2vec model generated from the 
        public access subset of PubMed Central full text journal articles. """
        term_a = term_a.lower ()
        term_b = term_b.lower ()
        result = None
        model = None
        if term_a.count(' ') == 1:
            raise ValueError ("We don't have a word embedding model for {0} word phrases".format (term_a.count(' ') + 1))
            model = self.bigram_model
            term_a = term_a.replace (' ', '_')
            term_b = term_b.replace (' ', '_')
        elif term_a.count (' ') == 0:
            model = self.model
        else:
            raise ValueError ("We don't have a word embedding model for {0} word phrases".format (term_a.count(' ') + 1))
            
        return model.similarity (term_a, term_b) if term_a in model.vocab and term_b in model.vocab else -1.0
            
        #return self.model.similarity (term_a, term_b) if term_a in self.model.vocab and term_b in self.model.vocab else -1.0


if __name__ == "__main__":
    ct2 = Chemotext2 (ServiceContext.create_context ())
    print (ct2.get_semantic_similarity ("lung cancer", "p53"))
    print (ct2.get_semantic_similarity ("cell line", "disease"))
    print (ct2.get_semantic_similarity ("cellular component", "nucleus"))
    print (ct2.get_semantic_similarity ("cell cycle", "krebbs"))

    print (ct2.get_semantic_similarity ("MAPK2", "P53"))

    w = [ "albuterol", "imatinib", "aspirin", "atrovent", "decadron", "medrol", "rayos" , "abemaciclib", "abraxane"]
    pairs = zip (w, w[1:])
    for k, v in pairs:
        print ((" k %s -> v %s : sim: %s" % (k, v, ct2.get_semantic_similarity (k, v))))
    print (ct2.model.most_similar (positive=['aspirin' ]))
    print (ct2.model.most_similar (positive=['p53' ]))
    print (ct2.model.most_similar (positive=['kit' ]))
    print (ct2.model.most_similar (positive=['asthma' ]))

    print (ct2.get_semantic_similarity('ebola', 'niemann'))
    print (ct2.get_semantic_similarity('Ebola', 'niemann'))
    print (ct2.get_semantic_similarity('ebola', 'Niemann'))
    print (ct2.get_semantic_similarity('ebola', 'niemann-pick'))
    print (ct2.get_semantic_similarity('niemann', 'pick'))
    print ('')
    print (ct2.get_semantic_similarity('Ebola Virus','niemann'))
    print (ct2.get_semantic_similarity('Ebola','niemann-pick disease'))
    print (ct2.get_semantic_similarity('Ebola virus','niemann-pick disease'))
    print ('')
    #This is pretty clear that the bigram is tough to use:
    print (ct2.get_semantic_similarity('Usher','WHRN'))           #gives 0.3
    print (ct2.get_semantic_similarity('Usher syndrome','WHRN'))  #gives -1
 

