import requests
from greent.service import Service
import time


class UniProt(Service):

    """ Generic GENE id translation service. Essentially a highly generic synonym finder. """
    def __init__(self, context): 
        super(UniProt, self).__init__("uniprot", context)

    #TODO: share the retry logic in Service?
    def query(self,url,data):
        """if the prefix is malformed, then you get a 400.  If the prefix is ok, but there is no data, you get
        a valid json response with no entries.  So failures here are most likely timeouts and stuff like that."""
        done = False
        num_tries = 0
        max_tries = 10
        wait_time = 5 # seconds
        while num_tries < max_tries:
            try:
                return requests.post(url , data =data)
            except:
                num_tries += 1
                time.sleep(wait_time)
        return None

    def uniprot_2_hgnc(self, identifier):
        """Some services, like quickgo, return uniprot identifiers that other services have a hard time
        interpreting.  Things like UniProtKB:A0A0A0MR54
        But UniProt knows what they are and can convert them into something else."""
        identifier_parts = identifier.split(':')
        upkb = identifier_parts[1]
        data = {'from'  : 'ACC+ID',
                'to'    : 'HGNC_ID',
                'format': 'tab',
                'query' : upkb }
        r = self.query(self.url, data=data)
        lines = r.text.split("\n")
        answerlines = list(filter( lambda x: x.startswith(upkb), lines))
        answerparts = [ x.strip().split()[-1] for x in answerlines ]
        return answerparts

