import json
from greent.cachedservice import CachedService
from greent.graph_components import KNode, KEdge
from greent.util import LoggingUtil

logger = LoggingUtil.init_logging (__file__)

class Onto(CachedService):
    """ An abstraction for generic questions about ontologies. """
    def __init__(self, name, context):
        super(Onto,self).__init__(name, context)
    def is_a(self,identifier,candidate_ancestor):
        obj = self.get(f"{self.url}/is_a/{identifier}/{candidate_ancestor}")
        #print (f"obj: {json.dumps(obj, indent=2)}")
        return obj is not None and 'is_a' in obj and obj['is_a']
    def get_label(self,identifier):
        """ Get the label for an identifier. """
        obj = self.get(f"{self.url}/label/{identifier}")
        return obj['label'] if 'label' in obj else None
    def search(self,name,is_regex=False):
        """ Search ontologies for a term. """
        obj = self.get(f"{self.url}/search/{name}/{'true' if is_regex else 'false'}")
        return [ v['id'] for v in obj['values'] ] if obj and 'values' in obj else []
    def get_xrefs(self,identifier, filter=None):
        """ Get external references. Optionally filter results. """
        obj = self.get(f"{self.url}/xrefs/{identifier}")
        result = []
        if 'xrefs' in obj:
            for xref in obj['xrefs']:
                if filter:
                    for f in filter:
                        if xref.startswith(f):
                            result.append (xref)
                else:
                    result.append (xref)
        return result
    def get_synonyns(self,identifier,curie_pattern=None):
        return self.get(f"{self.url}/synonyms/{identifier}")
    def lookup(self,identifier):
        obj = self.get(f"{self.url}/lookup/{identifier}")
        return [ ref["id"] for ref in obj['refs'] ] if 'refs' in obj else []
