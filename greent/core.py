import json

from greent.ontologies.go import GO
from greent.ontologies.hpo import HPO
from greent.ontologies.mondo import Mondo
from greent.services.biolink import Biolink
from greent.services.chembio import ChemBioKS
from greent.services.chemotext import Chemotext
from greent.services.ctd import CTD
from greent.services.hetio import HetIO
from greent.services.hgnc import HGNC
from greent.services.oxo import OXO
from greent.services.pharos import Pharos
from greent.services.quickgo import QuickGo
from greent.services.tkba import TranslatorKnowledgeBeaconAggregator
from greent.services.uberongraph import UberonGraphKS
from greent.services.unichem import UniChem
from greent.service import ServiceContext
from greent.util import LoggingUtil

logger = LoggingUtil.init_logging (__file__)

class GreenT:

    ''' The Green Translator API - a single Python interface aggregating access mechanisms for 
    all Green Translator services. '''

    def __init__(self, config=None, override={}):
        self.service_context = ServiceContext.create_context (config)
        service_context = self.service_context
        self.translator_registry = None
        self.lazy_loader = {
            "chembio"          : lambda :  ChemBioKS (self.service_context),
            "chemotext"        : lambda :  Chemotext (self.service_context),
            "pharos"           : lambda :  Pharos (self.service_context),
            "oxo"              : lambda :  OXO (self.service_context),
            "hetio"            : lambda :  HetIO (self.service_context),
            "biolink"          : lambda :  Biolink (self.service_context),
            "mondo"            : lambda :  Mondo(self.service_context),
            "hpo"              : lambda :  HPO (self.service_context),
            "go"               : lambda :  GO(self.service_context),
            "tkba"             : lambda :  TranslatorKnowledgeBeaconAggregator (self.service_context),
            "quickgo"          : lambda :  QuickGo (self.service_context),
            "hgnc"             : lambda :  HGNC(self.service_context),
            "uberongraph"      : lambda :  UberonGraphKS(self.service_context),
            "ctd"              : lambda :  CTD(self.service_context),
            "unichem"          : lambda :  UniChem(self.service_context)
        }
        
    def get_config_val(self, key):
        print (f"{self.service_context.config}")
        return self.service_context.config.conf.get (key, None)
    def __getattribute__(self, attr):
        """ Intercept all attribute accesses. Instantiate services on demand. """
        value = None
        __dict__ = super(GreenT, self).__getattribute__('__dict__')
        if attr in __dict__:
            value = super(GreenT, self).__getattribute__(attr)
        else:
            if attr in self.lazy_loader:
                value = self.lazy_loader [attr] ()
                __dict__[attr] = value
        return value
