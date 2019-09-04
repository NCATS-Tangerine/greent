from greent.ontologies.go2 import GO2
from greent.ontologies.hpo2 import HPO2
from greent.ontologies.mondo2 import Mondo2
from greent.services.biolink import Biolink
from greent.services.caster import Caster
from greent.services.chembio import ChemBioKS
from greent.services.chemotext import Chemotext
from greent.services.clingen import ClinGen
from greent.services.ctd import CTD
from greent.services.ensembl import Ensembl
from greent.services.gtopdb import gtopdb
from greent.services.gwascatalog import GWASCatalog
from greent.services.hetio import HetIO
from greent.services.hmdb_beacon import HMDB
from greent.services.hgnc import HGNC
from greent.services.kegg import KEGG
from greent.services.mychem import MyChem
from greent.services.myvariant import MyVariant
from greent.services.onto import Onto
#from greent.services.omnicorp import OmniCorp
#from greent.services.omnicorp_postgres import OmniCorp
from greent.services.oxo import OXO
#from greent.services.pharos import Pharos
from greent.services.pharos_mysql import PharosMySQL
from greent.services.quickgo import QuickGo
from greent.services.tkba import TranslatorKnowledgeBeaconAggregator
from greent.services.typecheck import TypeCheck
from greent.services.uberongraph import UberonGraphKS
from greent.services.unichem import UniChem
from greent.services.uniprot import UniProt
from greent.services.panther import Panther
#from greent.service import ServiceContext
from greent.util import LoggingUtil


logger = LoggingUtil.init_logging(__name__)

class GreenT:

    ''' The Green Translator API - a single Python interface aggregating access mechanisms for 
    all Green Translator services. '''
    #Getting rosetta in here is solely so that typecheck has access to the synonmizer - seems like
    # a crappy way to do this.   What's the right way?
    def __init__(self, context,rosetta):
        self.translator_registry = None
        #self.ont_api = context.config.conf.get("system",{}).get("generic_ontology_service", "false")
        self.ont_api = True
        self.service_context = context
        self.lazy_loader = {
            "biolink"          : lambda :  Biolink (self.service_context),
            "caster"           : lambda :  Caster(self.service_context, self),
            "chembio"          : lambda :  ChemBioKS (self.service_context),
            "chemotext"        : lambda :  Chemotext (self.service_context),
            "clingen"          : lambda :  ClinGen(self.service_context),
            "ctd"              : lambda :  CTD(self.service_context),
            "ensembl"          : lambda :  Ensembl(self.service_context),
            "go"               : lambda :  GO2(self.service_context),
            "gtopdb"           : lambda :  gtopdb(self.service_context),
            "gwascatalog"      : lambda :  GWASCatalog(self.service_context, rosetta),
            "hetio"            : lambda :  HetIO (self.service_context),
            "hgnc"             : lambda :  HGNC(self.service_context),
            "hmdb"             : lambda :  HMDB(self.service_context),
            "hpo"              : lambda :  HPO2 (self.service_context),
            "kegg"             : lambda :  KEGG (self.service_context),
            "mondo"            : lambda :  Mondo2(self.service_context),
            "mychem"           : lambda :  MyChem(self.service_context),
            "myvariant"        : lambda :  MyVariant(self.service_context, rosetta),
            #"omnicorp"         : lambda :  OmniCorp (self.service_context),
            "oxo"              : lambda :  OXO (self.service_context),
            "onto"             : lambda :  Onto ("onto", self.service_context),
            #"pharos"           : lambda :  Pharos (self.service_context),
            "pharos"           : lambda :  PharosMySQL (self.service_context),
            "quickgo"          : lambda :  QuickGo (self.service_context),
            "tkba"             : lambda :  TranslatorKnowledgeBeaconAggregator (self.service_context),
            "typecheck"        : lambda :  TypeCheck(self.service_context, self, rosetta),
            "uberongraph"      : lambda :  UberonGraphKS(self.service_context),
            "unichem"          : lambda :  UniChem(self.service_context),
            "uniprot"          : lambda :  UniProt(self.service_context),
            "panther"          : lambda :  Panther(self.service_context)
        }
        
    def get_config_val(self, key):
        print (f"{self.service_context.config}")
        return self.service_context.config.get (key, None)
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
