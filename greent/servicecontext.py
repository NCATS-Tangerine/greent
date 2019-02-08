import os
from greent.cache import Cache
from greent.core import GreenT
from greent.config import Config
from greent.util import LoggingUtil
import socket

class ServiceContext:
    """ A context for all service objects. Centralizes control over how services behave
    and a common point of configuration. """
    def __init__(self, rosetta, config=None):
        if config is None:
            config_name = "greent.conf"
            config = os.path.join (os.path.dirname (__file__), config_name)
        self.config = Config (config)
        self.core = GreenT (self,rosetta)
        
        # Initiaize the cache.
        redis_conf = self.config["cache"]
        self.cache = Cache (
            redis_host = redis_conf.get ("host"),
            redis_port = redis_conf.get ("port"),
            redis_db = redis_conf.get ("db"),
            redis_password = redis_conf.get ("password"))
        #redis_conf = self.config["redis"]
        #self.cache = Cache (
        #    redis_host = self.config.get ("RESULTS_HOST"),
        #    redis_port = self.config.get ("RESULTS_PORT"),
        #    redis_db = self.config.get ("CACHE_DB"))
 
        
    @staticmethod
    def create_context (config=None):
        return ServiceContext (config)
    
