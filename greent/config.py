import os
import yaml
from greent.util import Resource

class Config:
    def __init__(self, config):
        '''
        if not config.startswith (os.sep):
            config = os.path.join (os.path.dirname (__file__), config)
        '''
        config_path = Resource.get_resource_path (config)
        with open(config_path, 'r') as f:
            self.conf = yaml.safe_load (f)
    def get_service (self, service):
        return self.conf['translator']['services'][service]
