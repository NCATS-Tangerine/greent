import os
import yaml
from greent.util import Resource

class Config:
    def __init__(self, config, prefix=''):
        '''
        if not config.startswith (os.sep):
            config = os.path.join (os.path.dirname (__file__), config)
        '''
        if isinstance(config, str):
            config_path = Resource.get_resource_path (config)
            with open(config_path, 'r') as f:
                self.conf = yaml.safe_load (f)
        elif isinstance(config, dict):
            self.conf = config
        else:
            raise ValueError
        self.prefix = prefix
    def get_service (self, service):
        return self.conf['translator']['services'][service]
    def get(self, *args):
        '''
        Use this accessor instead of getting conf directly in order to permit overloading with environment variables.
        Imagine you have a config file of the form

          person:
            address:
              street: Main

        This will be overridden by an environment variable by the name of PERSON_ADDRESS_STREET,
        e.g. export PERSON_ADDRESS_STREET=Gregson
        '''
        name = self.prefix+'_'+args[0] if self.prefix else args[0]
        try:
            env_name = name.upper()
            return os.environ[env_name]
        except KeyError:
            value = self.conf.get(*args)
            if isinstance(value, dict):
                return Config(value, prefix=name)
            else:
                return value