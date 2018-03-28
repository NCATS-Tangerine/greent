import pytest
import json
import os
from greent.rosetta import Rosetta

@pytest.fixture
def rosetta (conf):
    """ Rosetta fixture """
    config = conf.get ("config", "greent.conf")
    print (f"CONFIG: *** > {config}")
    return Rosetta(debug=True, greentConf=config)

@pytest.fixture
def conf():
    conf = {}
    if os.path.exists ("pytest.conf"):
        with open ('pytest.conf', 'r') as stream:
            conf = json.loads (stream.read ())
    return conf    
