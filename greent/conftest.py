import pytest
from greent.rosetta import Rosetta

@pytest.fixture
def rosetta ():
    "Rosetta fixture"
    rosetta = Rosetta(debug=True)
    return rosetta


