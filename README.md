# GreenT

GreenT is a library of interfaces to biomedical and environmental data services.

Developed at the University of North Carolina at Chapel Hill, the API provides a Python package, Python interface, and a GraphQL service.

## Installation

```
(virtualenv)$ pip install greent
```

## Running the GraphQL Server:
```
(virtualenv)$ python -m greent.app
```

## Usage Example

```
from greent.core import GreenT
translator = GreenT ()
```

By default, the constructor above will use the public GraphQL API instance hosted at RENCI: 
```
https://stars-app.renci.org/greent/graphql
```

## Services

GreenT currently presents four main services. More information coming on these soon.

### ChemBio

This is a set of endpoints relating to the Chem2Bio2RDF data set.

### Clinical

A set of clinical derived data with no personally identifiable information.

### Environmental Exposures

Data derived from the CMAQ model.

### Chemotext

Mention data for terms in PubMed's Medline meta data.

