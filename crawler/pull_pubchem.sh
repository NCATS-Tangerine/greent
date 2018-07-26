#!/bin/bash
#mkdir -p pubchem
#cd pubchem
#wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/substance/pc_substance2compound*
#wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/substance/pc_substance2descriptor*
#wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/substance/pc_substance_match*
#wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/synonym/pc_synonym2compound*
#wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/synonym/pc_synonym_topic*
java -cp ../blazegraph.jar com.bigdata.rdf.store.DataLoader ../fastload.properties substance
#java -cp ../blazegraph.jar com.bigdata.rdf.store.DataLoader ../fastload.properties synonym
