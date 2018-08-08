CREATE SCHEMA omnicorp;
CREATE TABLE omnicorp.PR ( pubmedid int, curie varchar(255) );
\copy omnicorp.PR FROM '../../omnicorp/PR' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.PR (pubmedid);
CREATE INDEX ON omnicorp.PR (curie);
CREATE TABLE omnicorp.T ( pubmedid int, curie varchar(255) );
\copy omnicorp.T FROM '../../omnicorp/T' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.T (pubmedid);
CREATE INDEX ON omnicorp.T (curie);
CREATE TABLE omnicorp.CHMO ( pubmedid int, curie varchar(255) );
\copy omnicorp.CHMO FROM '../../omnicorp/CHMO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.CHMO (pubmedid);
CREATE INDEX ON omnicorp.CHMO (curie);
CREATE TABLE omnicorp.CP ( pubmedid int, curie varchar(255) );
\copy omnicorp.CP FROM '../../omnicorp/CP' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.CP (pubmedid);
CREATE INDEX ON omnicorp.CP (curie);
CREATE TABLE omnicorp.OMIABIS ( pubmedid int, curie varchar(255) );
\copy omnicorp.OMIABIS FROM '../../omnicorp/OMIABIS' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.OMIABIS (pubmedid);
CREATE INDEX ON omnicorp.OMIABIS (curie);
CREATE TABLE omnicorp.MPATH ( pubmedid int, curie varchar(255) );
\copy omnicorp.MPATH FROM '../../omnicorp/MPATH' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.MPATH (pubmedid);
CREATE INDEX ON omnicorp.MPATH (curie);
CREATE TABLE omnicorp.VO ( pubmedid int, curie varchar(255) );
\copy omnicorp.VO FROM '../../omnicorp/VO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.VO (pubmedid);
CREATE INDEX ON omnicorp.VO (curie);
CREATE TABLE omnicorp.GAZ ( pubmedid int, curie varchar(255) );
\copy omnicorp.GAZ FROM '../../omnicorp/GAZ' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.GAZ (pubmedid);
CREATE INDEX ON omnicorp.GAZ (curie);
CREATE TABLE omnicorp.CL ( pubmedid int, curie varchar(255) );
\copy omnicorp.CL FROM '../../omnicorp/CL' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.CL (pubmedid);
CREATE INDEX ON omnicorp.CL (curie);
CREATE TABLE omnicorp.dcelements ( pubmedid int, curie varchar(255) );
\copy omnicorp.dcelements FROM '../../omnicorp/dcelements' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.dcelements (pubmedid);
CREATE INDEX ON omnicorp.dcelements (curie);
CREATE TABLE omnicorp.REO ( pubmedid int, curie varchar(255) );
\copy omnicorp.REO FROM '../../omnicorp/REO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.REO (pubmedid);
CREATE INDEX ON omnicorp.REO (curie);
CREATE TABLE omnicorp.GOCHEREL ( pubmedid int, curie varchar(255) );
\copy omnicorp.GOCHEREL FROM '../../omnicorp/GOCHEREL' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.GOCHEREL (pubmedid);
CREATE INDEX ON omnicorp.GOCHEREL (curie);
CREATE TABLE omnicorp.MF ( pubmedid int, curie varchar(255) );
\copy omnicorp.MF FROM '../../omnicorp/MF' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.MF (pubmedid);
CREATE INDEX ON omnicorp.MF (curie);
CREATE TABLE omnicorp.foaf ( pubmedid int, curie varchar(255) );
\copy omnicorp.foaf FROM '../../omnicorp/foaf' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.foaf (pubmedid);
CREATE INDEX ON omnicorp.foaf (curie);
CREATE TABLE omnicorp.FLOPO ( pubmedid int, curie varchar(255) );
\copy omnicorp.FLOPO FROM '../../omnicorp/FLOPO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.FLOPO (pubmedid);
CREATE INDEX ON omnicorp.FLOPO (curie);
CREATE TABLE omnicorp.NCIT ( pubmedid int, curie varchar(255) );
\copy omnicorp.NCIT FROM '../../omnicorp/NCIT' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.NCIT (pubmedid);
CREATE INDEX ON omnicorp.NCIT (curie);
CREATE TABLE omnicorp.CLO ( pubmedid int, curie varchar(255) );
\copy omnicorp.CLO FROM '../../omnicorp/CLO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.CLO (pubmedid);
CREATE INDEX ON omnicorp.CLO (curie);
CREATE TABLE omnicorp.MeSH ( pubmedid int, curie varchar(255) );
\copy omnicorp.MeSH FROM '../../omnicorp/MeSH' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.MeSH (pubmedid);
CREATE INDEX ON omnicorp.MeSH (curie);
CREATE TABLE omnicorp.CHEBI ( pubmedid int, curie varchar(255) );
\copy omnicorp.CHEBI FROM '../../omnicorp/CHEBI' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.CHEBI (pubmedid);
CREATE INDEX ON omnicorp.CHEBI (curie);
CREATE TABLE omnicorp.RO ( pubmedid int, curie varchar(255) );
\copy omnicorp.RO FROM '../../omnicorp/RO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.RO (pubmedid);
CREATE INDEX ON omnicorp.RO (curie);
CREATE TABLE omnicorp.UO ( pubmedid int, curie varchar(255) );
\copy omnicorp.UO FROM '../../omnicorp/UO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.UO (pubmedid);
CREATE INDEX ON omnicorp.UO (curie);
CREATE TABLE omnicorp.MOD ( pubmedid int, curie varchar(255) );
\copy omnicorp.MOD FROM '../../omnicorp/MOD' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.MOD (pubmedid);
CREATE INDEX ON omnicorp.MOD (curie);
CREATE TABLE omnicorp.CARO ( pubmedid int, curie varchar(255) );
\copy omnicorp.CARO FROM '../../omnicorp/CARO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.CARO (pubmedid);
CREATE INDEX ON omnicorp.CARO (curie);
CREATE TABLE omnicorp.HsapDv ( pubmedid int, curie varchar(255) );
\copy omnicorp.HsapDv FROM '../../omnicorp/HsapDv' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.HsapDv (pubmedid);
CREATE INDEX ON omnicorp.HsapDv (curie);
CREATE TABLE omnicorp.NBO ( pubmedid int, curie varchar(255) );
\copy omnicorp.NBO FROM '../../omnicorp/NBO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.NBO (pubmedid);
CREATE INDEX ON omnicorp.NBO (curie);
CREATE TABLE omnicorp.GO ( pubmedid int, curie varchar(255) );
\copy omnicorp.GO FROM '../../omnicorp/GO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.GO (pubmedid);
CREATE INDEX ON omnicorp.GO (curie);
CREATE TABLE omnicorp.PATO ( pubmedid int, curie varchar(255) );
\copy omnicorp.PATO FROM '../../omnicorp/PATO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.PATO (pubmedid);
CREATE INDEX ON omnicorp.PATO (curie);
CREATE TABLE omnicorp.GOCHE ( pubmedid int, curie varchar(255) );
\copy omnicorp.GOCHE FROM '../../omnicorp/GOCHE' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.GOCHE (pubmedid);
CREATE INDEX ON omnicorp.GOCHE (curie);
CREATE TABLE omnicorp.SO ( pubmedid int, curie varchar(255) );
\copy omnicorp.SO FROM '../../omnicorp/SO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.SO (pubmedid);
CREATE INDEX ON omnicorp.SO (curie);
CREATE TABLE omnicorp.HGNC ( pubmedid int, curie varchar(255) );
\copy omnicorp.HGNC FROM '../../omnicorp/HGNC' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.HGNC (pubmedid);
CREATE INDEX ON omnicorp.HGNC (curie);
CREATE TABLE omnicorp.OBA ( pubmedid int, curie varchar(255) );
\copy omnicorp.OBA FROM '../../omnicorp/OBA' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.OBA (pubmedid);
CREATE INDEX ON omnicorp.OBA (curie);
CREATE TABLE omnicorp.BFO ( pubmedid int, curie varchar(255) );
\copy omnicorp.BFO FROM '../../omnicorp/BFO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.BFO (pubmedid);
CREATE INDEX ON omnicorp.BFO (curie);
CREATE TABLE omnicorp.DOID ( pubmedid int, curie varchar(255) );
\copy omnicorp.DOID FROM '../../omnicorp/DOID' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.DOID (pubmedid);
CREATE INDEX ON omnicorp.DOID (curie);
CREATE TABLE omnicorp.PCO ( pubmedid int, curie varchar(255) );
\copy omnicorp.PCO FROM '../../omnicorp/PCO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.PCO (pubmedid);
CREATE INDEX ON omnicorp.PCO (curie);
CREATE TABLE omnicorp.GOREL ( pubmedid int, curie varchar(255) );
\copy omnicorp.GOREL FROM '../../omnicorp/GOREL' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.GOREL (pubmedid);
CREATE INDEX ON omnicorp.GOREL (curie);
CREATE TABLE omnicorp.OMP ( pubmedid int, curie varchar(255) );
\copy omnicorp.OMP FROM '../../omnicorp/OMP' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.OMP (pubmedid);
CREATE INDEX ON omnicorp.OMP (curie);
CREATE TABLE omnicorp.NCBITaxon ( pubmedid int, curie varchar(255) );
\copy omnicorp.NCBITaxon FROM '../../omnicorp/NCBITaxon' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.NCBITaxon (pubmedid);
CREATE INDEX ON omnicorp.NCBITaxon (curie);
CREATE TABLE omnicorp.BSPO ( pubmedid int, curie varchar(255) );
\copy omnicorp.BSPO FROM '../../omnicorp/BSPO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.BSPO (pubmedid);
CREATE INDEX ON omnicorp.BSPO (curie);
CREATE TABLE omnicorp.UBPROP ( pubmedid int, curie varchar(255) );
\copy omnicorp.UBPROP FROM '../../omnicorp/UBPROP' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.UBPROP (pubmedid);
CREATE INDEX ON omnicorp.UBPROP (curie);
CREATE TABLE omnicorp.HP ( pubmedid int, curie varchar(255) );
\copy omnicorp.HP FROM '../../omnicorp/HP' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.HP (pubmedid);
CREATE INDEX ON omnicorp.HP (curie);
CREATE TABLE omnicorp.dcterms ( pubmedid int, curie varchar(255) );
\copy omnicorp.dcterms FROM '../../omnicorp/dcterms' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.dcterms (pubmedid);
CREATE INDEX ON omnicorp.dcterms (curie);
CREATE TABLE omnicorp.UPHENO ( pubmedid int, curie varchar(255) );
\copy omnicorp.UPHENO FROM '../../omnicorp/UPHENO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.UPHENO (pubmedid);
CREATE INDEX ON omnicorp.UPHENO (curie);
CREATE TABLE omnicorp.IDO ( pubmedid int, curie varchar(255) );
\copy omnicorp.IDO FROM '../../omnicorp/IDO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.IDO (pubmedid);
CREATE INDEX ON omnicorp.IDO (curie);
CREATE TABLE omnicorp.FOODON ( pubmedid int, curie varchar(255) );
\copy omnicorp.FOODON FROM '../../omnicorp/FOODON' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.FOODON (pubmedid);
CREATE INDEX ON omnicorp.FOODON (curie);
CREATE TABLE omnicorp.OBI ( pubmedid int, curie varchar(255) );
\copy omnicorp.OBI FROM '../../omnicorp/OBI' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.OBI (pubmedid);
CREATE INDEX ON omnicorp.OBI (curie);
CREATE TABLE omnicorp.ENVO ( pubmedid int, curie varchar(255) );
\copy omnicorp.ENVO FROM '../../omnicorp/ENVO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.ENVO (pubmedid);
CREATE INDEX ON omnicorp.ENVO (curie);
CREATE TABLE omnicorp.OGMS ( pubmedid int, curie varchar(255) );
\copy omnicorp.OGMS FROM '../../omnicorp/OGMS' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.OGMS (pubmedid);
CREATE INDEX ON omnicorp.OGMS (curie);
CREATE TABLE omnicorp.PO ( pubmedid int, curie varchar(255) );
\copy omnicorp.PO FROM '../../omnicorp/PO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.PO (pubmedid);
CREATE INDEX ON omnicorp.PO (curie);
CREATE TABLE omnicorp.UBERON ( pubmedid int, curie varchar(255) );
\copy omnicorp.UBERON FROM '../../omnicorp/UBERON' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.UBERON (pubmedid);
CREATE INDEX ON omnicorp.UBERON (curie);
CREATE TABLE omnicorp.MONDO ( pubmedid int, curie varchar(255) );
\copy omnicorp.MONDO FROM '../../omnicorp/MONDO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.MONDO (pubmedid);
CREATE INDEX ON omnicorp.MONDO (curie);
CREATE TABLE omnicorp.IAO ( pubmedid int, curie varchar(255) );
\copy omnicorp.IAO FROM '../../omnicorp/IAO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.IAO (pubmedid);
CREATE INDEX ON omnicorp.IAO (curie);
CREATE TABLE omnicorp.FAO ( pubmedid int, curie varchar(255) );
\copy omnicorp.FAO FROM '../../omnicorp/FAO' DELIMITER E'\t' CSV
CREATE INDEX ON omnicorp.FAO (pubmedid);
CREATE INDEX ON omnicorp.FAO (curie);
