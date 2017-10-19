import json
import graphene
from graphene import resolve_only_args
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
from greent.core import GreenT
from greent.translator import Translation

# http://graphql.org/learn/introspection/

'''
class ExposureInterface (graphene.Interface):
    start_time    = graphene.String ()
    end_time      = graphene.String ()
    exposure_type = graphene.String ()
    latitude      = graphene.String ()
    longitude     = graphene.String ()
    units         = graphene.String ()
    value         = graphene.String ()
'''
class ExposureInterface (graphene.Interface):
    date_time  = graphene.String ()
    latitude   = graphene.String ()
    longitude  = graphene.String ()
    value      = graphene.String ()

class ExposureScore (graphene.ObjectType):
    class Meta:
        interfaces = (ExposureInterface, )

class ExposureValue (graphene.ObjectType):
    class Meta:
        interfaces = (ExposureInterface, )

class ExposureCondition (graphene.ObjectType):
    chemical = graphene.String ()
    gene     = graphene.String ()
    pathway  = graphene.String ()
    pathName = graphene.String ()
    pathID   = graphene.String ()
    human    = graphene.String ()

class Drug(graphene.ObjectType):
    generic_name = graphene.String ()
          
class GenePath(graphene.ObjectType):
    uniprot_gene = graphene.String ()
    kegg_path    = graphene.String ()
    path_name    = graphene.String ()
    human        = graphene.String ()

# No map type in GraphQL: https://github.com/facebook/graphql/issues/101

class PatientVisit(graphene.ObjectType):
    date = graphene.String ()
    visit_type = graphene.String ()

class Location(graphene.ObjectType):
    latitude = graphene.String ()
    longitude = graphene.String ()

class Prescription(graphene.ObjectType):
    medication = graphene.String ()
    date       = graphene.String ()

class Diagnosis(graphene.ObjectType):
    diagnosis  = graphene.String ()
    visit      = graphene.Field (PatientVisit)
    
class Patient(graphene.ObjectType):
    birth_date    = graphene.String ()
    race          = graphene.String ()
    sex           = graphene.String ()
    patient_id    = graphene.String ()
    geo_code      = graphene.Field (Location)
    prescriptions = graphene.List (Prescription)
    diagnoses     = graphene.List (Diagnosis)

class DrugToDisease (graphene.ObjectType):
    drug_name = graphene.String ()
    target_name = graphene.String ()
    disease_name = graphene.String ()

class Attribute(graphene.ObjectType):
    key = graphene.String ()
    value = graphene.String ()
    
class Thing(graphene.ObjectType):
    value      = graphene.String ()
    attributes = graphene.List (of_type=Attribute)
    
greenT = GreenT (config='greent.conf', override={
    "clinical_url" : "http://localhost:5000/patients"
})

class GreenQuery (graphene.ObjectType):

    endotype = graphene.List(of_type=graphene.String,
                             query=graphene.String ())

    exposure_score = graphene.List (of_type=ExposureScore,
                                    exposureType  = graphene.String (),
                                    startDate     = graphene.String (),
                                    endDate       = graphene.String (),
                                    exposurePoint = graphene.String ())
    
    exposure_value = graphene.List (of_type=ExposureValue,
                                    exposureType  = graphene.String (),
                                    startDate     = graphene.String (),
                                    endDate       = graphene.String (),
                                    exposurePoint = graphene.String ())

    patients = graphene.List (of_type=Patient,
                              age=graphene.Int (),
                              race=graphene.String (),
                              sex=graphene.String ())
    
    exposure_conditions = graphene.List (of_type=ExposureCondition,
                                         chemicals = graphene.List(graphene.String))
    
    drugs_by_condition = graphene.List(of_type=Drug,
                                       conditions = graphene.List(graphene.String))
    
    gene_paths_by_disease = graphene.List (of_type=GenePath,
                                           diseases = graphene.List(graphene.String))

    drug_gene_disease = graphene.List (of_type=DrugToDisease,
                                       drug_name = graphene.String (),
                                       disease_name = graphene.String ())
    
    translate = graphene.List (of_type = Thing,
                               thing   = graphene.String (),
                               domainA = graphene.String (),
                               domainB = graphene.String ())

    '''
class Attribute(graphene.ObjectType):
    key = graphene.String ()
    value = graphene.String ()
    
class Thing(graphene.ObjectType):
    value      = graphene.String ()
    attributes = graphene.List (of_type=Attribute)

    '''
    def resolve_endotype (obj, args, context, info):
        return greenT.endotype.get_endotype (json.loads (args.get("query")))
    
    def resolve_translate (obj, args, context, info):
        translation = Translation (obj=args.get("thing"),
                                   type_a=args.get("domainA"),
                                   type_b=args.get("domainB"))
        return list (map (lambda v : Thing (value=v), greenT.translator.translate (translation)))
    
    def resolve_exposure_score (obj, args, context, info):
        result = None
        result = greenT.get_exposure_scores (
            exposure_type  = args.get ("exposureType"),
            start_date     = args.get ("startDate"),
            end_date       = args.get ("endDate"),
            exposure_point = args.get ("exposurePoint"))
        out = []
        for r in result['scores']:
            latitude, longitude = r['latLon'].split (",")
            out.append (ExposureValue (date_time  = datetime.strftime (r['dateTime'], "%Y-%m-%d"),
                                       latitude   = latitude,
                                       longitude  = longitude,
                                       value      = r['score']))
        return out

    def resolve_exposure_value (obj, args, context, info):
        result = None
        result = greenT.get_exposure_values (
            exposure_type  = args.get ("exposureType"),
            start_date     = args.get ("startDate"),
            end_date       = args.get ("endDate"),
            exposure_point = args.get ("exposurePoint"))
        out = []
        for r in result['values']:
            latitude, longitude = r['latLon'].split (",")
            out.append (ExposureValue (date_time  = datetime.strftime (r['dateTime'], "%Y-%m-%d"),
                                       latitude   = latitude,
                                       longitude  = longitude,
                                       value      = r['value']))
        return out

    def resolve_patients (obj, args, context, info):
        result = None
        
        result = greenT.get_patients (
            age = args.get ("age"),
            sex = args.get ("sex"),
            race = args.get ("race"),
            location = args.get ("location"))

        out = []
        for r in result:
            diagnoses = []
            for key, value in r['diag'].items ():
                visit_date = list (value)[0]
                visit_type = value[visit_date]
                diagnosis = list (list (value)[0])[1]
                diagnoses.append (Diagnosis (
                    diagnosis = key,
                    visit = PatientVisit (date = visit_date,
                                          visit_type = visit_type)))

            prescriptions = []
            for key, value in r['medList'].items ():
                prescriptions.append (Prescription (
                    medication = key,
                    date       = value))
                
            out.append (Patient (
                birth_date    = r['birth_date'],
                race          = r['race'],
                sex           = r['sex'],
                patient_id    = r['patient_id'],
                geo_code      = Location (r['geoCode']['GEO:LAT'], r['geoCode']['GEO:LONG']),
                prescriptions = prescriptions,
                diagnoses     = diagnoses))

        return out

    def resolve_exposure_conditions (obj, args, context, info):
        obj = args.get ("chemicals")
        result = greenT.get_exposure_conditions (chemicals = obj)
        if result:
            out = []
            for r in result:
                out.append (ExposureCondition (
                    chemical = r["chemical"],
                    gene     = r["gene"],
                    pathway  = r["pathway"],
                    pathName = r["pathName"],
                    pathID   = r["pathID"],
                    human    = r["human"] ))
                result = out
        return result

    def resolve_drugs_by_condition (obj, args, context, info):
        conditions = args.get ("conditions")
        diseases = greenT.get_drugs_by_condition (conditions = conditions)
        return list(map(lambda s : Drug(s), diseases))

    def resolve_gene_paths_by_disease (obj, args, context, info):
        diseases = args.get ("diseases")
        gene_paths = greenT.get_genes_pathways_by_disease (diseases = diseases)
        return list(map(lambda g : GenePath (
            uniprot_gene = g['uniprotGene'],
            kegg_path    = g['keggPath'],
            path_name    = g['pathName'],
            human        = g['human']), gene_paths))
    def resolve_drug_gene_disease (obj, args, context, info):
        drug_name = args.get ("drug_name")
        disease_name = args.get ("disease_name")
        paths = greenT.get_drug_gene_disease (disease_name=disease_name, drug_name=drug_name)
        return list(map(lambda dd : DrugToDisease (
            drug_name = drug_name,
            target_name = dd['uniprotSymbol'],
            disease_name = disease_name), paths))

Schema = graphene.Schema(query=GreenQuery)


'''
{
  translate (thing:"Imatinib",
    domainA: "http://chem2bio2rdf.org/drugbank/resource/Generic_Name",
  	domainB: "http://chem2bio2rdf.org/uniprot/resource/gene")
  {
    type
    value
  }
}

{
  translate (thing:"DOID:2841",
    domainA: "http://identifiers.org/doid/",
  	domainB: "http://identifiers.org/mesh/disease/id")
  {
    type
    value
  }
}


{
  translate (thing:"Asthma",
					  domainA :	"http://identifiers.org/mesh/disease/name/",
						domainB : "http://identifiers.org/mesh/drug/name/")
  {
  	type
    value
  }
}


{
  exposureValue(exposureType: "pm25", 
    		startDate: "2010-01-06", 
    		endDate: "2010-01-06", 
    	        exposurePoint: "35.9131996,-79.0558445") {
    value
  }
}

{
  exposureScore(exposureType: "pm25", 
    		startDate: "2010-01-06", 
    		endDate: "2010-01-06", 
    		exposurePoint: "35.9131996,-79.0558445") {
    value
  }
}

{
  exposureConditions (chemicals: [ "D052638" ] ) {
    chemical
    gene
    pathway
    pathName
    pathID
    human
  } 
}

{
  drugsByCondition (conditions: [ "d001249" ] ) {
    genericName
  } 
}

{
  genePathsByDisease (diseases: [ "d001249" ] ) {
    uniprotGene
    keggPath
    pathName
  } 
}

{
  patients {
    birthDate
    patientId
    geoCode {
      latitude
      longitude
    }
    prescriptions {
      date
      medication
    }
    diagnoses {
      diagnosis
      visit {
        date
        visitType
      }
    }
  }
}


{
  __type(name: "Patient") {
    name
    fields {
      name
      type {
        name
        kind
      }
    }
  }
}

'''
