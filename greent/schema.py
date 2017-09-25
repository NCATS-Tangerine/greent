import graphene
from graphene import resolve_only_args
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
from .core import GreenT

# http://graphql.org/learn/introspection/

class ExposureInterface (graphene.Interface):
    start_time    = graphene.String ()
    end_time      = graphene.String ()
    exposure_type = graphene.String ()
    latitude      = graphene.String ()
    longitude     = graphene.String ()
    units         = graphene.String ()
    value         = graphene.String ()
    
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

class Thing(graphene.ObjectType):
    type  = graphene.String ()
    value = graphene.String ()
    
greenT = GreenT ({
    "clinical_url" : "http://localhost:5000/patients"
})

class GreenQuery (graphene.ObjectType):

    exposure_score = graphene.Field (type=ExposureScore,
                                    exposureType  = graphene.String (),
                                    startDate     = graphene.String (),
                                    endDate       = graphene.String (),
                                    exposurePoint = graphene.String ())
    
    exposure_value = graphene.Field (type=ExposureValue,
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

    translate = graphene.List (of_type = Thing,
                               thing   = graphene.String (),
                               domainA = graphene.String (),
                               domainB = graphene.String ())

    def resolve_translate (obj, args, context, info):
        return list (map (lambda v : Thing (type=args.get ("domainB"), value=v),
                          greenT.translate (thing   = args.get ("thing"),
                                            domainA = args.get ("domainA"),
                                            domainB = args.get ("domainB"))))
    
    def resolve_exposure_score (obj, args, context, info):
        result = None
        result = greenT.get_exposure_scores (
            exposure_type  = args.get ("exposureType"),
            start_date     = args.get ("startDate"),
            end_date       = args.get ("endDate"),
            exposure_point = args.get ("exposurePoint"))
        if result and len(result) == 1:
            result = json.loads (result)
            result = result[0]
            result = ExposureScore (exposure_type = result['exposure_type'],
                                    start_time    = result['start_time'],
                                    end_time      = result['end_time'],
                                    latitude      = result['latitude'],
                                    longitude     = result['longitude'],
                                    units         = result['units'],
                                    value         = result['value'])
        return result

    def resolve_exposure_value (obj, args, context, info):
        result = None
        result = greenT.get_exposure_values (
            exposure_type  = args.get ("exposureType"),
            start_date     = args.get ("startDate"),
            end_date       = args.get ("endDate"),
            exposure_point = args.get ("exposurePoint"))
        if result and len(result) == 1:
            result = json.loads (result)
            result = result[0]
            result = ExposureValue (exposure_type = result['exposure_type'],
                                    start_time    = result['start_time'],
                                    end_time      = result['end_time'],
                                    latitude      = result['latitude'],
                                    longitude     = result['longitude'],
                                    units         = result['units'],
                                    value         = result['value'])
        return result

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
    
Schema = graphene.Schema(query=GreenQuery)


'''

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
