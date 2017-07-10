from graphql.type.definition import GraphQLArgument, GraphQLField, GraphQLNonNull, GraphQLObjectType
from graphql.type.scalars import GraphQLString
from graphql.type.schema import GraphQLSchema

from core import GreenT

def resolve_raises(*_):
    raise Exception("Throws!")

greenT = GreenT ()

def get_exposure_scores (obj, args, context, info):
    return greenT.exposures.get_scores (
        exposure_type  = args.get ("exposureType"),
        start_date     = args.get ("startDate"),
        end_date       = args.get ("endDate"),
        exposure_point = args.get ("exposurePoint"))
    
def get_exposure_values (obj, args, context, info):
    return greenT.exposures.get_values (
        exposure_type  = args.get ("exposureType"),
        start_date     = args.get ("startDate"),
        end_date       = args.get ("endDate"),
        exposure_point = args.get ("exposurePoint"))

def get_patients (obj, args, context, info):
    return greenT.get_patients (age = args.get ("age"),
                                sex = args.get ("sex"),
                                race = args.get ("race"),
                                location = args.get ("location"))

QueryRootType = GraphQLObjectType(
    name='QueryRoot',
    fields={
        'thrower': GraphQLField(GraphQLNonNull(GraphQLString), resolver=resolve_raises),
        'request': GraphQLField(GraphQLNonNull(GraphQLString),
                                resolver=lambda obj, args, context, info: context.args.get('q')),
        'context': GraphQLField(GraphQLNonNull(GraphQLString),
                                resolver=lambda obj, args, context, info: context),
        'test': GraphQLField(
            type=GraphQLString,
            args={
                'who': GraphQLArgument(GraphQLString)
            },
            resolver=lambda obj, args, context, info: 'Hello %s' % (args.get('who') or 'World')
        ),
        
        'patients' : GraphQLField (
            type=GraphQLString,
            args = {
                'age' : GraphQLArgument (GraphQLString),
                'sex' : GraphQLArgument (GraphQLString),
                'race' : GraphQLArgument (GraphQLString),
                'location' : GraphQLArgument (GraphQLString)
            },
            resolver = get_patients
        ),

        'exposureScore' : GraphQLField (
            type=GraphQLString,
            args = {
                'type' : GraphQLArgument (GraphQLString)
            },
            resolver = get_exposure_scores
        ),
        'exposureValue' : GraphQLField (
            type=GraphQLString,
            args = {
                'type' : GraphQLArgument (GraphQLString)
            },
            resolver = get_exposure_values
        )


    }
)

Schema = GraphQLSchema (QueryRootType)
