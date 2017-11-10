
from bravado.client import SwaggerClient
from greent.graph_components import KNode,KEdge,elements_to_json
from bravado.requests_client import RequestsClient
from greent.service import Service

class SwaggerEndpoint(Service):

    def __init__(self, name, context): #swagger_endpoint_url):
        super(SwaggerEndpoint, self).__init__(name, context)

        http_client = RequestsClient ()
        self.client = SwaggerClient.from_url(
            self.url, #swagger_endpoint_url,
            http_client=http_client,
            config={
                'use_models': False
            })

    def inspect (self):
        for name, obj in inspect.getmembers (self.client):
            print ("-- name: {0} obj: {1}".format (name, obj))
            for n, o in inspect.getmembers (obj):
                if (n.endswith ('_get') or n.endswith ('_post')):
                    print ("-- INVOKE: method-> {0} obj: {1}".format (n, o))

                    
