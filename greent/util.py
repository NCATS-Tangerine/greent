import logging
import inspect
import json
import requests
import traceback
import unittest
import datetime
import yaml
from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient

class Config:
    def __init__(self, config):
        with open(config) as f:
            self.conf = yaml.safe_load(f)
    def get_service (self, service):
        return self.conf['translator']['services'][service]
    
class LoggingUtil(object):
    """ Logging utility controlling format and setting initial logging level """
    @staticmethod
    def init_logging (name):
        FORMAT = '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
        logging.basicConfig (format=FORMAT, level=logging.INFO) #DEBUG)
        return logging.getLogger (name)

class Munge(object):
    @staticmethod
    def gene (gene):
        return gene.split ("/")[-1:][0] if gene.startswith ("http://") else gene
    
class SwaggerEndpoint(object):

    def __init__(self, swagger_endpoint_url):
        http_client = RequestsClient ()
        # TODO: fix ssl certificate.
        self.client = SwaggerClient.from_url(
            swagger_endpoint_url,
            http_client=http_client,
            config={
            #    "validate_requests" : False,
            #    "validate_responses" : False
                'use_models': False
            })

    def inspect (self):
        for name, obj in inspect.getmembers (self.client):
            print ("-- name: {0} obj: {1}".format (name, obj))
            for n, o in inspect.getmembers (obj):
                if (n.endswith ('_get') or n.endswith ('_post')):
                    print ("-- INVOKE: method-> {0} obj: {1}".format (n, o))
