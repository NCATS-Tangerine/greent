import logging
import inspect
import json
import requests
import traceback
import unittest
import datetime
import os
import yaml
from collections import namedtuple
from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient

#loggers = {}
class LoggingUtil(object):
    """ Logging utility controlling format and setting initial logging level """
    @staticmethod
    def init_logging (name, level=logging.INFO, format='short'):
        FORMAT = {
            "short" : '%(funcName)s: %(message)s',
            "long"  : '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
        }[format]
        handler = logging.StreamHandler()
        formatter = logging.Formatter(FORMAT)
        handler.setFormatter(formatter)
        logger = logging.getLogger (name)
        logger.addHandler(handler)
        logger.setLevel(level)
        return logger
    
        '''
        logger = None
        global loggers
        if name in loggers:
            logger = loggers[name]
        else:
            loggers[name] = logger
        return logger
        '''
        
class Munge(object):
    @staticmethod
    def gene (gene):
        return gene.split ("/")[-1:][0] if gene.startswith ("http://") else gene
    
class Text:
    """ Utilities for processing text. """

    @staticmethod
    def get_curie (text):
        return text.upper ().split (':')[0] if ':' in text else None
        
    @staticmethod
    def un_curie (text):
        return text.split (':')[1] if ':' in text else text
        
    @staticmethod
    def short (obj, limit=80):
        text = str(obj) if obj else None
        return (text[:min(len(text),limit)] + ('...' if len(text)>limit else '')) if text else None

    @staticmethod
    def path_last (text):
        return text.split ('/')[-1:][0] if '/' in text else text

    @staticmethod
    def obo_to_curie (text):
        return ':'.join( text.split('/')[-1].split('_') )

class Resource:
    @staticmethod
    def get_resource_path(resource_name):
        """ Given a string resolve it to a module relative file path unless it is already an absolute path. """
        resource_path = resource_name
        if not resource_path.startswith (os.sep):
            resource_path = os.path.join (os.path.dirname (__file__), resource_path)
        return resource_path
    @staticmethod
    def load_json (path):
        result = None
        with open (path, 'r') as stream:
            result = json.loads (stream.read ())
        return result

    @staticmethod
    def load_yaml (path):
        result = None
        with open (path, 'r') as stream:
            result = yaml.load (stream.read ())
        return result
    
    def get_resource_obj (resource_name, format='json'):
        result = None
        path = Resource.get_resource_path (resource_name)
        if os.path.exists (path):
            m = {
                'json' : Resource.load_json,
                'yaml' : Resource.load_yaml
            }
            if format in m:
                result = m[format](path)
        return result
    
class DataStructure:
    @staticmethod
    def to_named_tuple (type_name, d):
        return namedtuple(type_name, d.keys())(**d)
