import json
import logging
import operator
import os
import pickle
import requests
import redis
import traceback
from greent.util import LoggingUtil
from lru import LRU

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

class CacheSerializer:
    """ Generic serializer. """
    def __init__(self):
        pass

class PickleCacheSerializer(CacheSerializer):
    """ Use Python's default serialization. """
    def __init__(self):
        pass
    def dumps(self, obj):
        return pickle.dumps (obj)
    def loads(self, str):
        return pickle.loads (str)

class JSONCacheSerializer(CacheSerializer):
    pass # would be nice

class Cache:
    """ Cache objects by configurable means. """
    def __init__(self, cache_path="cache",
                 serializer=PickleCacheSerializer,
                 redis_host="localhost", redis_port=6379, redis_db=0, redis_password="",
                 enabled=True, prefix=''):
        
        """ Connect to cache. """
        self.enabled = enabled
        self.prefix = prefix
        try:
            if redis_password:
                self.redis = redis.StrictRedis(host=redis_host, port=int(redis_port), db=int(redis_db), password=redis_password)
            else:
                self.redis = redis.StrictRedis(host=redis_host, port=int(redis_port), db=int(redis_db))
            self.redis.get ('x')
            logger.info(f"Cache connected to redis at {redis_host}:{redis_port}/{redis_db}")
        except Exception as e:
            self.redis = None
            #logger.debug (traceback.format_exc ())
            logger.error(e)
            logger.error(f"Failed to connect to redis at {redis_host}:{redis_port}/{redis_db}.")
        self.cache_path = cache_path
        if not os.path.exists (self.cache_path):
            os.makedirs (self.cache_path)
        self.cache = LRU (1000) 
        self.serializer = serializer ()
        
    def get(self, key):
        """ Get a cached item by key. """
        #if any(map(lambda v : v in key.lower(), [ "go:", "mondo:", "hp:" ])):
        #    return None
        key = self.prefix + key
        result = None
        if self.enabled:
            if key in self.cache:
                result = self.cache[key]
            elif self.redis:
                rec = self.redis.get (key)
                result = self.serializer.loads (rec) if rec is not None else None
                self.cache[key] = result
            else:
                path = os.path.join (self.cache_path, key)
                if os.path.exists (path):
                    with open(path, 'rb') as stream:
                        result = self.serializer.loads (stream.read ())
                        self.cache[key] = result
        return result
    
    def set(self, key, value):
        """ Add an item to the cache. """
        key = self.prefix + key
        if self.enabled:
            if self.redis:
                if value is not None:
                    self.redis.set (key, self.serializer.dumps (value))
                    self.cache[key] = value
            else:
                path = os.path.join (self.cache_path, key)
                with open(path, 'wb') as stream:
                    stream.write (self.serializer.dumps (value))
                self.cache[key] = value

    def flush(self):
        if self.prefix:
            keys = self.redis.keys(f'{self.prefix}*')
            if keys:
                self.redis.delete(*keys)
        else:
            self.redis.flushdb()
