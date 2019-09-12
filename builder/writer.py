#!/usr/bin/env python

import os
import sys
from time import sleep, strftime
from datetime import datetime
import logging
import json
import pickle

import pika

from greent.util import LoggingUtil
from greent.export import BufferedWriter
from builder.buildmain import setup
from greent.graph_components import KNode, KEdge
from builder.api import logging_config

logger = LoggingUtil.init_logging("builder.writer", level=logging.DEBUG)

greent_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
sys.path.insert(0, greent_path)
rosetta = setup(os.path.join(greent_path, 'greent', 'greent.conf'))

connection = pika.BlockingConnection(pika.ConnectionParameters(
    host=os.environ['BROKER_HOST'],
    virtual_host='builder',
    credentials=pika.credentials.PlainCredentials(os.environ['BROKER_USER'], os.environ['BROKER_PASSWORD'])
))
channel = connection.channel()

channel.queue_declare(queue='neo4j')

writer = BufferedWriter(rosetta)

def callback(ch, method, properties, body):
    # logger.info(f" [x] Received {body}")
    graph = pickle.loads(body)
    if isinstance(graph, str) and graph == 'flush':
        logger.debug('Flushing buffer...')
        writer.flush()
        return
    for node in graph['nodes']:
        # logger.debug(f'Writing node {node.id}')
        writer.write_node(node)
    for edge in graph['edges']:
        # logger.debug(f'Writing edge {edge.source_id}->{edge.target_id}')
        if 'force' in graph:
            writer.write_edge(edge, force_create= True)
        else:
            writer.write_edge(edge)

channel.basic_consume('neo4j', callback, auto_ack=True)

logger.info(' [*] Waiting for messages.')
print('To exit press CTRL+C')
channel.start_consuming()
