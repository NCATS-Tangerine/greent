#!/usr/bin/env python

import os
import sys
from time import sleep, strftime
from datetime import datetime
import logging
import json

import pika

from greent.util import LoggingUtil
from greent.export import BufferedWriter
from builder.buildmain import setup
from greent.graph_components import KNode, KEdge

logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

greent_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
sys.path.insert(0, greent_path)
rosetta = setup(os.path.join(greent_path, 'greent', 'greent.conf'))

connection = pika.BlockingConnection(pika.ConnectionParameters(host=os.environ['BROKER_HOST'],
    virtual_host='builder',
    credentials=pika.credentials.PlainCredentials(os.environ['BROKER_USER'], os.environ['BROKER_PASSWORD'])))
channel = connection.channel()

channel.queue_declare(queue='neo4j')

def callback(ch, method, properties, body):
    body = body.decode()
    print(f" [x] Received {body}")
    if isinstance(body, str) and body == 'flush':
        with BufferedWriter(rosetta) as writer:
            writer.flush()
        return
    graph = json.loads(body)
    with BufferedWriter(rosetta) as writer:
        for node in graph['nodes']:
            writer.write_node(KNode(node))
        for edge in graph['edges']:
            writer.write_edge(KEdge(edge))

channel.basic_consume(callback,
                      queue='neo4j',
                      no_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
