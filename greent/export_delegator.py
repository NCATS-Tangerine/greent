
import os
import pickle
import pika
import logging
import requests
from greent.util import LoggingUtil
from greent.export import BufferedWriter
from greent.annotators.annotator_factory import annotate_shortcut
import traceback

logger = LoggingUtil.init_logging("builder.writer_delegate", level=logging.DEBUG, logFilePath=f'{os.environ["ROBOKOP_HOME"]}/logs/')

class WriterDelegator:
    def __init__(self, rosetta):
        self.rosetta = rosetta
        self.synonymizer = rosetta.synonymizer
        response = requests.get(f"{os.environ['BROKER_API']}queues/")
        queues = response.json()
        num_consumers = [q['consumers'] for q in queues if q['name'] == 'neo4j']
        if num_consumers and num_consumers[0]:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                heartbeat=0,
                host=os.environ['BROKER_HOST'],
                virtual_host='builder',
                credentials=pika.credentials.PlainCredentials(os.environ['BROKER_USER'], os.environ['BROKER_PASSWORD'])))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue='neo4j')
        else:
            self.connection = None
            self.channel = None
        
        self.buffered_writer = BufferedWriter(rosetta)

    def __enter__(self):
        return self

    def __del__(self):
        if self.connection is not None:
            self.connection.close()

    def __exit__(self,*args):
        self.flush()

    def write_node(self, node, synonymize=False):
        if synonymize:
            self.synonymizer.synonymize(node)
        try:
            result = annotate_shortcut(node, self.rosetta)
            #if type(result) == type(None):
            #    logger.debug(f'No annotator found for {node}')
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
        if self.channel is not None:
            self.channel.basic_publish(
                exchange='',
                routing_key='neo4j',
                body=pickle.dumps({'nodes': [node], 'edges': []}))
        else:
            self.buffered_writer.write_node(node)
            
        def write_edge(self, edge, force_create=False):
        if self.channel is not None:
            write_message = {'nodes': [], 'edges': [edge]}
            if force_create:
                write_message['force'] = True
            self.channel.basic_publish(
                exchange='',
                routing_key='neo4j',
                body=pickle.dumps(write_message))
        else:
            self.buffered_writer.write_edge(edge, force_create)

    def flush(self):
        if self.connection and self.connection.is_open:
            if self.channel is not None:
                self.channel.basic_publish(
                    exchange='',
                    routing_key='neo4j',
                    body=pickle.dumps('flush'))
        else:
            self.buffered_writer.flush()
             