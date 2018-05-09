'''
Tasks for Celery workers
'''

import os
import sys
import logging

from celery import Celery, signals
from celery.utils.log import get_task_logger
from kombu import Queue

# setup 'builder' logger first so that we know it's here when setting up children
import builder.api.logging_config

from builder.api.setup import app
from builder.question import Question

greent_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..')
sys.path.insert(0, greent_path)
from greent import node_types

from builder.buildmain import run_query, generate_query
from builder.pathlex import tokenize_path
from builder.buildmain import setup

rosetta = setup(os.path.join(greent_path, 'greent', 'greent.conf'))

# set up Celery
app.config['broker_url'] = os.environ["CELERY_BROKER_URL"]
app.config['result_backend'] = os.environ["CELERY_RESULT_BACKEND"]
celery = Celery(app.name, broker=app.config['broker_url'])
celery.conf.update(app.config)
celery.conf.task_queues = (
    Queue('update', routing_key='update'),
)
# Tell celery not to mess with logging at all
@signals.setup_logging.connect
def setup_celery_logging(**kwargs):
    pass
celery.log.setup()

@celery.task(bind=True, queue='update')
def update_kg(self, question_json):
    '''
    Update the shared knowledge graph with respect to a question
    '''
    # logger = get_task_logger(__name__)

    self.update_state(state='UPDATING KG')
    logger = logging.getLogger(__name__)
    logger.info("Updating the knowledge graph...")

    try:
        question = Question(question_json)
        symbol_lookup = {node_types.type_codes[a]:a for a in node_types.type_codes} # invert this dict
        # assume the nodes are in order
        node_string = ''.join([symbol_lookup[n.type] for n in question.nodes])
        start_identifiers = question.nodes[0].identifiers
        end_identifiers = question.nodes[-1].identifiers

        steps = tokenize_path(node_string)
        query = generate_query(steps, start_identifiers, end_identifiers)
        run_query(query, supports=[], rosetta=rosetta, prune=False)

        logger.info("Done updating.")
        return "You updated the KG!"

    except Exception as err:
        logger.exception(f"Something went wrong with updating KG: {err}")
        raise err
