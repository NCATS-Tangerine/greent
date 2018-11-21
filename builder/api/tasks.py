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

from greent import node_types
from greent.util import LoggingUtil

from builder.buildmain import run_query, generate_query, run
from builder.pathlex import tokenize_path
from builder.buildmain import setup, build_spec


# set up Celery
celery = Celery(app.name)
celery.conf.update(
    broker_url=os.environ["CELERY_BROKER_URL"],
    result_backend=os.environ["CELERY_RESULT_BACKEND"],
    task_track_started=True,
)
celery.conf.task_queues = (
    Queue('update', routing_key='update'),
)
# Tell celery not to mess with logging at all
@signals.setup_logging.connect
def setup_celery_logging(**kwargs):
    pass
celery.log.setup()

#logger = logging.getLogger(__name__)
logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)

@celery.task(bind=True, queue='update')
def update_kg(self, question_json, task_acks_late=True, track_started=True, worker_prefetch_multiplier=1):
    '''
    Update the shared knowledge graph with respect to a question
    '''

    greent_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..')
    sys.path.insert(0, greent_path)
    logger.info("Setting up rosetta...")
    try:
        rosetta = setup(os.path.join(greent_path, 'greent', 'greent.conf'))
    except Exception as err:
        logger.exception(f"Could not update KG because could not setup rosetta: {err}")
        raise err

    self.update_state(state='UPDATING KG')
    logger.info("Updating the knowledge graph...")

    try:
        logger.debug(question_json)
        q = Question(question_json)
        logger.info("Program acquired...")
        programs = q.compile(rosetta)
        logger.info("Running Program...")
        for p in programs:
            p.run_program()

        logger.info("Done updating.")
        return "You updated the KG!"

    except Exception as err:
        logger.exception(f"Something went wrong with updating KG: {err}")
        raise err
