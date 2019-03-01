'''
Tasks for Celery workers
'''

import os
import sys
import logging
import redis
import json

from celery import Celery, signals
from celery.utils.log import get_task_logger
from kombu import Queue

# setup 'builder' logger first so that we know it's here when setting up children
from builder.api.logging_config import set_up_main_logger, add_task_id_based_handler, clear_log_handlers

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

redis_client = redis.Redis(
    host=os.environ['RESULTS_HOST'],
    port=os.environ['RESULTS_PORT'],
    db=os.environ['BUILDER_RESULTS_DB'],
    password=os.environ['RESULTS_PASSWORD'])

logger = logging.getLogger('builder')

@signals.after_task_publish.connect()
def initialize_queued_task_results(**kwargs):
    # headers=None, body=None, exchange=None, routing_key=None
    task_id = kwargs['headers']['id']
    logger.info(f'Queuing task: {task_id}')

    redis_key = 'celery-task-meta-'+task_id
    initial_status = {"status": "QUEUED",
        "result": None,
        "traceback": None,
        "children": [],
        "task_id": task_id
    }
    redis_client.set(redis_key, json.dumps(initial_status))

    # initial_status_again = redis_client.get(redis_key)
    # logger.info(f'Got initial status {initial_status_again}')


@signals.task_prerun.connect()
def setup_logging(signal=None, sender=None, task_id=None, task=None, *args, **kwargs):
    """
    Changes the main logger's handlers so they could log to a task specific log file.    
    """
    logger = logging.getLogger('greent')
    clear_log_handlers(logger)
    add_task_id_based_handler(logger, task_id)
    logger = logging.getLogger('builder')
    clear_log_handlers(logger)
    add_task_id_based_handler(logger, task_id)

@signals.task_postrun.connect()
def tear_down_task_logging(**kwargs):
    """
    Reverts back logging to main configuration once task is finished.
    """
    logger = logging.getLogger('greent')
    clear_log_handlers(logger)
    logger = logging.getLogger('builder')
    clear_log_handlers(logger)
    # change logging config back to the way it was
    set_up_main_logger()
    #finally log task has finished to main file
    logger = logging.getLogger(__name__)
    logger.info(f"task {kwargs.get('task_id')} finished ...")

@celery.task(bind=True, queue='update')
def update_kg(self, question_json, task_acks_late=True, track_started=True, worker_prefetch_multiplier=1):
    '''
    Update the shared knowledge graph with respect to a question
    '''
    logger = LoggingUtil.init_logging(__name__, level=logging.DEBUG)
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
