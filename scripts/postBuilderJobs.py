import os
import requests
from collections import defaultdict
import json
import time

def get_question_ids():
    qids = []
    with open('qids','r') as inf:
        qids = [line.strip()[1:-1] for line in inf]
    return qids[4:] # already did 4....

def submit_next(qid):
    url = f'http://robokop.renci.org/api/q/{qid}'
    response=requests.get(url,auth=(os.environ['ADMIN_EMAIL'], os.environ['ADMIN_PASSWORD'])).json()
    machine_question = response['question']
    post_to_builder(machine_question)

def post_to_builder(machine_question):
    url = f'http://robokop.renci.org:6010/api/'
    response=requests.post(url,auth=(os.environ['ADMIN_EMAIL'], os.environ['ADMIN_PASSWORD']), json=machine_question).json()
    print(json.dumps(response,indent=4))

def fillit():
    question_ids = get_question_ids()
    for qid in question_ids[::-1]:
        submit_next(qid)

if __name__ == '__main__':
    fillit()





