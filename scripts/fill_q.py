import os
import requests
from collections import defaultdict
import json
import time

def count_state():
    url = 'http://robokop.renci.org/api/tasks'
    response = requests.get(url).json()
    statecount = defaultdict(int)
    for k in response:
        statecount[ response[k]['state'] ] += 1
    return statecount

def get_question_ids():
    qids = []
    with open('qids','r') as inf:
        qids = [line.strip()[1:-1] for line in inf]
    return qids[4:] # already did 4....

def submit_next(qid):
    url = f'http://robokop.renci.org/api/q/{qid}/refresh_kg'
    response=requests.post(url,auth=(os.environ['ADMIN_EMAIL'], os.environ['ADMIN_PASSWORD'])).json()
    print(json.dumps(response,indent=4))

def fillit():
    question_ids = get_question_ids()
    while len(question_ids) > 0:
        sc = count_state()
        if sc['RECEIVED'] < 3:
            submit_next(question_ids.pop())
        time.sleep(60)

if __name__ == '__main__':
    fillit()
