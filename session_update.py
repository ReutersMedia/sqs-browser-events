import json
import boto3
import botocore.exceptions
import threading

from multiprocessing.pool import ThreadPool
from boto3.dynamodb.types import TypeDeserializer

import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

_thread_local = threading.local()

def get_session():
    if not hasattr(_thread_local,'boto_session'):
        _thread_local.boto_session = boto3.session.Session()
    return _thread_local.boto_session


def clean_entry(e):
    if e is None:
        return e
    ds = TypeDeserializer()
    return dict([(k,ds.deserialize(v)) for k,v in e.iteritems()])

def get_queue(img):
    if img == None:
        return None
    ce = clean_entry(img)
    return ce.get('sqsUrl')

def proc(ev):
    r = ev['dynamodb']
    k = r['Keys']
    old_q = get_queue(r.get('OldImage'))
    if old_q is None:
        return False

    new_q = get_queue(r.get('NewImage'))
    if old_q == new_q:
        return False
    
    try:
        # remove the old queue
        c = get_session().client('sqs')
        c.delete_queue(QueueUrl=old_q)
        LOGGER.info("Removed queue {0}".format(old_q))
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            LOGGER.info("Attempted to remove queue, does not exist: '{0}'".format(old_q))
        else:
            LOGGER.exception("Error removing queue {0}".format(old_q))
    except:
        LOGGER.exception("Unable to remove queue {0}".format(old_q))
    return False
    
def k_seq(x):
    return x['kinesis'].get('sequenceNumber',0)

def lambda_handler(event, context):
    LOGGER.debug("Received event: " + json.dumps(event, indent=2))
    n_fail = 0
    n_succ = 0
    recs = event['Records']
    try:
        recs.sort(cmp=lambda x,y: cmp(k_seq(x),k_seq(y)))
    except:
        LOGGER.exception("Unable to sort event list, likely unexpected type")
    tp = ThreadPool(min(len(recs),20))
    try:
        rc = tp.map_async(proc,recs)
        rc.wait(max(context.get_remaining_time_in_millis()/1000.0 - 2.0,0))
        if not rc.ready():
            LOGGER.error("Timeout waiting on processors")
            tp.terminate()
        else:
            n_del = len([x for x in rc.get() if x])
            LOGGER.info("Processed {0} records, {1} queues deleted".format(len(recs),n_del))
    finally:
        tp.close()
