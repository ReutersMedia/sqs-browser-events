import os
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
        return None
    ds = TypeDeserializer()
    return dict([(k,ds.deserialize(v)) for k,v in e.iteritems()])

def check_resource_removed(old_s,new_s,field):
    if old_s is None:
        return None
    old_v = old_s.get(field)
    if new_s is not None:
        new_v = new_s.get(field)
    else:
        new_v = None
    if old_v is not None and old_v != new_v:
        return old_v
    return None

def check_remove_queue(r):
    old_s,new_s = r
    old_q = check_resource_removed(old_s,new_s,'sqsUrl')
    if old_q is not None:
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

def check_remove_identities(rlist):
    ids_to_remove = [x for x in [ check_resource_removed(old_s,new_s,'identityId') for old_s,new_s in rlist ] if x is not None ]
    if len(ids_to_remove)==0:
        return 0
    cog_region = os.getenv('COGNITO_REGION')
    if cog_region is not None and len(cog_region)>0:
        cog_c = get_session().client('cognito-identity',region_name=cog_region)
    else:
        cog_c = get_session().client('cognito-identity')
    n_removed = 0
    for id_batch in [ids_to_remove[i:i+60] for i in xrange(0,len(ids_to_remove),60)]:
        try:
            cog_c.delete_identities(IdentityIdsToDelete=id_batch)
            LOGGER.info("Deleting Cognito identities: {0!r}".format(id_batch))
            n_removed += len(id_batch)
        except:
            LOGGER.exception("Unable to remove cognito identities: {0!r}".format(id_batch))
    return n_removed

def event_to_dynamo_images(ev):
    r = ev['dynamodb']
    old_s = clean_entry(r.get('OldImage'))
    new_s = clean_entry(r.get('NewImage'))
    return (old_s, new_s)

def lambda_handler(event, context):
    LOGGER.debug("Received event: " + json.dumps(event, indent=2))
    n_fail = 0
    n_succ = 0
    recs = event['Records']
    tp = ThreadPool(min(len(recs),20))
    try:
        recs = [event_to_dynamo_images(x) for x in recs]
        rc = tp.map_async(check_remove_queue,recs)
        # identities can be bulk-deleted in groups of 60 via AWS API, so handle in this thread
        check_remove_identities(recs)
        rc.wait(max(context.get_remaining_time_in_millis()/1000.0 - 2.0,0))
        if not rc.ready():
            LOGGER.error("Timeout waiting on processors")
            tp.terminate()
        else:
            n_del = len([x for x in rc.get() if x])
            LOGGER.info("Processed {0} records, {1} queues deleted".format(len(recs),n_del))
    finally:
        tp.close()
