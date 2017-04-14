from __future__ import print_function

import os
import sys
import base64
import hashlib
import json
import concurrent.futures
import threading
import time

from collections import defaultdict

import boto3

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(current_path, './lib'))

import dynamo_sessions
import common

import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

class DispatcherException(Exception):
    pass

def send_to_sqs_handler(q_batch):
    try:
        # flatten
        msg_list = [(sqs_url,aes_key,msg_list) for (sqs_url,aes_key),msg_list in q_batch]
        session = boto3.session.Session()
        c = session.client('lambda')
        c.invoke(FunctionName=os.getenv('SQS_SENDER_LAMBDA'),
                 Payload=json.dumps({'msg_list':msg_list},cls=common.DecimalEncoder),
                 InvocationType='Event')
    except:
        LOGGER.exception("Unable to send to sqs batch handler")
        return False
    return True

def parse_id(path_p,field):
    try:
        return int(path_p[field])
    except:
        return None

def add_to_user_history(user_msg_list):
    try:
        session = boto3.session.Session()
        c = session.client('lambda')
        c.invoke(FunctionName=os.getenv('USER_HISTORY_ADDER_LAMBDA'),
                 Payload=json.dumps({'user_msg_list':user_msg_list},cls=common.DecimalEncoder),
                 InvocationType='Event')
        LOGGER.info("Dispatched {0} message groups to history adder lambda".format(len(user_msg_list)))
    except:
        LOGGER.exception("Unable to send to user history adder")
        return False
    return True

def get_queue_batches(queue_d,bsize=25):
    cur_batch = []
    for q,msg_list in queue_d.iteritems():
        if len(cur_batch)>=bsize:
            yield cur_batch
            cur_batch = []
        cur_batch.append( (q,msg_list) )
    yield cur_batch
    

def get_sessions_for_target(target):
    return dynamo_sessions.lookup(account_id=target[0],
                                  session_id=target[1],
                                  user_id=target[2],
                                  max_expired_age=43200)



def dispatch(msg_d):
    # look up queues
    queue_d = defaultdict(list)
    user_d = defaultdict(list)
    for target,msg_list in msg_d.iteritems():
        for queue in get_sessions_for_target(target):
            queue_d[(queue['sqsUrl'],queue['aesKey'])].extend(msg_list)
            user_d[int(queue['userId'])].extend(msg_list)

    # first write to user dynamodb history
    user_hist_th = threading.Thread(target=add_to_user_history,args=(user_d.items(),))
    user_hist_th.start()

    queue_batches = get_queue_batches(queue_d,bsize=int(os.getenv('SQS_SENDER_BATCH_SIZE',25)))
    sqs_urls = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_sqsurl = {executor.submit(send_to_sqs_handler, qbatch): qbatch for qbatch in queue_batches}
        for future in concurrent.futures.as_completed(future_to_sqsurl):
            sqs_url_list = future_to_sqsurl[future]
            success = future.result()
            if success:
                sqs_urls.extend(sqs_url_list)
    LOGGER.info("Sent {0} messages to SQS handler".format(len(sqs_urls)))
    user_hist_th.join()
    return sqs_urls


def get_message_target(m):
    # determines set of queue targets, must be hashable
    return (parse_id(m,'accountId'),
            m.get('sessionId'),
            parse_id(m,'userId'))

def k_seq(x):
    kin = x.get('kinesis')
    if kin == None:
        return 0
    return kin.get('sequenceNumber',0)


def parse_records(records):
    for rec in records:
        try:
            if 'kinesis' in rec:
                r = json.loads(base64.b64decode(rec['kinesis']['data']))
                if 'messageId' not in r:
                    # add message ID derived from Kinesis event
                    r['messageId'] = base64.urlsafe_b64encode(hashlib.sha1(rec['eventID']).digest())
                t = get_message_target(r)
                LOGGER.info("MessageId={0}, account={1}, user={2}, session={3}".format(r['messageId'],t[0],t[2],t[1]))
                yield (t,r)
            else:
                LOGGER.error("Unrecognized record, is not from Kinesis: {0!r}".format(r))
        except:
            LOGGER.exception("Unable to dispatch record {0!r}".format(rec))

        
def lambda_handler(event, context):
    LOGGER.debug("Received event: {0!r}".format(event))
    recs = event['Records']
    # kinesis events are in order, others are not
    recs.sort(cmp=lambda x,y: cmp(k_seq(x),k_seq(y)))
    targets = defaultdict(list)
    [targets[target].append(msg) for target,msg in parse_records(recs)]
    dispatch(targets)


def api_gateway_handler(event, context):
    LOGGER.debug("Received event: {0!r}".format(event))
    path_p = event.get('pathParameters')
    qsp = event.get('queryStringParameters')
    if qsp is None:
        qsp = {}
    try:
        msg = qsp.copy()
        if path_p is not None:
            msg.update(path_p)
        if 'messageId' not in msg:
            msg['messageId'] = base64.urlsafe_b64encode(hashlib.sha1(str(time.time())+repr(msg)).digest())
        r = dispatch({get_message_target(msg):[msg]})
        return common.gen_json_resp({'success':True,
                                     'sqsUrls':r})
    except:
        LOGGER.exception("Unable to handle request")
        return common.gen_json_resp({'success':False,
                                     'message':'error handling request'},
                                    code='500')
