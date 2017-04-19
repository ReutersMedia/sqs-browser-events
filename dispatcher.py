from __future__ import print_function

import os
import sys
import base64
import hashlib
import json
import threading
import time

from collections import defaultdict
from multiprocessing.pool import ThreadPool

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
        if len(msg_list)==0:
            return True
        session = boto3.session.Session()
        c = session.client('lambda')
        c.invoke(FunctionName=os.getenv('SQS_SENDER_LAMBDA'),
                 Payload=json.dumps({'msg_list':msg_list},cls=common.DecimalEncoder),
                 InvocationType='Event')
        LOGGER.info("Dispatched {0} message groups to SQS sender lambda".format(len(msg_list)))
    except:
        LOGGER.exception("Unable to send to sqs batch handler")
        return False
    return True

def parse_id(path_p,field):
    try:
        return int(path_p[field])
    except:
        return None

def add_user_history(user_batch):
    if len(user_batch)==0:
        return True
    try:
        session = boto3.session.Session()
        c = session.client('lambda')
        c.invoke(FunctionName=os.getenv('USER_HISTORY_ADDER_LAMBDA'),
                 Payload=json.dumps({'user_msg_list':user_batch},cls=common.DecimalEncoder),
                 InvocationType='Event')
        LOGGER.info("Dispatched {0} message groups to history adder lambda".format(len(user_batch)))
    except:
        LOGGER.exception("Unable to send to user history adder")
        return False
    return True

# some buffer for overhead
MAX_MESSAGE_SIZE = 100000

def get_dict_batches(queue_d,bsize=25,max_msg_list_size=10):
    cur_batch = defaultdict(list)
    batch_size = 0
    for q,msg_list in queue_d.iteritems():
        for msg in msg_list:
            msg_size = len(json.dumps(msg))
            if msg_size >= MAX_MESSAGE_SIZE:
                raise DispatcherException("exceeded maximum message size of {0}".format(MAX_MESSAGE_SIZE))
            if len(cur_batch)>=bsize \
               or batch_size+msg_size >= MAX_MESSAGE_SIZE \
               or len(cur_batch[q])>=max_msg_list_size:
                yield [(k,v) for k,v in cur_batch.items() if len(v)>0]
                cur_batch = defaultdict(list)
                batch_size = 0
            cur_batch[q].append(msg)
            batch_size += msg_size
    yield [(k,v) for k,v in cur_batch.items() if len(v)>0]
    

def get_sessions_for_target(target):
    return dynamo_sessions.lookup(account_id=target[0],
                                  session_id=target[1],
                                  user_id=target[2],
                                  max_expired_age=86400)


def dispatch(msg_d):
    # look up queues
    queue_d = defaultdict(list)
    user_d = defaultdict(list)
    tnow = time.time()
    msg_type_cnt = defaultdict(int)
    for target,msg_list in msg_d.iteritems():
        for m in msg_list:
            msg_type_cnt[m.get('_type','None')] += 1
        for queue in get_sessions_for_target(target):
            # only send to SQS if session is not expired
            if int(queue['expires']) > tnow:
                queue_d[(queue['sqsUrl'],queue['aesKey'])].extend(msg_list)
            # but accumulate user messages
            user_d[int(queue['userId'])].extend(msg_list)
    if len(user_d)==0 and len(queue_d)==0:
        return []
    msg_type_info = dict([('_msgcount_'+k,v) for k,v in msg_type_cnt.iteritems()])
    msg_type_info['dispatchEventCount'] = sum([len(x) for x in user_d.itervalues()])
    LOGGER.info(json.dumps(msg_type_info))
    q_tp = ThreadPool(20)
    u_tp = ThreadPool(20)
    try:
        queue_batches = get_dict_batches(queue_d,bsize=int(os.getenv('DISPATCHER_BATCH_SIZE',20)))
        queue_r = q_tp.map_async(lambda x: send_to_sqs_handler(x), queue_batches)
        user_batches = get_dict_batches(user_d,bsize=int(os.getenv('DISPATCHER_BATCH_SIZE',20)))
        user_r = u_tp.map_async(lambda x: add_user_history(x), user_batches)
        user_r.wait()
        queue_r.wait()
        sqs_urls = queue_r.get()
    finally:
        q_tp.close()
        u_tp.close()
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
    # filter out user and account messages
    recs = [x for x in recs if x.get('_type') not in ('account','user')]
    targets = defaultdict(list)
    [targets[target].append(msg) for target,msg in parse_records(recs)]
    if len(targets)>0:
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
