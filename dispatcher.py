from __future__ import print_function

import os
import sys
import base64
import json
import concurrent.futures

import boto3

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(current_path, './lib'))

import dynamo_sessions
import common

import pyaes
import random

import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

class DispatcherException(Exception):
    pass

def send_to_sqs(r, q):
    sqs_url = q['sqsUrl']
    aes_key = base64.b64decode(q['aesKey'])
    c = boto3.client('sqs')
    init_ctr = random.randint(0,9999999999) # OK if crappy python pseudo-random, only to prevent dupe blocks
    ctr = pyaes.Counter(initial_value=init_ctr)
    aes = pyaes.AESModeOfOperationCTR(aes_key,counter=ctr)
    m = str(init_ctr)+'|'+base64.b64encode(aes.encrypt(json.dumps(r)))
    try:
        c.send_message(QueueUrl=sqs_url,
                       MessageBody=m)
    except:
        LOGGER.error("Unable to send to queue {0}".format(sqs_url))
        return False
    return True

def parse_id(path_p,field):
    try:
        return int(path_p[field])
    except:
        return None
    
def dispatch(m):
    # look up queues
    # only dispatch to ones that have been active in last 24 hours
    queues = dynamo_sessions.lookup(account_id=parse_id(m,'accountId'),
                                    session_id=m.get('sessionId'),
                                    user_id=parse_id(m,'userId'))
    # unique initialization vector for AES
    LOGGER.info("Dispatching message to {0} queues".format(len(queues)))
    sqs_urls = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_sqsurl = {executor.submit(send_to_sqs, m, q): q for q in queues}
        for future in concurrent.futures.as_completed(future_to_sqsurl):
            sqs_url = future_to_sqsurl[future]
            success = future.result()
            if success:
                sqs_urls.append(sqs_url)
    return sqs_urls
        

def k_seq(x):
    kin = x.get('kinesis')
    if kin == None:
        return 0
    return kin.get('sequenceNumber',0)


def proc_rec(rec):
    try:
        r = None
        if 'kinesis' in rec:
            r = json.loads(base64.b64decode(rec['kinesis']['data']))
        if r is not None:
            dispatch(r)
    except:
        LOGGER.exception("Unable to dispatch record {0!r}".format(rec))
            
            
def lambda_handler(event, context):
    LOGGER.debug("Received event: {0!r}".format(event))
    recs = event['Records']
    # kinesis events are in order, others are not
    recs.sort(cmp=lambda x,y: cmp(k_seq(x),k_seq(y)))
    [proc_rec(x) for x in recs]

    
def gen_json_resp(d,code='200'):
    return {'statusCode': code,
            'body': json.dumps(d,cls=common.DecimalEncoder),
            'headers': {
                'Content-Type': 'application/json'
                }
        }


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
        r = dispatch(msg)
        return gen_json_resp({'success':True,
                              'sqsUrls':r})
    except:
        LOGGER.exception("Unable to handle request")
        return gen_json_resp({'success':False,
                              'message':'error handling request'},
                             code='500')
