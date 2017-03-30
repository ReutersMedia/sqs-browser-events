from __future__ import print_function

import os
import sys
import base64
import json

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


def send_to_sqs(r, sqs_url):
    c = boto3.client('sqs')
    c.send_message(QueueUrl=sqs_url,
                   MessageBody=json.dumps(r))
    return sqs_url


    
def parse_account_id(path_p):
    try:
        return int(path_p['account_id'])
    except:
        LOGGER.exception("must provide an integer account ID")
        raise DispatcherException("Invalid accountId, must be integer")
    
def dispatch(m):
    # look up queues
    queues = dynamo_sessions.lookup(parse_account_id(m),
                                    m.get('sessionId'),
                                    m.get('userId'))
    return [send_to_sqs(m,x['sqsUrl']) for x in queues]
        

def k_seq(x):
    kin = x.get('kinesis')
    if kin == None:
        return 0
    return kin.get('sequenceNumber',0)


def proc_rec(rec):
    try:
        r = None
        if 'kinesis' in rec:
            r = json.loads(base64.b64decode(rec['data']))
        if r is not None:
            dispatch(r)
    except:
        LOGGER.exception("Unable to dispatch record {0!r}".format(rec))
            
            
def lambda_hander(event, context):
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
        msg.update(path_p)
        r = dispatch(msg)
        return gen_json_resp({'success':True,
                              'sqsUrls':r})
    except:
        LOGGER.exception("Unable to handle request")
        return gen_json_resp({'success':False,
                              'message':'error handling request'},
                             code='500')
