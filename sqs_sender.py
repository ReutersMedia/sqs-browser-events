from __future__ import print_function

import os
import sys
import base64
import json
import concurrent.futures
import threading
import time

import boto3
import botocore.exceptions

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(current_path, './lib'))

import common

import pyaes
import random

import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

_thread_local = threading.local()

def get_session():
    if not hasattr(_thread_local,'boto_session'):
        _thread_local.boto_session = boto3.session.Session()
    return _thread_local.boto_session


def encode_msg(aes_key,m):
    m_json = json.dumps(m,cls=common.DecimalEncoder)
    init_ctr = random.randint(0,9999999999) # OK if crappy python pseudo-random, only to prevent dupe blocks
    ctr = pyaes.Counter(initial_value=init_ctr)
    aes = pyaes.AESModeOfOperationCTR(aes_key,counter=ctr)
    return str(init_ctr)+'|'+base64.b64encode(aes.encrypt(m_json))

def send_to_sqs(payload):
    sqs_url,aes_key_b64,msg_list = payload
    try:
        aes_key = base64.b64decode(aes_key_b64)
        msg_batch = [ {"Id":str(i),"MessageBody":encode_msg(aes_key,x)} for i,x in enumerate(msg_list) ]
        LOGGER.info(json.dumps({'sqsMessageCount':len(msg_batch)}))
        session = get_session()
        c = session.client('sqs')
        n_retries = 0
        while True:
            r = c.send_message_batch(QueueUrl=sqs_url,
                                     Entries=msg_batch)
            failures = r.get('Failed')
            if failures is None or len(failures)==0:
                break
            elif n_retries < 5:
                LOGGER.warn("Failure sending {0} messages, retrying".format(len(failures)))
                failed_ids = set([x['Id'] for x in failures])
                msg_batch = [x for x in msg_batch if x['Id'] in failed_ids]
                time.sleep(5)
            else:
                LOGGER.error("Failed to deliver {0} messages".format(len(failures)))
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            LOGGER.warn("Queue does not exist: {0}".format(sqs_url))
            return False
        LOGGER.exception("Unable to send to queue {0}".format(sqs_url))
        return False
    except:
        LOGGER.exception("Unable to send to queue {0}".format(sqs_url))
        return False
    return True
            

def split_msg_list(msg_list):
    # at most 10 messages (SQS queue limit)
    for sqs_url,aes_key,m_list in msg_list:
        for i in xrange(0,len(m_list),10):
            yield (sqs_url,aes_key,m_list[i:i+10])

def lambda_handler(event, context):
    msg_list = split_msg_list(event['msg_list'])
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_sqsurl = {executor.submit(send_to_sqs, payload): payload for payload in msg_list}
        for future in concurrent.futures.as_completed(future_to_sqsurl):
            sqs_url = future_to_sqsurl[future]
            success = future.result()
    LOGGER.info("Done sending to SQS")
    return True
