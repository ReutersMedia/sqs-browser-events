from __future__ import print_function

import os
import sys
import base64
import json
import concurrent.futures
import threading
import time

import boto3

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(current_path, './lib'))

import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

def send_to_sqs(payload):
    sqs_url,m = payload
    try:
        LOGGER.info("Sending {1} bytes to {0}".format(sqs_url,len(m)))
        session = boto3.session.Session()
        c = session.client('sqs')
        c.send_message(QueueUrl=sqs_url,
                       MessageBody=m)
    except:
        LOGGER.error("Unable to send to queue {0}".format(sqs_url))
        return False
    return True
            
            
def lambda_handler(event, context):
    msg_list = event['msg_list']
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_sqsurl = {executor.submit(send_to_sqs, payload): payload for payload in msg_list}
        for future in concurrent.futures.as_completed(future_to_sqsurl):
            sqs_url = future_to_sqsurl[future]
            success = future.result()
    LOGGER.info("Done sending to SQS")
    return True
