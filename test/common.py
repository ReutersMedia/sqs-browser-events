from __future__ import print_function

import json
import boto3
import pyaes
import base64


def get_msgs(session,raw=False):
    sqs_c = boto3.client('sqs',
                         aws_access_key_id=session['accessKey'],
                         aws_secret_access_key=session['secretKey'],
                         aws_session_token=session['sessionToken'])
    msgs = []
    while True:
        print("Checking queue {0}".format(session['sqsUrl']))
        r = sqs_c.receive_message(QueueUrl=session['sqsUrl'],
                                  MaxNumberOfMessages=10)            
        if 'Messages' not in r:
            break
        for m in r['Messages']:
            init_ctr,msg = m['Body'].split('|',1)
            ctr = pyaes.Counter(initial_value=int(init_ctr))
            aes = pyaes.AESModeOfOperationCTR(base64.b64decode(session['aesKey']),counter=ctr)
            dec_m = aes.decrypt(base64.b64decode(msg))
            print("   Found message: {0}".format(dec_m))
            msgs.append(json.loads(dec_m))
            sqs_c.delete_message(QueueUrl=session['sqsUrl'],
                                 ReceiptHandle=m['ReceiptHandle'])
    if raw:
        return sorted(msgs)
    else:
        return sorted([x['msg'] for x in msgs])
