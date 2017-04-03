from __future__ import print_function

import sys
import os
import pprint
import urllib
import random
import uuid
import time
import json
import decimal

import boto3
import unittest
import testenv

import requests
from requests_aws4auth import AWS4Auth

import base64
import pyaes


def clean_entry(e):
    if e is None:
        return e
    d = {}
    for k,v in e.iteritems():
        if "N" in v:
            x = decimal.Decimal(v['N'])
            if x % 1 == 0:
                x = int(x)
            else:
                x = float(x)
        elif "S" in v:
            x = v['S']
        else:
            x = v
        d[k] = x
    return d


class TestSessions(unittest.TestCase):
    
    def setUp(self):
        self.props = testenv.setup()
        auth = AWS4Auth(os.getenv('AWS_ACCESS_KEY_ID'),
                        os.getenv('AWS_SECRET_ACCESS_KEY'),
                        self.props['REGION'],
                        'execute-api')
        def call_gateway(path,params=None):
            url = self.props['GATEWAY_URL'] + path
            print("calling {0} : {1!r}".format(url,params))
            resp = requests.get(url,params=params,auth=auth)
            d = json.loads(resp.text)
            return d
        self.call_gw = call_gateway

    def check_session(self,s):
        self.assertIn('sqsUrl',s)
        self.assertIn('accessKey',s)
        self.assertGreater(s['expires'],time.time()+1200)
        self.assertIn('identityId',s)
        self.assertIn('secretKey',s)
        self.assertIn('accountId',s)

        
    def get_msgs(self, session):
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
        return sorted([x['msg'] for x in msgs])

    
    def test_create_and_poll(self):
        ac_id1 = random.randint(10000000,50000000)
        ac_id2 = random.randint(50000001,80000000)
        session1a = str(uuid.uuid1())
        session1b = str(uuid.uuid1())
        session2a = str(uuid.uuid1())
        session2b = str(uuid.uuid1())
        user_id1 = random.randint(80000001,90000000)
        user_id2 = random.randint(90000001,100000000)
        r = self.call_gw('/create/{0}/{1}'.format(ac_id1,session1a))
        self.assertTrue(r['success'])
        s1a = r['session']
        self.check_session(s1a)
        s1b = self.call_gw('/create/{0}/{1}'.format(ac_id1,session1b))['session']
        s2a = self.call_gw('/create/{0}/{1}'.format(ac_id2,session2a),{'userId':user_id1})['session']
        s2b = self.call_gw('/create/{0}/{1}'.format(ac_id2,session2b),{'userId':user_id2})['session']

        c = boto3.client('kinesis',region_name=self.props['REGION'])
        # msg to session1a, session1b
        r = self.call_gw('/notify/{0}'.format(ac_id1),{'msg':'test1'})
        c.put_record(StreamName=self.props['EVENT_STREAM'],
                     Data=json.dumps({'accountId':ac_id1,
                                      'msg':'test1-k'}),
                     PartitionKey=str(ac_id1))
        # msg to session1b
        r = self.call_gw('/notify/{0}/session/{1}'.format(ac_id1,session1b),{'msg':'test2'})
        c.put_record(StreamName=self.props['EVENT_STREAM'],
                     Data=json.dumps({'accountId':ac_id1,
                                      'sessionId':session1b,
                                      'msg':'test2-k'}),
                     PartitionKey=str(ac_id1))
        # msg to session2a
        r = self.call_gw('/notify/{0}/user/{1}'.format(ac_id2,user_id1),{'msg':'test3'})
        c.put_record(StreamName=self.props['EVENT_STREAM'],
                     Data=json.dumps({'accountId':ac_id2,
                                      'userId':user_id1,
                                      'msg':'test3-k'}),
                     PartitionKey=str(ac_id2))
        time.sleep(10)

        msgs_1a = self.get_msgs(s1a)
        msgs_1b = self.get_msgs(s1b)
        msgs_2a = self.get_msgs(s2a)
        msgs_2b = self.get_msgs(s2b)
        pprint.pprint(msgs_1a)
        pprint.pprint(msgs_1b)
        pprint.pprint(msgs_2a)
        pprint.pprint(msgs_2b)
                
        self.assertListEqual(msgs_1a,['test1','test1-k'])
        self.assertListEqual(msgs_1b,['test1','test1-k','test2','test2-k'])
        self.assertListEqual(msgs_2a,['test3','test3-k'])
        self.assertListEqual(msgs_2b,[])


    def test_user_dispatch(self):
        ac_id1 = random.randint(10000000,50000000)
        session1a = str(uuid.uuid1())
        user_id1 = random.randint(80000001,90000000)
        s = self.call_gw('/create/{0}/{1}'.format(ac_id1,session1a),{'userId':user_id1})['session']
        r = self.call_gw('/notify/{0}/user/{1}'.format(ac_id1,user_id1),{'msg':'test9'})
        time.sleep(5)
        msgs = self.get_msgs(s)
        self.assertListEqual(msgs,['test9'])

        
    def test_renew(self):
        ac_id1 = random.randint(10000000,50000000)
        session1a = str(uuid.uuid1())
        r = self.call_gw('/create/{0}/{1}'.format(ac_id1,session1a))
        self.assertTrue(r['success'])
        r = self.call_gw('/notify/{0}'.format(ac_id1),{'msg':'test5'})
        # renew session
        r = self.call_gw('/renew/{0}/{1}'.format(ac_id1,session1a))
        s1a = r['session']
        self.check_session(s1a)
        time.sleep(5)
        msgs_1a = self.get_msgs(s1a)
        self.assertListEqual(msgs_1a,['test5'])

    def test_double_create(self):
        ac_id1 = random.randint(10000000,50000000)
        session1a = str(uuid.uuid1())
        r = self.call_gw('/create/{0}/{1}'.format(ac_id1,session1a))
        self.assertTrue(r['success'])
        r = self.call_gw('/create/{0}/{1}'.format(ac_id1,session1a))
        self.assertTrue(r['success'])
        s1a = r['session']
        r = self.call_gw('/notify/{0}'.format(ac_id1),{'msg':'test6'})
        time.sleep(5)
        msgs_1a = self.get_msgs(s1a)
        self.assertListEqual(msgs_1a,['test6'])

    def test_create_destroy(self):
        ac_id1 = random.randint(10000000,50000000)
        session1a = str(uuid.uuid1())
        r = self.call_gw('/create/{0}/{1}'.format(ac_id1,session1a))
        self.assertTrue(r['success'])
        s1a = r['session']
        r = self.call_gw('/destroy/{0}/{1}'.format(ac_id1,session1a))
        self.assertTrue(r['success'])
        r = self.call_gw('/notify/{0}'.format(ac_id1),{'msg':'test6'})
        time.sleep(5)
        with self.assertRaises(Exception):
            get_msgs(s1a)

    def test_cleanup(self):
        ac_id1 = random.randint(10000000,50000000)
        session1a = str(uuid.uuid1())
        r = self.call_gw('/create/{0}/{1}'.format(ac_id1,session1a))
        self.assertTrue(r['success'])
        s1a = r['session']
        # modify expiration date so 5 days in past
        dynamodb = boto3.resource('dynamodb')
        t = dynamodb.Table(self.props['SESSION_TABLE'])
        item = t.get_item(Key={'accountId':ac_id1,
                               'sessionId':session1a})['Item']
        item['expires'] = int(time.time()-86400*5)
        t.put_item(Item=item)
        # now clean, should be removed along with it's SQS url
        self.call_gw('/cleanup')
        r = t.get_item(Key={'accountId':ac_id1,
                            'sessionId':session1a})
        self.assertNotIn('Item',r)
        sqs_c = boto3.client('sqs')
        queues = sqs_c.list_queues(QueueNamePrefix=item['sqsQueueName'])
        self.assertNotIn('QueueUrls',queues)
            
        
if __name__=="__main__":
    unittest.main()
