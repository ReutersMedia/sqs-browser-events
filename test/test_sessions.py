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

from common import get_msgs

import botocore.exceptions

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

        
    def test_create_and_poll(self):
        ac_id1 = random.randint(10000000,50000000)
        ac_id2 = random.randint(50000001,80000000)
        session1a = str(uuid.uuid1())
        session1b = str(uuid.uuid1())
        session2a = str(uuid.uuid1())
        session2b = str(uuid.uuid1())
        user_id1 = random.randint(80000001,90000000)
        user_id2 = user_id1 + 1
        user_id3 = user_id1 + 2
        user_id4 = user_id1 + 3
        r = self.call_gw('/create/{0}/{1}/{2}'.format(ac_id1,user_id3,session1a))
        self.assertTrue(r['success'])
        s1a = r['session']
        self.check_session(s1a)
        s1b = self.call_gw('/create/{0}/{1}/{2}'.format(ac_id1,user_id4,session1b))['session']
        s2a = self.call_gw('/create/{0}/{1}/{2}'.format(ac_id2,user_id1,session2a))['session']
        s2b = self.call_gw('/create/{0}/{1}/{2}'.format(ac_id2,user_id2,session2b))['session']

        c = boto3.client('kinesis',region_name=self.props['REGION'])
        # msg to session1a, session1b
        r = self.call_gw('/notify/account/{0}'.format(ac_id1),{'msg':'test1'})
        c.put_record(StreamName=self.props['EVENT_STREAM'],
                     Data=json.dumps({'accountId':ac_id1,
                                      'msg':'test1-k'}),
                     PartitionKey=str(ac_id1))
        # msg to session1b
        r = self.call_gw('/notify/user/{0}/session/{1}'.format(user_id4,session1b),{'msg':'test2'})
        c.put_record(StreamName=self.props['EVENT_STREAM'],
                     Data=json.dumps({'accountId':ac_id1,
                                      'sessionId':session1b,
                                      '_sqs_only':1,
                                      'msg':'test2-k'}),
                     PartitionKey=str(ac_id1))
        # msg to session2a
        r = self.call_gw('/notify/user/{1}'.format(ac_id2,user_id1),{'msg':'test3'})
        c.put_record(StreamName=self.props['EVENT_STREAM'],
                     Data=json.dumps({'accountId':ac_id2,
                                      'userId':user_id1,
                                      'msg':'test3-k'}),
                     PartitionKey=str(ac_id2))
        time.sleep(10)

        msgs_1a = get_msgs(s1a)
        msgs_1b = get_msgs(s1b)
        msgs_2a = get_msgs(s2a)
        msgs_2b = get_msgs(s2b)

        self.assertListEqual(msgs_1a,['test1','test1-k'])
        self.assertListEqual(msgs_1b,['test1','test1-k','test2','test2-k'])
        self.assertListEqual(msgs_2a,['test3','test3-k'])
        self.assertListEqual(msgs_2b,[])


    def test_user_dispatch(self):
        ac_id1 = random.randint(10000000,50000000)
        session1a = str(uuid.uuid1())
        user_id1 = random.randint(80000001,90000000)
        s = self.call_gw('/create/{0}/{1}/{2}'.format(ac_id1,user_id1,session1a))['session']
        r = self.call_gw('/notify/user/{0}'.format(user_id1),{'msg':'test9'})
        time.sleep(5)
        msgs = get_msgs(s)
        self.assertListEqual(msgs,['test9'])
        
    def test_broadcast(self):
        ac_id1 = random.randint(10000000,50000000)
        session1a = str(uuid.uuid1())
        user_id1 = random.randint(80000001,90000000)
        s = self.call_gw('/create/{0}/{1}/{2}'.format(ac_id1,user_id1,session1a))['session']
        r = self.call_gw('/notify',{'msg':'test12'})
        time.sleep(30)
        msgs = get_msgs(s)
        self.assertListEqual(msgs,['test12'])
        
    def test_renew(self):
        ac_id1 = random.randint(10000000,50000000)
        session1a = str(uuid.uuid1())
        user_id = random.randint(0,10000000)
        r = self.call_gw('/create/{0}/{1}/{2}'.format(ac_id1,user_id,session1a))
        self.assertTrue(r['success'])
        r = self.call_gw('/notify/account/{0}'.format(ac_id1),{'msg':'test5'})
        # renew session
        r = self.call_gw('/renew/{0}/{1}/{2}'.format(ac_id1,user_id,session1a))
        s1a = r['session']
        self.check_session(s1a)
        time.sleep(5)
        msgs_1a = get_msgs(s1a)
        self.assertListEqual(msgs_1a,['test5'])

    def test_double_create(self):
        ac_id1 = random.randint(10000000,50000000)
        session1a = str(uuid.uuid1())
        user_id = random.randint(0,10000000)
        r = self.call_gw('/create/{0}/{1}/{2}'.format(ac_id1,user_id,session1a))
        self.assertTrue(r['success'])
        r = self.call_gw('/create/{0}/{1}/{2}'.format(ac_id1,user_id,session1a))
        self.assertTrue(r['success'])
        s1a = r['session']
        r = self.call_gw('/notify/account/{0}'.format(ac_id1),{'msg':'test6'})
        time.sleep(5)
        msgs_1a = get_msgs(s1a)
        self.assertListEqual(msgs_1a,['test6'])

    def test_create_destroy(self):
        ac_id1 = random.randint(10000000,50000000)
        session1a = str(uuid.uuid1())
        user_id = random.randint(0,10000000)
        r = self.call_gw('/create/{0}/{1}/{2}'.format(ac_id1,user_id,session1a))
        self.assertTrue(r['success'])
        s1a = r['session']
        r = self.call_gw('/destroy/{0}/{1}/{2}'.format(ac_id1,user_id,session1a))
        self.assertTrue(r['success'])
        r = self.call_gw('/notify/account/{0}'.format(ac_id1),{'msg':'test6'})
        time.sleep(5)
        with self.assertRaises(Exception):
            get_msgs(s1a)

    def test_cleanup(self):
        ac_id1 = random.randint(10000000,50000000)
        session1a = str(uuid.uuid1())
        user_id = random.randint(0,10000000)
        r = self.call_gw('/create/{0}/{1}/{2}'.format(ac_id1,user_id,session1a))
        self.assertTrue(r['success'])
        s1a = r['session']
        # modify expiration date so 5 days in past
        dynamodb = boto3.resource('dynamodb')
        t = dynamodb.Table(self.props['SESSION_TABLE'])
        item = t.get_item(Key={'userId':user_id,
                               'sessionId':session1a})['Item']
        t.delete_item(Key={'userId':user_id,
                           'sessionId':session1a})
        # takes time for lambda to pick up, and queue deletes are not instantaneous
        time.sleep(30)
        # check whether Cognito ID still exists
        cog_c = boto3.client('cognito-identity')
        with self.assertRaises(botocore.exceptions.ClientError):
            print("checking identity {0}".format(item['identityId']))
            r = cog_c.describe_identity(IdentityId=item['identityId'])
            
        # check whether SQS queue still exists
        sqs_c = boto3.client('sqs')
        queues = sqs_c.list_queues(QueueNamePrefix=item['sqsQueueName'])
        self.assertNotIn('QueueUrls',queues)
            
        
if __name__=="__main__":
    unittest.main()
