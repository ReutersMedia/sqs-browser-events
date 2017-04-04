from __future__ import print_function

import sys
import os
import pprint
import urllib
import random
import uuid
import time
import json

import boto3
import unittest
import testenv

import requests
from requests_aws4auth import AWS4Auth



class TestUserMessageApi(unittest.TestCase):
    
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


    def test_create_and_query(self):
        ac_id1 = random.randint(10000000,50000000)
        ac_id2 = random.randint(50000001,80000000)
        session1 = str(uuid.uuid1())
        session2 = str(uuid.uuid1())
        user_id1 = random.randint(80000001,90000000)
        user_id2 = user_id1 + 1
        r = self.call_gw('/create/{0}/{1}/{2}'.format(ac_id1,user_id1,session1))
        r = self.call_gw('/create/{0}/{1}/{2}'.format(ac_id2,user_id2,session2))
        t1 = time.time()
        time.sleep(1)
        self.call_gw('/notify/user/{0}'.format(user_id1),{'msg':'test1'})
        self.call_gw('/notify/user/{0}'.format(user_id1),{'msg':'test2'})
        self.call_gw('/notify/user/{0}'.format(user_id2),{'msg':'test3'})
        self.call_gw('/notify/user/{0}'.format(user_id2),{'msg':'test4'})
        time.sleep(5)
        t2 = time.time()
        time.sleep(1)
        self.call_gw('/notify/user/{0}'.format(user_id1),{'msg':'test5'})
        self.call_gw('/notify/user/{0}'.format(user_id1),{'msg':'test6'})
        time.sleep(5)
        t3 = time.time()
        r = self.call_gw('/messages/user/{0}'.format(user_id1))
        
    def test_set_read_status(self):
        ac_id = random.randint(10000000,50000000)
        session = str(uuid.uuid1())
        user_id = random.randint(80000001,90000000)
        r = self.call_gw('/create/{0}/{1}/{2}'.format(ac_id,user_id,session))
        time.sleep(1)
        self.call_gw('/notify/user/{0}'.format(user_id),{'msg':'test1'})
        self.call_gw('/notify/user/{0}'.format(user_id),{'msg':'test2'})
        self.call_gw('/notify/user/{0}'.format(user_id),{'msg':'test3'})
        time.sleep(5)
        r = self.call_gw('/messages/user/{0}'.format(user_id))
        for m in r['messages']:
            self.assertEqual(m['is_read'],0)
        # set read receipt
        for m in r['messages']:
            r = self.call_gw('/messages/set-read/user/{0}/message/{1}'.format(user_id,m['messageId']))
            pprint.pprint(r)
        time.sleep(0.5)
        r = self.call_gw('/messages/user/{0}'.format(user_id))
        for m in r['messages']:
            self.assertEqual(m['is_read'],1)
        
        
if __name__=="__main__":
    unittest.main()
